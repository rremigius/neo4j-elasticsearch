package org.neo4j.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Update;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
* @author mh
* @since 25.04.15
*/
class ElasticSearchEventHandler implements TransactionEventHandler<Collection<BulkableAction>>, JestResultHandler<JestResult> {
    private final JestClient client;
    private final static Logger logger = Logger.getLogger(ElasticSearchEventHandler.class.getName());
    private final ElasticSearchIndexSettings indexSettings;
    private final Set<String> indexLabels;
    private final String indexAll;
    private final String indexAllType = "node";
    private boolean useAsyncJest = true;

    public ElasticSearchEventHandler(JestClient client, ElasticSearchIndexSettings indexSettings, String indexAll) {
        this.client = client;
        this.indexSettings = indexSettings;
        this.indexLabels = indexSettings.getIndexSpec().keySet();
        this.indexAll = indexAll;
    }

    @Override
    public Collection<BulkableAction> beforeCommit(TransactionData transactionData) throws Exception {
        Map<IndexId, BulkableAction> actions = new HashMap<>(1000);

        for (Node node : transactionData.createdNodes()) {
        	actions.putAll(indexRequests(node));
        }
        for (LabelEntry labelEntry : transactionData.assignedLabels()) {
            if (transactionData.isDeleted(labelEntry.node())) {
                actions.putAll(deleteRequests(labelEntry.node()));
            } else {
                actions.putAll(indexRequests(labelEntry.node()));
            }
        }
        for (LabelEntry labelEntry : transactionData.removedLabels()) {
        	actions.putAll(deleteRequests(labelEntry.node(), labelEntry.label()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
        	actions.putAll(indexRequests(propEntry.entity()));
        }
        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (!transactionData.isDeleted(propEntry.entity()))
                actions.putAll(updateRequests(propEntry.entity()));
        }
        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions.values();
    }

    public void setUseAsyncJest(boolean useAsyncJest) {
        this.useAsyncJest = useAsyncJest;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Collection<BulkableAction> actions) {
        if (actions.isEmpty()) return;
        try {
            Bulk bulk = new Bulk.Builder()
                    .addAction(actions).build();
            if (useAsyncJest) {
                client.executeAsync(bulk, this);
            }
            else {
                client.execute(bulk);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error updating ElasticSearch ", e);
        }
    }

    private boolean hasLabel(Node node) {
        for (Label l: node.getLabels()) {
            if (indexLabels.contains(l.name())) return true;
        }
        return false;
    }

    private boolean hasLabel(LabelEntry labelEntry) {
        return indexLabels.contains(labelEntry.label().name());
    }

    private boolean hasLabel(PropertyEntry<Node> propEntry) {
        return hasLabel(propEntry.entity());
    }
    
    private Map<IndexId, Index> indexRequests(Node node) {
        HashMap<IndexId, Index> reqs = new HashMap<>();

        String id = id(node);
    	if(indexAll != null) {
    		reqs.put(new IndexId(indexAll, id), new Index.Builder(nodeToJson(node, null))
            .type(indexAllType)
            .index(indexAll)
            .id(id)
            .build());
    	}
        
        for (Label l: node.getLabels()) {
            if (!indexLabels.contains(l.name())) continue;

            for (ElasticSearchIndexSpec spec: indexSettings.getIndexSpec().get(l.name())) {
                String indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id), new Index.Builder(nodeToJson(node, spec.getProperties()))
                .type(l.name())
                .index(indexName)
                .id(id)
                .build());
            }
        }
        return reqs;
    }

    private Map<IndexId, Delete> deleteRequests(Node node) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

        String id = id(node);
    	if(indexAll != null) {
    		reqs.put(new IndexId(indexAll, id),
			         new Delete.Builder(id).index(indexAll).build());
    	}
        
    	for (Label l: node.getLabels()) {
    		if (!indexLabels.contains(l.name())) continue;
    		for (ElasticSearchIndexSpec spec: indexSettings.getIndexSpec().get(l.name())) {
    		    String indexName = spec.getIndexName();
    			reqs.put(new IndexId(indexName, id),
    			         new Delete.Builder(id).index(indexName).build());
    		}
    	}
    	return reqs;
    }
    
    private Map<IndexId, Delete> deleteRequests(Node node, Label label) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

        String id = id(node);
    	if(indexAll != null) {
    		reqs.put(new IndexId(indexAll, id),
                    new Delete.Builder(id)
                              .index(indexAll)
                              .type(indexAllType)
                              .build());
    	}
        
        if (indexLabels.contains(label.name())) {
            for (ElasticSearchIndexSpec spec: indexSettings.getIndexSpec().get(label.name())) {
                String indexName = spec.getIndexName();
                reqs.put(new IndexId(indexName, id),
                         new Delete.Builder(id)
                                   .index(indexName)
                                   .type(label.name())
                                   .build());
            }
        }
        return reqs;
    }
    
    private Map<IndexId, Update> updateRequests(Node node) {
    	HashMap<IndexId, Update> reqs = new HashMap<>();

    	String id = id(node);
    	if(indexAll != null) {
    		reqs.put(new IndexId(indexAll, id),
			        new Update.Builder(nodeToJson(node, null))
                			  .type(indexAllType)
                			  .index(indexAll)
                			  .id(id(node))
                			  .build());
    	}
    	
    	for (Label l: node.getLabels()) {
    		if (!indexLabels.contains(l.name())) continue;

    		for (ElasticSearchIndexSpec spec: indexSettings.getIndexSpec().get(l.name())) {
    		    String indexName = spec.getIndexName();
    			reqs.put(new IndexId(indexName, id),
    			        new Update.Builder(nodeToJson(node, spec.getProperties()))
                    			  .type(l.name())
                    			  .index(spec.getIndexName())
                    			  .id(id(node))
                    			  .build());
    		}
    	}
    	return reqs;
    }

    private String id(Node node) {
        return String.valueOf(node.getId());
    }

    private Map nodeToJson(Node node, Set<String> properties) {
        Map<String,Object> json = new LinkedHashMap<>();
        
        if(indexSettings.getIncludeIDField()) 
        	json.put("id", id(node));
        if(indexSettings.getIncludeLabelsField()) 
        	json.put("labels", labels(node));

        // Write properties one level deeper, to avoid conflicts with "id" and "labels"
        Map<String,Object> jsonProperties = new LinkedHashMap<>();
        json.put("properties", jsonProperties);

	    // If specified properties are empty, copy all node properties
        if(properties == null || properties.isEmpty()) {
        	for (String prop : node.getPropertyKeys()) {
        		jsonProperties.put(prop, node.getProperty(prop));
        	}
	    // Else, only copy specified properties
        } else {
        	for (String prop : properties) {
				if (node.hasProperty(prop)) {
					Object value = node.getProperty(prop);
					jsonProperties.put(prop, value);
				}
			}
        }

        return json;
    }
    
    private String[] labels(Node node) {
        List<String> result=new ArrayList<>();
        for (Label label : node.getLabels()) {
            result.add(label.name());
        }
        return result.toArray(new String[result.size()]);
    }

    @Override
    public void afterRollback(TransactionData transactionData, Collection<BulkableAction> actions) {
    }

    @Override
    public void completed(JestResult jestResult) {
        if (jestResult.isSucceeded() && jestResult.getErrorMessage() == null) {
            logger.fine("ElasticSearch Update Success");
        } else {
            logger.severe("ElasticSearch Update Failed: " + jestResult.getErrorMessage());
        }
    }

    @Override
    public void failed(Exception e) {
        logger.log(Level.WARNING,"Problem Updating ElasticSearch ",e);
    }
    
    private class IndexId {
        final String indexName, id;
        public IndexId(String indexName, String id) {
            this.indexName = indexName;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result
                    + ((indexName == null) ? 0 : indexName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof IndexId))
                return false;
            IndexId other = (IndexId) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (indexName == null) {
                if (other.indexName != null)
                    return false;
            } else if (!indexName.equals(other.indexName))
                return false;
            return true;
        }
        
        private ElasticSearchEventHandler getOuterType() {
            return ElasticSearchEventHandler.this;
        }

        @Override
        public String toString() {
            return "IndexId [indexName=" + indexName + ", id=" + id + "]";
        }
    }
}
