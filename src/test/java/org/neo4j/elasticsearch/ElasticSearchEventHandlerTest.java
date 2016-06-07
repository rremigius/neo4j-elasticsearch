package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;

public class ElasticSearchEventHandlerTest {

    public static final String INDEX = "test-index";
    public static final String LABEL = "Label";
    public static final String INDEX_ALL = "index-all";
    private ElasticSearchEventHandler handler;
    private ElasticSearchIndexSettings indexSettings;
    private GraphDatabaseService db;
    private JestClient client;
    private Node node;
    private String id;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        client = factory.getObject();
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        Map<String, List<ElasticSearchIndexSpec>> indexSpec =
                ElasticSearchIndexSpecParser.parseIndexSpec(INDEX + ":" + LABEL + "(foo)");
        indexSettings = new ElasticSearchIndexSettings(indexSpec, true, true);
        
        handler = new ElasticSearchEventHandler(client, indexSettings, INDEX_ALL);
        // don't use async Jest for testing
        handler.setUseAsyncJest(false);
        db.registerTransactionEventHandler(handler);
        
       // create index
       client.execute(new CreateIndex.Builder(INDEX).build());
       
       node = createNode();
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.execute(new DeleteIndex.Builder(INDEX_ALL).build());
        client.shutdownClient();
        db.unregisterTransactionEventHandler(handler);
        db.shutdown();
    }

    private Node createNode() {
        Transaction tx = db.beginTx();
        Node node = db.createNode(Label.label(LABEL));
        node.setProperty("foo", "bar");
        tx.success();tx.close();
        id = String.valueOf(node.getId());
        return node;
    }
    
    private void assertIndexCreation(JestResult response) throws java.io.IOException {
        client.execute(new Get.Builder(INDEX, id).build());
        assertEquals(true, response.isSucceeded());
        assertEquals(INDEX, response.getValue("_index"));
        assertEquals(id, response.getValue("_id"));
        assertEquals(LABEL, response.getValue("_type"));
    }
    
    @Test
    public void testAfterCommit() throws Exception {
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(singletonList(LABEL), source.get("labels"));
        assertEquals(id, source.get("id"));
        assertThat(source.get("properties"), instanceOf(Map.class));
        assertEquals("bar", ((Map)source.get("properties")).get("foo"));
    }
    
    @Test
    public void testAfterCommitWithoutID() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        indexSettings.setIncludeIDField(false);
        client.execute(new CreateIndex.Builder(INDEX).build());
        node = createNode();

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(singletonList(LABEL), source.get("labels"));
        assertEquals(null, source.get("id"));
        assertThat(source.get("properties"), instanceOf(Map.class));
        assertEquals("bar", ((Map)source.get("properties")).get("foo"));
    }
    
    @Test
    public void testAfterCommitWithoutLabels() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        indexSettings.setIncludeLabelsField(false);
        client.execute(new CreateIndex.Builder(INDEX).build());
        node = createNode();

        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Map source = response.getSourceAsObject(Map.class);
        assertEquals(null, source.get("labels"));
        assertEquals(id, source.get("id"));
        assertThat(source.get("properties"), instanceOf(Map.class));
        assertEquals("bar", ((Map)source.get("properties")).get("foo"));
    }

    @Test
    public void testDelete() throws Exception {
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Transaction tx = db.beginTx();
        node = db.getNodeById(Integer.parseInt(id));
        assertEquals("bar", node.getProperty("foo")); // check that we get the node that we just added
        assertEquals(LABEL,response.getValue("_type"));
        node.delete();
        tx.success();tx.close();

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        assertEquals(false, response.getValue("found"));
    }

    @Test
    public void testUpdate() throws Exception {
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);
        
        assertThat(response.getSourceAsObject(Map.class).get("properties"), instanceOf(Map.class));
        assertEquals("bar", ((Map)response.getSourceAsObject(Map.class).get("properties")).get("foo"));

        Transaction tx = db.beginTx();
        node = db.getNodeById(Integer.parseInt(id));
        node.setProperty("foo", "quux");
        tx.success(); tx.close();

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        assertEquals(true,response.isSucceeded());
        assertEquals(true, response.getValue("found"));
        assertThat(response.getSourceAsObject(Map.class).get("properties"), instanceOf(Map.class));
        assertEquals("quux", ((Map)response.getSourceAsObject(Map.class).get("properties")).get("foo"));
    }
}
