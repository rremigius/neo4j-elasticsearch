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
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ElasticSearchEventHandlerIntegrationTest {

    public static final String LABEL = "MyLabel";
    public static final String INDEX = "my_index";
    public static final String INDEX_ALL = "index_all";
    public static final String LABEL_ALL = "node";
    private GraphDatabaseService db;
    private JestClient client;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .build());
        client = factory.getObject();
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(config())
                .newGraphDatabase();

        // create index
        client.execute(new CreateIndex.Builder(INDEX_ALL).build());
    }

    private Map<String, String> config() {
        return stringMap(
                "elasticsearch.host_name", "http://localhost:9200",
                "elasticsearch.index_spec", INDEX + ":" + LABEL + "(foo)",
                "elasticsearch.index_all", INDEX_ALL);
    }

    @After
    public void tearDown() throws Exception {
    	client.execute(new DeleteIndex.Builder(INDEX).build());
        client.execute(new DeleteIndex.Builder(INDEX_ALL).build());
        client.shutdownClient();
        db.shutdown();
    }

    @Test
    public void testAfterCommit() throws Exception {
        Transaction tx = db.beginTx();
        org.neo4j.graphdb.Node node = db.createNode(DynamicLabel.label(LABEL));
        String id = String.valueOf(node.getId());
        node.setProperty("foo", "foobar");
        tx.success();
        tx.close();
        
        Thread.sleep(1000); // wait for the async elasticsearch query to complete
        
        // Node should be indexed in general index
        JestResult responseAll = client.execute(new Get.Builder(INDEX_ALL, id).build());
        assertEquals(INDEX_ALL, responseAll.getValue("_index"));
        assertEquals(LABEL_ALL, responseAll.getValue("_type"));
        
        assertEquals(true, responseAll.isSucceeded());
	        
        assertEquals(id, responseAll.getValue("_id"));
	
        Map source = responseAll.getSourceAsObject(Map.class);
        assertEquals(id, source.get("id"));
        assertThat(source.get("properties"), instanceOf(Map.class));
        assertEquals("foobar", ((Map)source.get("properties")).get("foo"));
    }
}
