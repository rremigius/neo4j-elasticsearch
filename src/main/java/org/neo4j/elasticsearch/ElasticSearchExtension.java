package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;

import io.searchbox.client.config.HttpClientConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.Map;
import java.util.logging.Logger;
import java.text.ParseException;

/**
 * @author mh
 * @since 25.04.15
 */
public class ElasticSearchExtension extends LifecycleAdapter {
    private final GraphDatabaseService gds;
    private final static Logger logger = Logger.getLogger(ElasticSearchExtension.class.getName());
    private final String hostName;
    private boolean enabled = true;
    private ElasticSearchEventHandler handler;
    private JestClient client;
    private ElasticSearchIndexSettings indexSettings;
    private String indexAll;

    public ElasticSearchExtension(GraphDatabaseService gds, String hostName, String indexSpec, String indexAll, Boolean includeIDField, Boolean includeLabelsField) {
        Map iSpec;
        try {
            iSpec = ElasticSearchIndexSpecParser.parseIndexSpec(indexSpec);
            if (iSpec.size() == 0) {
                logger.severe("ElasticSearch Integration: syntax error in index_spec");
                enabled = false;
            }
            this.indexSettings = new ElasticSearchIndexSettings(iSpec, includeIDField, includeLabelsField);
        } catch (ParseException e) {
            logger.severe("ElasticSearch Integration: Can't define index twice");
            enabled = false;
        }
        logger.info("Elasticsearch Integration: Running " + hostName + " - " + indexSpec);
        this.gds = gds;
        this.hostName = hostName;
        this.indexAll = indexAll;
    }

    @Override
    public void init() throws Throwable {
        if (!enabled) return;

        client = getJestClient(hostName);
        handler = new ElasticSearchEventHandler(client, indexSettings, indexAll);
        gds.registerTransactionEventHandler(handler);
        logger.info("Connecting to ElasticSearch");
    }

    @Override
    public void shutdown() throws Throwable {
        if (!enabled) return;
        gds.unregisterTransactionEventHandler(handler);
        client.shutdownClient();
        logger.info("Disconnected from ElasticSearch");
    }

    private JestClient getJestClient(final String hostName) throws Throwable {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(hostName).multiThreaded(true).readTimeout(60000).build());
        return factory.getObject();
    }
}
