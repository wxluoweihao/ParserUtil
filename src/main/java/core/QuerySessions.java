package core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.hive.HivePlugin;
import io.trino.server.PluginManager;
import io.trino.spi.Plugin;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;

public class QuerySessions {

    private Map<String, Session> sessionMap;
    private QueryIdGenerator queryIdGenerator;
    private DependenciesManager dependenciesManager;

    private Map<String, Boolean> installedPlugins;

    private TransactionManager transactionManager;


    private SessionPropertyManager sessionPropertyManager;


    private PluginManager pluginManager;


    private CatalogManager catalogManager;

    public QuerySessions() {
        this.dependenciesManager = new DependenciesManager("0.0.1");
        this.sessionMap = new HashMap<>();
        this.queryIdGenerator = new QueryIdGenerator();
        this.installedPlugins = new HashMap<>();
    }

    public QueryUtil connectHiveCatalog(String hiveSessionName, String catalogName, String schemaName, String connectUri, String hdfsConfigs) {

        // install hive plugin
        addHivePlugin();

        // connect catalog
        this.dependenciesManager.getCatalogManager().createCatalog(
                catalogName,
                new ConnectorName("hive"),
                ImmutableMap.<String, String>builder()
                .put("hive.metastore.uri", connectUri)
                .put("hive.config.resources", hdfsConfigs).build(), false);


        // create session for hive
        Session session = initHiveSession("hive", catalogName, schemaName);
        this.sessionMap.put(hiveSessionName, session);

        return new QueryUtil();
    }

    private QuerySessions addHivePlugin() {
        if(!this.installedPlugins.containsKey(HivePlugin.class.getCanonicalName())) {
            Plugin hivePlugin = new HivePlugin();
            this.dependenciesManager.getPluginManager()
                    .installPlugin(hivePlugin, ignored -> hivePlugin.getClass().getClassLoader());
        }
        return this;
    }

    private Session initHiveSession(String userName, String catalogName, String schemaName) {
        TransactionId transactionId = this.dependenciesManager.getTransactionManager().beginTransaction(true);
        Session sampleSession = Session.builder(this.dependenciesManager.getSessionPropertyManager())
                .setQueryId(this.queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser(userName))
                .setSource("adhoc")
                .setCatalog(catalogName)
                .setSchema(schemaName)
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Pacific/Apia"))
                .setLocale(ENGLISH)
                .setClientCapabilities(ImmutableSet.of())
                .setRemoteUserAddress("address")
                .setUserAgent("agent")
                .build();

        return new Session(
                sampleSession.getQueryId(),
                Span.getInvalid(),
                Optional.of(transactionId),
                sampleSession.isClientTransactionSupport(),
                sampleSession.getIdentity(),
                sampleSession.getSource(),
                sampleSession.getCatalog(),
                sampleSession.getSchema(),
                sampleSession.getPath(),
                sampleSession.getTraceToken(),
                sampleSession.getTimeZoneKey(),
                sampleSession.getLocale(),
                sampleSession.getRemoteUserAddress(),
                sampleSession.getUserAgent(),
                sampleSession.getClientInfo(),
                sampleSession.getClientTags(),
                sampleSession.getClientCapabilities(),
                sampleSession.getResourceEstimates(),
                sampleSession.getStart(),
                sampleSession.getSystemProperties(),
                sampleSession.getCatalogProperties(),
                this.dependenciesManager.getSessionPropertyManager(),
                sampleSession.getPreparedStatements(),
                sampleSession.getProtocolHeaders(),
                sampleSession.getExchangeEncryptionKey());
    }
}
