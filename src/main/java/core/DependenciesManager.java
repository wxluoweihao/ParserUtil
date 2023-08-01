package core;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.util.Types;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.airlift.bootstrap.Bootstrap;
import io.trino.sql.analyzer.AnalyzerFactory;
import modules.NodeModule;
import modules.CatalogManagerModule;
import modules.ServerMainModule;
import modules.TransactionManagerModule;
import modules.ServerSecurityModule;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.FeaturesConfig;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.client.NodeVersion;
import io.trino.connector.*;
import io.trino.cost.*;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.eventlistener.EventListenerManager;
import io.trino.eventlistener.EventListenerModule;
import io.trino.exchange.ExchangeManagerModule;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.*;
import io.trino.execution.resourcegroups.NoOpResourceGroupManager;
import io.trino.execution.scheduler.NodeScheduler;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.execution.scheduler.UniformNodeSelectorFactory;
import io.trino.execution.warnings.WarningCollectorModule;
import io.trino.index.IndexManager;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.*;
import io.trino.security.AccessControlManager;
import io.trino.security.AccessControlModule;
import io.trino.security.GroupProviderManager;
import io.trino.server.*;
import io.trino.server.security.*;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.split.PageSinkManager;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.SessionTimeProvider;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.*;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.testing.TestingAccessControlManager;
import io.trino.transaction.*;
import io.trino.type.BlockTypeOperators;
import io.trino.type.InternalTypeManager;
import io.trino.util.FinalizerService;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.tracing.Tracing.noopTracer;
import static io.trino.connector.CatalogServiceProviderModule.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class DependenciesManager {

    public interface MetadataProvider
    {
        Metadata getMetadata(
                SystemSecurityMetadata systemSecurityMetadata,
                TransactionManager transactionManager,
                GlobalFunctionCatalog globalFunctionCatalog,
                TypeManager typeManager);
    }

    private SqlParser sqlParser;
    private PlannerContext plannerContext;
    private OptimizerConfig optimizerConfig;
    private CatalogFactory catalogFactory;
    private CatalogStore catalogStore;
    private CoordinatorDynamicCatalogManager catalogManager;
    private EventListenerManager eventListenerManager;
    private GlobalFunctionCatalog globalFunctionCatalog;
    private PluginManager pluginManager;
    private BlockEncodingManager blockEncodingManager;
    private TypeOperators typeOperators;
    private BlockTypeOperators blockTypeOperators;
    private BlockEncodingSerde blockEncodingSerde;
    private FeaturesConfig featuresConfig;
    private TypeRegistry typeRegistry;
    private TypeManager typeManager;
    private NodeInfo nodeInfo;
    private NodeVersion nodeVersion;
    private HandleResolver handleResolver;
    private TransactionManager transactionManager;
    private AccessControlManager accessControlManager;
    private InternalNodeManager nodeManager;
    private Metadata metadataManager;
    private MetadataProvider metadataProvider;
    private NodeSchedulerConfig nodeSchedulerConfig;
    private Tracer tracer;
    private SplitManager splitManager ;
    private PageSourceManager pageSourceManager;
    private PageSinkManager pageSinkManager;
    private IndexManager indexManager;
    private SessionPropertyManager sessionPropertyManager;
    private NodePartitioningManager nodePartitioningManager;
    private FunctionManager functionManager;
    private SchemaPropertyManager schemaPropertyManager;
    private ColumnPropertyManager columnPropertyManager;
    private TablePropertyManager tablePropertyManager;
    private MaterializedViewPropertyManager materializedViewPropertyManager;
    private AnalyzePropertyManager analyzePropertyManager;
    private FinalizerService finalizerService;
    private StatementAnalyzerFactory statementAnalyzerFactory;
    private TypeAnalyzer typeAnalyzer;
    private StatsCalculator statsCalculator;
    private ScalarStatsCalculator scalarStatsCalculator;
    private TaskCountEstimator taskCountEstimator;
    private CostCalculator costCalculator;
    private CostCalculator estimatedExchangesCostCalculator;
    private PlanFragmenter planFragmenter;
    private QueryIdGenerator queryIdGenerator;
    private TaskManagerConfig taskManagerConfig;
    private AnalyzerFactory analyzerFactory;

    private List<PlanOptimizer> planOptimizers;

    private SessionSupplier sessionSupplier;

    private QueryPreparer queryPreparer;

    public DependenciesManager(String versionNum) {

        Logger log = Logger.get(Server.class);
        log.info("Java version: %s", StandardSystemProperty.JAVA_VERSION.value());
        log.info("Trino parser version: %s",versionNum);

        // Custom modules for dependencies injection in trino parser.
        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new EventListenerModule(),
                new ExchangeManagerModule(),
                new CatalogManagerModule(),
                new TransactionManagerModule(),
                new ServerMainModule(versionNum),
                new WarningCollectorModule()
        );
        Bootstrap app = new Bootstrap(modules.build());
        app.doNotInitializeLogging();

        // Initialize objects
        try {
            Injector injector = app.initialize();

            log.info("Trino version: %s", injector.getInstance(NodeVersion.class).getVersion());
            ConnectorServicesProvider connectorServicesProvider = injector.getInstance(ConnectorServicesProvider.class);
            connectorServicesProvider.loadInitialCatalogs();
            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(optionalKey(PasswordAuthenticatorManager.class)).ifPresent(PasswordAuthenticatorManager::loadPasswordAuthenticator);
            injector.getInstance(EventListenerManager.class).loadEventListeners();
            injector.getInstance(GroupProviderManager.class).loadConfiguredGroupProvider();
            injector.getInstance(ExchangeManagerRegistry.class).loadExchangeManager();


            this.nodeInfo = injector.getInstance(NodeInfo.class);
            this.nodeVersion = injector.getInstance(NodeVersion.class);
            this.tracer = injector.getInstance(Tracer.class);
            this.sqlParser = injector.getInstance(SqlParser.class);
            this.eventListenerManager = injector.getInstance(EventListenerManager.class);
            this.handleResolver = injector.getInstance(HandleResolver.class);
            this.nodeManager = injector.getInstance(InternalNodeManager.class);
            this.queryIdGenerator = injector.getInstance(QueryIdGenerator.class);
            this.optimizerConfig = injector.getInstance(OptimizerConfig.class);
            this.nodeSchedulerConfig = injector.getInstance(NodeSchedulerConfig.class);
            this.taskManagerConfig = injector.getInstance(TaskManagerConfig.class);
            this.finalizerService = injector.getInstance(FinalizerService.class);



            // ???
            this.featuresConfig = injector.getInstance(FeaturesConfig.class);
            this.blockEncodingManager = injector.getInstance(BlockEncodingManager.class);
            this.typeOperators = injector.getInstance(TypeOperators.class);
            this.typeRegistry = injector.getInstance(TypeRegistry.class);
            this.blockTypeOperators = injector.getInstance(BlockTypeOperators.class);
            this.typeManager = injector.getInstance(TypeManager.class);
            this.blockEncodingSerde = injector.getInstance(BlockEncodingSerde.class);

            // initialize catalog manager
            this.globalFunctionCatalog = injector.getInstance(GlobalFunctionCatalog.class);
            this.catalogFactory = injector.getInstance(CatalogFactory.class);
            this.catalogStore = injector.getInstance(CatalogStore.class);
            this.catalogManager = injector.getInstance(CoordinatorDynamicCatalogManager.class);

            // initialize transaction manager for managing metadata database connections
            this.transactionManager = injector.getInstance(TransactionManager.class);

            // initialize access control manager for manging access rules
            this.accessControlManager = injector.getInstance(AccessControlManager.class);

            // initialize metadata manager
            this.metadataManager = injector.getInstance(MetadataManager.class);
            this.splitManager = injector.getInstance(SplitManager.class);
            this.pageSourceManager = injector.getInstance(PageSourceManager.class);
            this.pageSinkManager = injector.getInstance(PageSinkManager.class);
            this.indexManager = injector.getInstance(IndexManager.class);
            this.sessionPropertyManager = injector.getInstance(SessionPropertyManager.class);
            this.nodePartitioningManager =  injector.getInstance(NodePartitioningManager.class);
            this.functionManager = injector.getInstance(FunctionManager.class);
            this.schemaPropertyManager = injector.getInstance(SchemaPropertyManager.class);
            this.columnPropertyManager = injector.getInstance(ColumnPropertyManager.class);
            this.tablePropertyManager = injector.getInstance(TablePropertyManager.class);
            this.materializedViewPropertyManager = injector.getInstance(MaterializedViewPropertyManager.class);
            this.analyzePropertyManager = injector.getInstance(AnalyzePropertyManager.class);
            
            this.plannerContext = injector.getInstance(PlannerContext.class);


            // initialize analyzer
            this.statementAnalyzerFactory = injector.getInstance(StatementAnalyzerFactory.class);
            this.typeAnalyzer = injector.getInstance(TypeAnalyzer.class);
            this.statsCalculator = injector.getInstance(StatsCalculator.class);
            this.scalarStatsCalculator = injector.getInstance(ScalarStatsCalculator.class);
            this.taskCountEstimator = injector.getInstance(TaskCountEstimator.class);
            this.costCalculator = injector.getInstance(CostCalculator.class);
            this.estimatedExchangesCostCalculator = injector.getInstance(CostCalculator.class);
            this.planFragmenter = injector.getInstance(PlanFragmenter.class);
            this.analyzerFactory = injector.getInstance(AnalyzerFactory.class);
            this.planOptimizers = injector.getInstance(PlanOptimizersFactory.class).get();
            this.sessionSupplier = injector.getInstance(SessionSupplier.class);
            this.queryPreparer = injector.getInstance(QueryPreparer.class);

            ServerPluginsProviderConfig serverPluginsProviderConfig = new ServerPluginsProviderConfig();
            this.pluginManager = initPluginManager(serverPluginsProviderConfig, this.nodeInfo, this.handleResolver, this.blockEncodingManager, this.typeRegistry, this.globalFunctionCatalog, this.catalogFactory, this.eventListenerManager, this.accessControlManager);
            injector.getInstance(StartupStatus.class).startupComplete();

            log.info("======== SERVER STARTED ========");

        }
        catch (ApplicationConfigurationException e) {
            StringBuilder message = new StringBuilder();
            message.append("Configuration is invalid\n");
            message.append("==========\n");
            message.append("\n");
            message.append("==========");
            log.error("%s", message);
            e.printStackTrace();
            System.exit(100);
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(100);
        }
    }

    private static <T> Key<Optional<T>> optionalKey(Class<T> type)
    {
        return Key.get((TypeLiteral<Optional<T>>) TypeLiteral.get(Types.newParameterizedType(Optional.class, type)));
    }

    private CostComparator initCostComparator(OptimizerConfig optimizerConfig) {
        return new CostComparator(optimizerConfig);
    }
    private List<PlanOptimizer> getPlanOptimizers(
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TaskManagerConfig taskManagerConfig,
            boolean forceSingleNode,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            ScalarStatsCalculator scalarStatsCalculator,
            CostCalculator costCalculator,
            CostCalculator estimatedExchangesCostCalculator,
            CostComparator costComparator,
            NodePartitioningManager nodePartitioningManager
    ) {
        return new CustomPlanOptimizers(
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                taskManagerConfig,
                forceSingleNode,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculator,
                estimatedExchangesCostCalculator,
                new CostComparator(optimizerConfig),
                taskCountEstimator,
                nodePartitioningManager,
                new RuleStatsRecorder()).get();
    }

    private TaskManagerConfig initTaskManagerConfig(int taskConcurrency) {
        return new TaskManagerConfig().setTaskConcurrency(taskConcurrency);
    }

    private QueryIdGenerator initQueryIdGenerator() {
        return new QueryIdGenerator();
    }

    private PlanFragmenter initPlanFragmenter(Metadata metadataManager, FunctionManager functionManager, TransactionManager transactionManager, CatalogManager catalogManager) {
        return new PlanFragmenter(metadataManager, functionManager, transactionManager, catalogManager, new QueryManagerConfig());
    }

    private CostCalculator initEstimatedExchangesCostCalculator(CostCalculator costCalculator, TaskCountEstimator taskCountEstimator) {
        return new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);
    }

    private CostCalculator initCostCalculator(TaskCountEstimator taskCountEstimator) {
        return new CostCalculatorUsingExchanges(taskCountEstimator);
    }

    private TaskCountEstimator initTaskCountEstimator(int taskCnt) {
        return new TaskCountEstimator(() -> taskCnt);
    }

    private ScalarStatsCalculator initScalarStatsCalculator(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer) {
        return new ScalarStatsCalculator(plannerContext, typeAnalyzer);
    }

    private StatsCalculator initStatsCalculator(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer) {
        StatsNormalizer normalizer = new StatsNormalizer();
        ScalarStatsCalculator scalarStatsCalculator = new ScalarStatsCalculator(plannerContext, typeAnalyzer);
        FilterStatsCalculator filterStatsCalculator = new FilterStatsCalculator(plannerContext, scalarStatsCalculator, normalizer);
        return new ComposableStatsCalculator(new StatsCalculatorModule.StatsRulesProvider(plannerContext, scalarStatsCalculator, filterStatsCalculator, normalizer).get());
    }


    private TypeAnalyzer initTypeAnalyzer(PlannerContext plannerContext, StatementAnalyzerFactory statementAnalyzerFactory) {
        return new TypeAnalyzer(this.plannerContext, this.statementAnalyzerFactory);
    }

    private StatementAnalyzerFactory initStatementAnalyzerFactory(
            CoordinatorDynamicCatalogManager catalogManager,
            PlannerContext plannerContext,
            SqlParser sqlParser,
            AccessControlManager accessControlManager,
            TransactionManager transactionManager,
            GroupProviderManager groupProvider,
            SessionPropertyManager sessionPropertyManager,
            TablePropertyManager tablePropertyManager,
            AnalyzePropertyManager analyzePropertyManager
            ) {
        TableProceduresRegistry tableProceduresRegistry = new TableProceduresRegistry(createTableProceduresProvider(catalogManager));
        TableFunctionRegistry tableFunctionRegistry = new TableFunctionRegistry(createTableFunctionProvider(catalogManager));
        TableProceduresPropertyManager tableProceduresPropertyManager = createTableProceduresPropertyManager(catalogManager);
        return new StatementAnalyzerFactory(
                plannerContext,
                sqlParser,
                SessionTimeProvider.DEFAULT,
                accessControlManager,
                transactionManager,
                groupProvider,
                tableProceduresRegistry,
                tableFunctionRegistry,
                sessionPropertyManager,
                tablePropertyManager,
                analyzePropertyManager,
                tableProceduresPropertyManager);
    }

    public PlannerContext initPlannerContext(Metadata metadataManager, TypeOperators typeOperators, BlockEncodingSerde blockEncodingSerde, TypeManager typeManager, FunctionManager functionManager, Tracer tracer) {
        return new PlannerContext(metadataManager, typeOperators, blockEncodingSerde, typeManager, functionManager, tracer);
    }

    private FinalizerService initFinalizerService() {
        return new FinalizerService();
    }
    private SplitManager initSplitManager(CoordinatorDynamicCatalogManager catalogManager) {
        return new SplitManager(createSplitManagerProvider(catalogManager), tracer, new QueryManagerConfig());
    }

    private PageSourceManager initPageSourceManager(CoordinatorDynamicCatalogManager catalogManager) {
        return new PageSourceManager(createPageSourceProvider(catalogManager));
    }
    private PageSinkManager initPageSinkManager(CoordinatorDynamicCatalogManager catalogManager) {
        return new PageSinkManager(createPageSinkProvider(catalogManager));
    }
    private IndexManager initIndexManager(CoordinatorDynamicCatalogManager catalogManager) {
        return new IndexManager(createIndexProvider(catalogManager));
    }

    private SessionPropertyManager initSessionPropertyManager(CoordinatorDynamicCatalogManager catalogManager, FeaturesConfig featuresConfig, TaskManagerConfig taskManagerConfig, OptimizerConfig optimizerConfig) {
        Set<SystemSessionPropertiesProvider> systemSessionProperties = ImmutableSet.<SystemSessionPropertiesProvider>builder()
                .addAll(requireNonNull(ImmutableSet.of(), "extraSessionProperties is null"))
                .add(new SystemSessionProperties(
                        new QueryManagerConfig(),
                        taskManagerConfig,
                        new MemoryManagerConfig(),
                        featuresConfig,
                        optimizerConfig,
                        new NodeMemoryConfig(),
                        new DynamicFilterConfig(),
                        new NodeSchedulerConfig()))
                .build();

        return CatalogServiceProviderModule.createSessionPropertyManager(systemSessionProperties, catalogManager);
    }
    private NodePartitioningManager initNodePartitioningManager(CoordinatorDynamicCatalogManager catalogManager, InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig, FinalizerService finalizerService, BlockTypeOperators blockTypeOperators) {
        NodeScheduler nodeScheduler = new NodeScheduler(new UniformNodeSelectorFactory(nodeManager, nodeSchedulerConfig, new NodeTaskMap(finalizerService)));
        return new NodePartitioningManager(nodeScheduler, blockTypeOperators, createNodePartitioningProvider(catalogManager));
    }
    private FunctionManager initFunctionManager(CoordinatorDynamicCatalogManager catalogManager, GlobalFunctionCatalog globalFunctionCatalog) {
        return new FunctionManager(createFunctionProvider(catalogManager), globalFunctionCatalog);
    }
    private SchemaPropertyManager initSchemaPropertyManager(CoordinatorDynamicCatalogManager catalogManager) {
        return createSchemaPropertyManager(catalogManager);
    }
    private ColumnPropertyManager initColumnPropertyManager(CoordinatorDynamicCatalogManager catalogManager) {
        return createColumnPropertyManager(catalogManager);
    }
    private TablePropertyManager initTablePropertyManager(CoordinatorDynamicCatalogManager catalogManager) {
        return createTablePropertyManager(catalogManager);
    }
    private MaterializedViewPropertyManager initMaterializedViewPropertyManager(CoordinatorDynamicCatalogManager catalogManager) {
        return createMaterializedViewPropertyManager(this.catalogManager);
    }
    private AnalyzePropertyManager initAnalyzePropertyManager(CoordinatorDynamicCatalogManager catalogManager) {
        return createAnalyzePropertyManager(catalogManager);
    }

    private Tracer initTracer() {
        return noopTracer();
    }
    private NodeSchedulerConfig initNodeSchedulerConfig() {
        return new NodeSchedulerConfig();
    }

    private InternalNodeManager initInterNodeManager() {
        return new InMemoryNodeManager();
    }

    private Metadata initMetadataManager(MetadataProvider metadataProvider) {
        return metadataProvider.getMetadata(
                new DisabledSystemSecurityMetadata(),
                transactionManager,
                globalFunctionCatalog,
                typeManager
        );
    }

    private NodeInfo initNodeInfo(String nodeName) {
        return new NodeInfo(nodeName);
    }

    private HandleResolver initHandleResolver() {
        return new HandleResolver();
    }

    private TransactionManager initTransactionManager(CatalogManager catalogManager) {
        ExecutorService notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        ScheduledExecutorService yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        return InMemoryTransactionManager.create(
                new TransactionManagerConfig().setIdleTimeout(new Duration(1, TimeUnit.DAYS)),
                yieldExecutor,
                catalogManager,
                notificationExecutor);
    }

    private NodeVersion initNodeVersion(String versionName) {
        return new NodeVersion(versionName);
    }

    private FeaturesConfig initFeatureConfig() {
        return new FeaturesConfig();
    }

    private TypeRegistry initRegistry(TypeOperators typeOperators, FeaturesConfig featuresConfig) {
        return new TypeRegistry(typeOperators, featuresConfig);
    }

    private TypeManager initTypeManager(TypeRegistry typeRegistry) {
        return new InternalTypeManager(typeRegistry);
    }

    private BlockEncodingSerde initBlockEncodingSerde(BlockEncodingManager blockEncodingManager, TypeManager typeManager) {
        return new InternalBlockEncodingSerde(blockEncodingManager, typeManager);
    }

    private BlockTypeOperators initBlockTypeOperators(TypeOperators typeOperators) {
        return new BlockTypeOperators(typeOperators);
    }

    private TypeOperators initTypeOperators() {
        return new TypeOperators();
    }

    private BlockEncodingManager initBlockEncodingManager() {
        return new BlockEncodingManager();
    }

    private AccessControlManager initAccessControlManager(TransactionManager transactionManager, EventListenerManager eventListenerManager) {
        return new TestingAccessControlManager(transactionManager, eventListenerManager);
    }

    private CatalogStore initCatalogStore() {
        return new InMemoryCatalogStore();
    }

    private CatalogFactory initCatalogFactory() {
        return new LazyCatalogFactory();
    }

    private CoordinatorDynamicCatalogManager initCatalogManager(CatalogStore catalogStore, CatalogFactory catalogFactory) {
        return new CoordinatorDynamicCatalogManager(catalogStore, catalogFactory, directExecutor());
    }

    private EventListenerManager initEventListenerManager() {
        return new EventListenerManager(new EventListenerConfig());
    }

    private SqlParser initSqlParser() {
        return new SqlParser();
    }

    private GlobalFunctionCatalog initGlobalFunctionCatalog() {
        return new GlobalFunctionCatalog();
    }

    private PluginManager initPluginManager(ServerPluginsProviderConfig serverPluginsProviderConfig, NodeInfo nodeInfo, HandleResolver handleResolver, BlockEncodingManager blockEncodingManager, TypeRegistry typeRegistry, GlobalFunctionCatalog globalFunctionCatalog, CatalogFactory catalogFactory, EventListenerManager eventListenerManager, AccessControlManager accessControlManager) {

        return new PluginManager(
                new ServerPluginsProvider(serverPluginsProviderConfig, directExecutor()),
                catalogFactory,
                globalFunctionCatalog,
                new NoOpResourceGroupManager(),
                accessControlManager,
                Optional.of(new PasswordAuthenticatorManager(new PasswordAuthenticatorConfig())),
                new CertificateAuthenticatorManager(),
                Optional.of(new HeaderAuthenticatorManager(new HeaderAuthenticatorConfig())),
                eventListenerManager,
                new GroupProviderManager(),
                new SessionPropertyDefaults(nodeInfo, accessControlManager),
                typeRegistry,
                blockEncodingManager,
                handleResolver,
                new ExchangeManagerRegistry());
    }

    public QueryPreparer getQueryPreparer() {
        return queryPreparer;
    }

    public SessionSupplier getSessionSupplier() {
        return sessionSupplier;
    }

    public AnalyzerFactory getAnalyzerFactory() {
        return analyzerFactory;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
    }

    public CatalogFactory getCatalogFactory() {
        return catalogFactory;
    }

    public CatalogStore getCatalogStore() {
        return catalogStore;
    }

    public CoordinatorDynamicCatalogManager getCatalogManager() {
        return catalogManager;
    }

    public EventListenerManager getEventListenerManager() {
        return eventListenerManager;
    }

    public GlobalFunctionCatalog getGlobalFunctionCatalog() {
        return globalFunctionCatalog;
    }

    public PluginManager getPluginManager() {
        return pluginManager;
    }

    public BlockEncodingManager getBlockEncodingManager() {
        return blockEncodingManager;
    }

    public TypeOperators getTypeOperators() {
        return typeOperators;
    }

    public BlockTypeOperators getBlockTypeOperators() {
        return blockTypeOperators;
    }

    public BlockEncodingSerde getBlockEncodingSerde() {
        return blockEncodingSerde;
    }

    public FeaturesConfig getFeaturesConfig() {
        return featuresConfig;
    }

    public TypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public TypeManager getTypeManager() {
        return typeManager;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public NodeVersion getNodeVersion() {
        return nodeVersion;
    }

    public HandleResolver getHandleResolver() {
        return handleResolver;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public AccessControlManager getAccessControlManager() {
        return accessControlManager;
    }

    public InternalNodeManager getNodeManager() {
        return nodeManager;
    }

    public Metadata getMetadataManager() {
        return metadataManager;
    }

    public MetadataProvider getMetadataProvider() {
        return metadataProvider;
    }

    public NodeSchedulerConfig getNodeSchedulerConfig() {
        return nodeSchedulerConfig;
    }

    public Tracer getTracer() {
        return tracer;
    }

    public SplitManager getSplitManager() {
        return splitManager;
    }

    public PageSourceManager getPageSourceManager() {
        return pageSourceManager;
    }

    public PageSinkManager getPageSinkManager() {
        return pageSinkManager;
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public SessionPropertyManager getSessionPropertyManager() {
        return sessionPropertyManager;
    }

    public NodePartitioningManager getNodePartitioningManager() {
        return nodePartitioningManager;
    }

    public FunctionManager getFunctionManager() {
        return functionManager;
    }

    public SchemaPropertyManager getSchemaPropertyManager() {
        return schemaPropertyManager;
    }

    public ColumnPropertyManager getColumnPropertyManager() {
        return columnPropertyManager;
    }

    public TablePropertyManager getTablePropertyManager() {
        return tablePropertyManager;
    }

    public MaterializedViewPropertyManager getMaterializedViewPropertyManager() {
        return materializedViewPropertyManager;
    }

    public AnalyzePropertyManager getAnalyzePropertyManager() {
        return analyzePropertyManager;
    }

    public FinalizerService getFinalizerService() {
        return finalizerService;
    }

    public StatementAnalyzerFactory getStatementAnalyzerFactory() {
        return statementAnalyzerFactory;
    }

    public TypeAnalyzer getTypeAnalyzer() {
        return typeAnalyzer;
    }

    public StatsCalculator getStatsCalculator() {
        return statsCalculator;
    }

    public ScalarStatsCalculator getScalarStatsCalculator() {
        return scalarStatsCalculator;
    }

    public TaskCountEstimator getTaskCountEstimator() {
        return taskCountEstimator;
    }

    public CostCalculator getCostCalculator() {
        return costCalculator;
    }

    public CostCalculator getEstimatedExchangesCostCalculator() {
        return estimatedExchangesCostCalculator;
    }

    public PlanFragmenter getPlanFragmenter() {
        return planFragmenter;
    }

    public QueryIdGenerator getQueryIdGenerator() {
        return queryIdGenerator;
    }

    public TaskManagerConfig getTaskManagerConfig() {
        return taskManagerConfig;
    }

    public OptimizerConfig getOptimizerConfig() {
        return optimizerConfig;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public List<PlanOptimizer> getPlanOptimizers() {
        return planOptimizers;
    }
}
