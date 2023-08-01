package modules;

import com.google.common.collect.ImmutableList;
import com.google.inject.*;
import com.google.inject.multibindings.Multibinder;
import core.CustomPlanOptimizers;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.cost.*;
import io.trino.execution.*;
import io.trino.execution.scheduler.*;
import io.trino.memory.*;
import io.trino.operator.ForScheduler;
import io.trino.server.*;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.QueryExplainerFactory;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.planner.*;
import io.trino.sql.rewrite.*;
import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static java.util.concurrent.Executors.*;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
//        install(new WebUiModule());

        // coordinator announcement
//        discoveryBinder(binder).bindHttpAnnouncement("trino-coordinator");

        // statement resource
//        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
//        jaxrsBinder(binder).bind(QueuedStatementResource.class);
//        jaxrsBinder(binder).bind(ExecutingStatementResource.class);
//        binder.bind(StatementHttpExecutionMBean.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(StatementHttpExecutionMBean.class).withGeneratedName();
//        binder.bind(QueryInfoUrlFactory.class).in(Scopes.SINGLETON);

        // allow large prepared statements in headers
//        configBinder(binder).bindConfigDefaults(HttpServerConfig.class, config -> {
//            config.setMaxRequestHeaderSize(DataSize.of(2, MEGABYTE));
//            config.setMaxResponseHeaderSize(DataSize.of(2, MEGABYTE));
//        });

        // failure detector
//        install(new FailureDetectorModule());
//        jaxrsBinder(binder).bind(NodeResource.class);
//        jaxrsBinder(binder).bind(WorkerResource.class);
//        install(internalHttpClientModule("workerInfo", ForWorkerInfo.class).build());

        // query monitor
//        jsonCodecBinder(binder).bindJsonCodec(ExecutionFailureInfo.class);
//        jsonCodecBinder(binder).bindJsonCodec(OperatorStats.class);
//        jsonCodecBinder(binder).bindJsonCodec(StageInfo.class);
//        jsonCodecBinder(binder).bindJsonCodec(StatsAndCosts.class);
//        configBinder(binder).bindConfig(QueryMonitorConfig.class);
//        binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);

        // query manager
//        jaxrsBinder(binder).bind(QueryResource.class);
//        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
//        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
//        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
//        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
//        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
//        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

        // dispatcher
//        binder.bind(DispatchManager.class).in(Scopes.SINGLETON);
//        // export under the old name, for backwards compatibility
//        newExporter(binder).export(DispatchManager.class).as(generator -> generator.generatedNameOf(QueryManager.class));
//        binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
//        binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);

        // local dispatcher
//        binder.bind(DispatchQueryFactory.class).to(LocalDispatchQueryFactory.class);

        // cluster memory manager
//        binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
//        binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);
//        install(internalHttpClientModule("memoryManager", ForMemoryManager.class)
//                .withTracing()
//                .withConfigDefaults(config -> {
//                    config.setIdleTimeout(new Duration(30, SECONDS));
//                    config.setRequestTimeout(new Duration(10, SECONDS));
//                }).build());
//
//        bindLowMemoryTaskKiller(MemoryManagerConfig.LowMemoryTaskKillerPolicy.NONE, NoneLowMemoryKiller.class);
//        bindLowMemoryTaskKiller(MemoryManagerConfig.LowMemoryTaskKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesTaskLowMemoryKiller.class);
//        bindLowMemoryTaskKiller(MemoryManagerConfig.LowMemoryTaskKillerPolicy.LEAST_WASTE, LeastWastedEffortTaskLowMemoryKiller.class);
//        bindLowMemoryQueryKiller(MemoryManagerConfig.LowMemoryQueryKillerPolicy.NONE, NoneLowMemoryKiller.class);
//        bindLowMemoryQueryKiller(MemoryManagerConfig.LowMemoryQueryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
//        bindLowMemoryQueryKiller(MemoryManagerConfig.LowMemoryQueryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesQueryLowMemoryKiller.class);
//
//        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

        // node allocator
//        binder.bind(BinPackingNodeAllocatorService.class).in(Scopes.SINGLETON);
//        binder.bind(NodeAllocatorService.class).to(BinPackingNodeAllocatorService.class);
//        binder.bind(PartitionMemoryEstimatorFactory.class).to(BinPackingNodeAllocatorService.class);

        // node monitor
//        binder.bind(ClusterSizeMonitor.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(ClusterSizeMonitor.class).withGeneratedName();

        // statistics calculator
        binder.install(new StatsCalculatorModule());

        // cost calculator
        binder.bind(TaskCountEstimator.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).to(CostCalculatorUsingExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostCalculator.class).annotatedWith(CostCalculator.EstimatedExchanges.class).to(CostCalculatorWithEstimatedExchanges.class).in(Scopes.SINGLETON);
        binder.bind(CostComparator.class).in(Scopes.SINGLETON);

        // dynamic filtering service
        binder.bind(DynamicFilterService.class).in(Scopes.SINGLETON);

        // analyzer
        binder.bind(AnalyzerFactory.class).in(Scopes.SINGLETON);

        // statement rewriter
        binder.bind(StatementRewrite.class).in(Scopes.SINGLETON);
        Multibinder<StatementRewrite.Rewrite> rewriteBinder = newSetBinder(binder, StatementRewrite.Rewrite.class);
        rewriteBinder.addBinding().to(DescribeInputRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(DescribeOutputRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(ShowQueriesRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(ShowStatsRewrite.class).in(Scopes.SINGLETON);
        rewriteBinder.addBinding().to(ExplainRewrite.class).in(Scopes.SINGLETON);

        // planner
        binder.bind(PlanFragmenter.class).in(Scopes.SINGLETON);
//        binder.bind(PlanOptimizersFactory.class).to(CustomPlanOptimizers.class).in(Scopes.SINGLETON);

        // Optimizer/Rule Stats exporter
        binder.bind(RuleStatsRecorder.class).in(Scopes.SINGLETON);
//        binder.bind(OptimizerStatsMBeanExporter.class).in(Scopes.SINGLETON);

        // query explainer
        binder.bind(QueryExplainerFactory.class).in(Scopes.SINGLETON);

        // explain analyze
//        binder.bind(ExplainAnalyzeContext.class).in(Scopes.SINGLETON);

        // execution scheduler
//        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
//        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
//        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
//        jsonCodecBinder(binder).bindJsonCodec(FailTaskRequest.class);
//        jsonCodecBinder(binder).bindJsonCodec(DynamicFiltersCollector.VersionedDynamicFilterDomains.class);
//        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();
//
//        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

//        install(internalHttpClientModule("scheduler", ForScheduler.class)
//                .withTracing()
//                .withFilter(GenerateTraceTokenRequestFilter.class)
//                .withConfigDefaults(config -> {
//                    config.setIdleTimeout(new Duration(30, SECONDS));
//                    config.setRequestTimeout(new Duration(10, SECONDS));
//                    config.setMaxConnectionsPerServer(250);
//                }).build());

//        binder.bind(ScheduledExecutorService.class).annotatedWith(ForScheduler.class)
//                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("stage-scheduler")));

        // query execution
//        QueryManagerConfig queryManagerConfig = buildConfigObject(QueryManagerConfig.class);
//        ThreadPoolExecutor queryExecutor = new ThreadPoolExecutor(
//                queryManagerConfig.getQueryExecutorPoolSize(),
//                queryManagerConfig.getQueryExecutorPoolSize(),
//                60, SECONDS,
//                new LinkedBlockingQueue<>(1000),
//                threadsNamed("query-execution-%s"));
//        queryExecutor.allowCoreThreadTimeOut(true);
//        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class).toInstance(queryExecutor);
//        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(QueryExecutionMBean.class)
//                .as(generator -> generator.generatedNameOf(QueryExecution.class));

        binder.bind(SplitSourceFactory.class).in(Scopes.SINGLETON);
        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();

//        binder.bind(EventDrivenTaskSourceFactory.class).in(Scopes.SINGLETON);
//        binder.bind(TaskDescriptorStorage.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(TaskDescriptorStorage.class).withGeneratedName();
//
//        binder.bind(TaskExecutionStats.class).in(Scopes.SINGLETON);
//        newExporter(binder).export(TaskExecutionStats.class).withGeneratedName();
//
//        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
//        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
//        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);
//
//        install(new QueryExecutionFactoryModule());
//
//        // cleanup
//        binder.bind(io.trino.server.CoordinatorModule.ExecutorCleanup.class).asEagerSingleton();
    }

//    @Provides
//    @Singleton
//    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
//    {
//        return manager;
//    }

//    @Provides
//    @Singleton
//    public static QueryPerformanceFetcher createQueryPerformanceFetcher(QueryManager queryManager)
//    {
//        return queryManager::getFullQueryInfo;
//    }

    @Provides
    private CostCalculatorWithEstimatedExchanges initCostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, TaskCountEstimator taskCountEstimator) {
        return new CostCalculatorWithEstimatedExchanges(costCalculator, taskCountEstimator);
    }

    @Provides
    @Singleton
    public PlanOptimizersFactory initPlanOptimizers(
            PlannerContext plannerContext,
            StatementAnalyzerFactory statementAnalyzerFactory,
            TaskManagerConfig taskManagerConfig,
            SplitManager splitManager,
            PageSourceManager pageSourceManager,
            StatsCalculator statsCalculator,
            ScalarStatsCalculator scalarStatsCalculator,
            CostCalculator costCalculator,
            CostCalculatorWithEstimatedExchanges costCalculatorWithEstimatedExchanges,
            CostComparator costComparator,
            TaskCountEstimator taskCountEstimator,
            NodePartitioningManager nodePartitioningManager,
            RuleStatsRecorder ruleStatsRecorder
    ) {
        return new CustomPlanOptimizers(
                plannerContext,
                new TypeAnalyzer(plannerContext, statementAnalyzerFactory),
                taskManagerConfig,
                true,
                splitManager,
                pageSourceManager,
                statsCalculator,
                scalarStatsCalculator,
                costCalculator,
                costCalculatorWithEstimatedExchanges,
                costComparator,
                taskCountEstimator,
                nodePartitioningManager,
                ruleStatsRecorder
        );
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ExecutorService createStatementResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("statement-response-%s"));
    }

//    @Provides
//    @Singleton
//    @ForStatementResource
//    public static BoundedExecutor createStatementResponseExecutor(@ForStatementResource ExecutorService coreExecutor, TaskManagerConfig config)
//    {
//        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
//    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ScheduledExecutorService createStatementTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("statement-timeout-%s"));
    }

    private void bindLowMemoryQueryKiller(MemoryManagerConfig.LowMemoryQueryKillerPolicy policy, Class<? extends LowMemoryKiller> clazz)
    {
        install(conditionalModule(
                MemoryManagerConfig.class,
                config -> policy == config.getLowMemoryQueryKillerPolicy(),
                binder -> binder
                        .bind(LowMemoryKiller.class)
                        .annotatedWith(LowMemoryKiller.ForQueryLowMemoryKiller.class)
                        .to(clazz)
                        .in(Scopes.SINGLETON)));
    }

    private void bindLowMemoryTaskKiller(MemoryManagerConfig.LowMemoryTaskKillerPolicy policy, Class<? extends LowMemoryKiller> clazz)
    {
        install(conditionalModule(
                MemoryManagerConfig.class,
                config -> policy == config.getLowMemoryTaskKillerPolicy(),
                binder -> binder
                        .bind(LowMemoryKiller.class)
                        .annotatedWith(LowMemoryKiller.ForTaskLowMemoryKiller.class)
                        .to(clazz)
                        .in(Scopes.SINGLETON)));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForStatementResource ExecutorService statementResponseExecutor,
                @ForStatementResource ScheduledExecutorService statementTimeoutExecutor,
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(statementResponseExecutor)
                    .add(statementTimeoutExecutor)
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}
