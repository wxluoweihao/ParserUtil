package core;

import com.google.common.collect.ImmutableMap;
import core.rules.CastLiterals;
import core.security.CheckRule;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.cost.CostComparator;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryIdGenerator;
import io.trino.execution.QueryPreparer;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.hive.HivePlugin;
import io.trino.security.AccessControl;
import io.trino.server.SessionContext;
import io.trino.server.SessionSupplier;
import io.trino.spi.Plugin;
import io.trino.spi.QueryId;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.*;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.*;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.planprinter.PlanPrinter;
import io.trino.sql.planner.sanity.PlanSanityChecker;
import io.trino.sql.tree.*;
import io.trino.tracing.TrinoAttributes;
import io.trino.transaction.TransactionId;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.ParameterExtractor.bindParameters;

public class QueryRewriter {

    private PlannerContext plannerContext;

    private AnalyzerFactory analyzerFactory;

    private DependenciesManager dependenciesManager;

    private List<PlanOptimizer> defaultPlanOptimizers;

    private LogicalPlanner logicalPlanner;

    private AccessControl accessControl;

    private SessionSupplier sessionSupplier;

    private QueryIdGenerator queryIdGenerator;

    private QueryPreparer queryPreparer;

    private Map<String, Boolean> installedPlugins;

    public QueryRewriter(DependenciesManager dependenciesManager) {
        this.dependenciesManager = dependenciesManager;
        this.plannerContext = this.dependenciesManager.getPlannerContext();
        this.defaultPlanOptimizers = this.dependenciesManager.getPlanOptimizers();
        this.analyzerFactory = this.dependenciesManager.getAnalyzerFactory();
        this.sessionSupplier = this.dependenciesManager.getSessionSupplier();
        this.queryIdGenerator = this.dependenciesManager.getQueryIdGenerator();
        this.accessControl = this.dependenciesManager.getAccessControlManager();
        this.queryPreparer = this.dependenciesManager.getQueryPreparer();
        this.installedPlugins = new HashMap<>();
    }

    public void addHiveCatalog(String catalogName, String connectUri, String hdfsConfigs) {
        // install hive plugin
        addHivePlugin();

        // connect catalog
        this.dependenciesManager.getCatalogManager().createCatalog(
                catalogName,
                new ConnectorName("hive"),
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", connectUri)
                        .put("hive.config.resources", hdfsConfigs).build(), false);
    }

    public QueryRewriter addHiveCatalog(String catalogName, String connectUri) {
        // install hive plugin
        addHivePlugin();

        // connect catalog
        this.dependenciesManager.getCatalogManager().createCatalog(
                catalogName,
                new ConnectorName("hive"),
                ImmutableMap.<String, String>builder()
                        .put("hive.metastore.uri", connectUri)
                        .build(), false);

        return this;
    }

    public static Statement rewrite(Statement queryStatement, AstVisitor rewriteRules) {
        return (Statement) queryStatement.accept(rewriteRules, queryStatement);
    }

    public AnalyzedQuery analyze(String sql) {
        TransactionId transactionId = this.dependenciesManager.getTransactionManager().beginTransaction(true);
        SessionContext sessionContext = SessionContextBuilder.build(transactionId, sql);
        QueryId nextQueryId = this.queryIdGenerator.createNextQueryId();

        // decode session
        Session sampleSession = this.sessionSupplier.createSession(nextQueryId, Span.getInvalid(), sessionContext);
        Session newSession = Session.builder(sampleSession).setTransactionId(transactionId).build();

        // check query execute permissions
        this.accessControl.checkCanExecuteQuery(sessionContext.getIdentity());

        // prepare query
        QueryPreparer.PreparedQuery preparedQuery = this.queryPreparer.prepareQuery(newSession, sql);
        Analyzer analyzer = this.analyzerFactory.createAnalyzer(
                newSession,
                preparedQuery.getParameters(),
                bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters()),
                WarningCollector.NOOP,
                new PlanOptimizersStatsCollector(Integer.MAX_VALUE));
        Statement query = preparedQuery.getStatement();
        Analysis analysis = analyzer.analyze(query);

        return new AnalyzedQuery(newSession, analyzer, analysis, query);
    }

    public QueryPlan createUnoptimizedQueryPlan(AnalyzedQuery analyzedQuery) {
        return createQueryPlan(analyzedQuery, new ArrayList<>(0));
    }

    public QueryPlan createOptimizedQueryPlan(AnalyzedQuery analyzedQuery) {
        return createQueryPlan(analyzedQuery, this.defaultPlanOptimizers);
    }

    public QueryPlan rewriteQueryPlan(AnalyzedQuery analyzedQuery, PlanOptimizer rewriteRule) {
        List<PlanOptimizer> planOptimizers = new ArrayList<>(1);
        planOptimizers.add(rewriteRule);
        return createQueryPlan(analyzedQuery,planOptimizers);
    }

    public QueryPlan createOptimizedQueryPlan(AnalyzedQuery analyzedQuery, List<PlanOptimizer> rewriteRules) {
        return createQueryPlan(analyzedQuery, rewriteRules);
    }

    private QueryPlan createQueryPlan(AnalyzedQuery analyzedQuery, List<PlanOptimizer> rewriteRules) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(
                analyzedQuery.getSession(),
                rewriteRules,
                new PlanSanityChecker(true),
                new PlanNodeIdAllocator(),
                this.plannerContext,
                this.dependenciesManager.getTypeAnalyzer(),
                this.dependenciesManager.getStatsCalculator(),
                this.dependenciesManager.getCostCalculator(),
                WarningCollector.NOOP,
                new PlanOptimizersStatsCollector(Integer.MAX_VALUE));
        Plan queryPlan = logicalPlanner.plan(analyzedQuery.getAnalysis());
        return new QueryPlan(analyzedQuery.getSession(), logicalPlanner, queryPlan, rewriteRules, this.plannerContext.getMetadata(), this.plannerContext.getFunctionManager());
    }

    public boolean validate(Plan plan, List<CheckRule> checkerRules) {
        PlanNode planNode = plan.getRoot();
        for(CheckRule rule : checkerRules) {
            planNode.accept(rule, rule.getRuleContext());
            Optional<Boolean> ruleIsPassed = rule.getRuleContext().getRulesStatusByName(rule.getRuleName());
            if(!ruleIsPassed.isPresent() || !ruleIsPassed.get().booleanValue()) {
                return false;
            }
        }

        return true;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public DependenciesManager getDependenciesManager() {
        return dependenciesManager;
    }

    public List<PlanOptimizer> getDefaultPlanOptimizers() {
        return defaultPlanOptimizers;
    }

    public AccessControl getAccessControl() {
        return accessControl;
    }

    public Map<String, Boolean> getInstalledPlugins() {
        return installedPlugins;
    }

    private QueryRewriter addHivePlugin() {
        if(!this.installedPlugins.containsKey(HivePlugin.class.getCanonicalName())) {
            Plugin hivePlugin = new HivePlugin();
            this.dependenciesManager.getPluginManager()
                    .installPlugin(hivePlugin, ignored -> hivePlugin.getClass().getClassLoader());
        }
        return this;
    }
}
