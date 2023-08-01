package core;

import coral.shading.io.airlift.log.Logger;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.LogicalPlanner;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.planprinter.PlanPrinter;

import java.util.List;

public class QueryPlan {

    Logger log = Logger.get(QueryPlan.class);

    private Session session;
    private final LogicalPlanner logicalPlanner;

    private final Plan queryPlan;

    private final List<PlanOptimizer> rewriteRules;

    private final Metadata metadata;

    private final FunctionManager functionManager;

    public QueryPlan(Session session, LogicalPlanner logicalPlanner, Plan queryPlan, List<PlanOptimizer> rewriteRules, Metadata metadata, FunctionManager functionManager) {
        this.session = session;
        this.logicalPlanner = logicalPlanner;
        this.queryPlan = queryPlan;
        this.rewriteRules = rewriteRules;
        this.metadata = metadata;
        this.functionManager = functionManager;
    }

    public Session getSession() {
        return session;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public FunctionManager getFunctionManager() {
        return functionManager;
    }

    public LogicalPlanner getLogicalPlanner() {
        return logicalPlanner;
    }

    public Plan getQueryPlan() {
        return queryPlan;
    }

    public List<PlanOptimizer> getRewriteRules() {
        return rewriteRules;
    }

    public void printPlan() {
        String queryPlanStr = PlanPrinter.textLogicalPlan(
                this.queryPlan.getRoot(),
                this.queryPlan.getTypes(),
                this.metadata,
                this.functionManager,
                StatsAndCosts.empty(),
                this.session,
                0,
                true);

        log.info("Logical plan after applying rewrite rule: \n%s", queryPlanStr);
    }
}
