package core.security;

import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class CheckRule extends PlanVisitor<PlanNode, CheckRule.RuleContext> {

    protected String ruleName;

    protected UserAccess userAccess;

    protected PlannerContext plannerContext;

    protected RuleContext ruleContext;


    public class RuleContext {
        private final Map<String, Boolean> rulesStatus = new HashMap<>();
        private final Map<String, String> symbolColumnMapping = new HashMap<>();

        public Optional<Boolean> getRulesStatusByName(String name) {
            if(rulesStatus.containsKey(name)) {
                return Optional.of(this.rulesStatus.get(name));
            } else {
                return Optional.empty();
            }
        }

        public void upsertRule(String ruleName, boolean status) {
            this.rulesStatus.put(ruleName, status);
        }

        public void upsertSymbolColumnMapping(Symbol symbol, String columnName) {
            this.symbolColumnMapping.put(symbol.getName(), columnName);
        }
    }

    public CheckRule(String ruleName, PlannerContext plannerContext, UserAccess userAccess) {
        this.ruleName = ruleName;
        this.plannerContext = plannerContext;
        this.userAccess = userAccess;
        this.ruleContext = new RuleContext();
    }

    abstract boolean validate(String tableName, List<String> columns);

    public RuleContext getRuleContext() {
        return ruleContext;
    }

    public String getRuleName() {
        return ruleName;
    }

    public UserAccess getUserAccess() {
        return userAccess;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    @Override
    protected PlanNode visitPlan(PlanNode node, RuleContext context) {


        for (PlanNode child : node.getSources()) {
            // go to child nodes
            child.accept(this, context);
        }

        return node;
    }

}
