package core.security;

import core.QueryRewriter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;

public class AccessPermisionCheck extends CheckRule {

    public AccessPermisionCheck(String ruleName, QueryRewriter queryPlanner, UserAccess userAccess) {
        super(ruleName, queryPlanner.getPlannerContext(), userAccess);
    }

    @Override
    boolean validate(String tableName, List<String> columns) {
        return true;
    }

    @Override
    public PlanNode visitTableScan(TableScanNode node, CheckRule.RuleContext context)
    {
        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            if (node.getOutputSymbols().contains(entry.getKey())) {
                node.getOutputSymbols().forEach(t -> System.out.println("symbol: " + t.getName()));
                String[] columnAndType = entry.getValue().toString().split(":");
                System.out.println("user " + userAccess.getUserId() + " has access on source column: " + columnAndType[0] +", column type: " + columnAndType[1]);
            }
        }

        return visitPlan(node, context);
    }
}
