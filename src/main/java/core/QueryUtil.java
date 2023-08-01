package core;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import core.security.CheckRule;
import core.security.UserAccess;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.execution.QueryPreparer;
import io.trino.server.SessionContext;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlFormatterUtil;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Plan;
import io.trino.sql.tree.Statement;
import io.trino.tracing.TrinoAttributes;
import io.trino.transaction.TransactionId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ENGLISH;

public class QueryUtil {

    private static QueryRewriter queryRewriter;

    private static SqlParser sqlParser = new SqlParser();

    private static DependenciesManager dependenciesManager;

    public static Statement parseSQL(String sql) {
        return sqlParser.createStatement(sql, new ParsingOptions());
    }

    public static SqlParser getQueryParser() {
        return sqlParser;
    }

    public static QueryRewriter getOrCreateQueryRewriter() {
        if(dependenciesManager == null) {
            dependenciesManager = new DependenciesManager("0.0.1");
        }

        if(queryRewriter == null) {
            queryRewriter = new QueryRewriter(dependenciesManager);
        }

        return queryRewriter;
    }

    public static void check(Plan queryPlan, List<CheckRule> checkRules) {
        for(CheckRule checkRule : checkRules) {
            queryPlan.getRoot().accept(checkRule, checkRule.getRuleContext());
        }
    }

    public static String statementToSql(Statement queryStatement) {
        return SqlFormatterUtil.getFormattedSql(queryStatement, sqlParser);
    }
}
