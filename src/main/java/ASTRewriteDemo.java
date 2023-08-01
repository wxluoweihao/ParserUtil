import core.AnalyzedQuery;
import core.QueryPlan;
import core.QueryRewriter;
import core.QueryUtil;
import core.rules.CastLiterals;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Level;
import io.airlift.log.Logging;
import io.trino.sql.SqlFormatterUtil;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.*;
import utils.SqlUtils;

import java.io.FileInputStream;
import java.util.Optional;
import java.util.Properties;

public class ASTRewriteDemo {
    public static void main(String[] args) throws Exception {
        // 1. prepare sql
        String sql =
                "SELECT\n" +
                        "    *\n" +
                        "FROM HDFS.TEST.EXAMPLE\n" +
                        "WHERE CAST(MESSAGE_A AS VARCHAR) != 'HAHAHAHA'";

        // 2. parse sql
        Statement queryStatement = QueryUtil.parseSQL(sql);
        String originalSQL = QueryUtil.statementToSql(queryStatement);

        Statement newQueryStatement = QueryRewriter.rewrite(queryStatement, new CastLiterals());
        String rewriteSQL = QueryUtil.statementToSql(newQueryStatement);

        System.out.println("Original SQL: \n" + originalSQL);
        System.out.println("Rewritten SQL: \n" + rewriteSQL);
    }
}
