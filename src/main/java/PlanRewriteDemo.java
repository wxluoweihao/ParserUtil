import core.*;
import core.security.AccessPermisionCheck;
import core.security.CheckRule;
import core.security.FakeUserAccess;
import core.security.UserAccess;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import utils.SqlUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PlanRewriteDemo {

    private static final Logger log = Logger.get(PlanRewriteDemo.class);

    public static void main(String[] args) throws IOException {
        String sql = "SELECT\n" +
                "    GROUP_KEY AS GROUP_KEY,\n" +
                "    COUNT(MESSAGE_A) AS CNT1,\n" +
                "    COUNT(MESSAGE_B) AS CNT2,\n" +
                "    ROW_NUMBER() OVER(PARTITION BY GROUP_KEY ORDER BY COUNT(MESSAGE_A)) AS RN\n" +
                "FROM (\n" +
                "    SELECT CAST('KEY' AS VARCHAR) AS GROUP_KEY, A.MESSAGE AS MESSAGE_A, B.MESSAGE AS MESSAGE_B\n" +
                "    FROM HDFS.TEST.EXAMPLE A\n" +
                "    LEFT JOIN (\n" +
                "        SELECT *\n" +
                "        FROM HDFS.TEST.EXAMPLE\n" +
                "    ) B ON A.MESSAGE = B.MESSAGE\n" +
                ") T\n" +
                "WHERE CAST(MESSAGE_A AS VARCHAR) != 'HAHAHAHA' AND CAST(MESSAGE_B AS VARCHAR) = 'TEST'\n" +
                "GROUP BY GROUP_KEY";

        Properties catalogProperties = new Properties();
        if(args.length == 2) {
            // 1. prepare sql
            String queryFile = args[0];
            sql = SqlUtils.readLocalFile(queryFile);
            catalogProperties.load(new FileInputStream(args[1]));
        } else {
            catalogProperties.load(new FileInputStream(args[0]));
        }

        // 3. connect to hive
        String sessionName = catalogProperties.getProperty("session.name");
        String catalogName = catalogProperties.getProperty("trino.catalog.name");
        String schemaName = catalogProperties.getProperty("trino.catalog.schema");
        String uri = catalogProperties.getProperty("trino.catalog.uri");
        String hdfsconfigPath = catalogProperties.getProperty("trino.hdfs.configs");
        System.out.println("Session name: " + sessionName);
        System.out.println("hive catalog name: " + catalogName);
        System.out.println("hive schema name: " + schemaName);
        System.out.println("hive schema uri: " + uri);
        System.out.println("hdfs config path: " + hdfsconfigPath);

        // 4. run query planner
        QueryRewriter queryRewriter = QueryUtil.getOrCreateQueryRewriter();
        queryRewriter.addHiveCatalog(catalogName, uri, hdfsconfigPath);
        AnalyzedQuery analyze1 = queryRewriter.analyze(sql);
        QueryPlan optimizedQueryPlan = queryRewriter.createOptimizedQueryPlan(analyze1);
        optimizedQueryPlan.printPlan();

        // 5. perform column access check
        List<CheckRule> rules = new ArrayList<>();
        UserAccess userAccess = new FakeUserAccess("mikeweihaoluo");
        rules.add(new AccessPermisionCheck("Access permission check", queryRewriter, userAccess));
        QueryUtil.check(optimizedQueryPlan.getQueryPlan(), rules);
    }
}
