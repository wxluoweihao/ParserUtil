#  SQL Parser & Rewriter
## An Dependecies Decoupled Library for Trino
In offical trino, query optimization and plan rewriting are tight coupled with many unnessary classes for query execution, such as web server, QueryManager, SqlTaskManager, Resource Manager etc. It is not practical for some users who just simply want to parse or rewrite a query while pertaining the original grammar supports and features from Trino.

This is a demo libray that remove unecssary dependencies from Trino, while retains basic functionalities like SQL parsing, query optimization, optimization rules and SQL validation against databases. On the other hand, there are some new features that may help usera to further their developments.

## Support for Original Trino Features
- Parsing SQL into AST (Done)
- Rewrite SQL base on AST (Done)
- User access control on SQL (In progress)
- Query rewrite & optimization (Done)
- Dependencies injection using airlift (Done)

## Features
- User access control configuration in runtime (Not started)
- Convert logical plan back to SQL (Not started)

## Tested on
- Apache Hive (Done)
- Posgresql (Not started)
- Mysql (Not started)

## Installation
### Requirement
| Dependencies          | Version | Comment                                                    |
|-----------------------|---------|------------------------------------------------------------|
| Openjdk               | 17      | JDK version is aligned with offical trino jdk requirement. |
| trino-main            | 421     | This is required.                                          |
| trino-hive-hadoop2    | 421     | Optional, it is required while connectiong Apache Hive.    |
| bootstrap(io.airlift) | 234     | This is required because it is used by official trino.     |

Build the project by running:
```
mvn clean install
```

## How to use
### SQL example
```
SELECT
    GROUP_KEY AS GROUP_KEY,
    COUNT(MESSAGE_A) AS CNT1,
    COUNT(MESSAGE_B) AS CNT2,
    ROW_NUMBER() OVER(PARTITION BY GROUP_KEY ORDER BY COUNT(MESSAGE_A)) AS RN
FROM (
    SELECT CAST('KEY' AS VARCHAR) AS GROUP_KEY, A.MESSAGE AS MESSAGE_A, B.MESSAGE AS MESSAGE_B
    FROM HDFS.TEST.EXAMPLE A
    LEFT JOIN (
        SELECT *
        FROM HDFS.TEST.EXAMPLE
    ) B ON A.MESSAGE = B.MESSAGE
) T
WHERE MESSAGE_A != 'HAHAHAHA' AND MESSAGE_B != 'TEST'
GROUP BY GROUP_KEY
```
### Parsing SQL
```
Statement queryStatement = QueryUtil.parseSQL(sql);
// or
Statement queryStatement = QueryUtil.getQueryParser().parse(sql, new ParserOption());
```
### Using Optimizers from Trino for query optimization and plan rewrite
Create QueryRewriter object for query plan rewrite
```
QueryRewriter queryRewriter = QueryUtil.getOrCreateQueryRewriter();
```

(must do)Add hive metastore connection:
```
queryRewriter.addHiveCatalog(catalogName, uri, hdfsconfigPath);
```
Analyze query, including type validation
```
AnalyzedQuery analyze = queryRewriter.analyze(sql);
```
Perform query optimization, which includes query plan rewrite.
```
QueryPlan optimizedQueryPlan = queryRewriter.createOptimizedQueryPlan(analyze);
```
Print query plan rewrite result
```
optimizedQueryPlan.printPlan();
```