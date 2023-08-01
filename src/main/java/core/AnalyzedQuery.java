package core;

import io.trino.Session;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.tree.Statement;

public class AnalyzedQuery {
    private final Session session;
    private final Analyzer analyzer;

    private final Analysis analysis;

    private final Statement queryStatement;

    public AnalyzedQuery(Session session, Analyzer analyzer, Analysis analysis, Statement queryStatement) {
        this.session = session;
        this.analyzer = analyzer;
        this.queryStatement = queryStatement;
        this.analysis = analysis;
    }

    public Session getSession() {
        return session;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public Analysis getAnalysis() {
        return analysis;
    }

    public Statement getQueryStatement() {
        return queryStatement;
    }
}
