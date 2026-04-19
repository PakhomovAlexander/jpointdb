package io.jpointdb.core.query;

import io.jpointdb.core.sql.Binder;
import io.jpointdb.core.sql.BoundAst.BoundSelect;
import io.jpointdb.core.sql.SqlAst;
import io.jpointdb.core.sql.SqlParser;
import io.jpointdb.core.table.Table;

/**
 * End-to-end: parse SQL → bind against table → execute → return result.
 */
public final class QueryEngine {

    private QueryEngine() {
    }

    public static QueryResult run(Table table, String sql) {
        SqlAst.Select ast = SqlParser.parseSelect(sql);
        BoundSelect bound = Binder.bind(ast, table);
        return Executor.execute(bound, table);
    }
}
