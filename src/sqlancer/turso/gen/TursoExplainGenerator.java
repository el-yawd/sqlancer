package sqlancer.turso.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoProvider;
import sqlancer.turso.TursoProvider.Action;

public final class TursoExplainGenerator {

    private TursoExplainGenerator() {}

    public static SQLQueryAdapter explain(TursoGlobalState globalState)
        throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        if (Randomly.getBoolean()) {
            sb.append("QUERY PLAN ");
        }
        Action action;
        do {
            action = Randomly.fromOptions(TursoProvider.Action.values());
        } while (action == Action.EXPLAIN);
        SQLQueryAdapter query = action.getQuery(globalState);
        sb.append(query);
        return new SQLQueryAdapter(sb.toString(), query.getExpectedErrors());
    }

    public static String explain(String selectStr) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN QUERY PLAN ");
        sb.append(selectStr);
        return sb.toString();
    }
}
