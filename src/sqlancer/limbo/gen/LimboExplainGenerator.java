package sqlancer.limbo.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboProvider;
import sqlancer.limbo.LimboProvider.Action;

public final class LimboExplainGenerator {

    private LimboExplainGenerator() {
    }

    public static SQLQueryAdapter explain(LimboGlobalState globalState) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        if (Randomly.getBoolean()) {
            sb.append("QUERY PLAN ");
        }
        Action action;
        do {
            action = Randomly.fromOptions(LimboProvider.Action.values());
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
