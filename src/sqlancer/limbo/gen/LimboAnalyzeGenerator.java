package sqlancer.limbo.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;

public final class LimboAnalyzeGenerator {

    private LimboAnalyzeGenerator() {
    }

    private enum AnalyzeTarget {
        SCHEMA, TABLE, INDEX, SQL_MASTER
    }

    public static SQLQueryAdapter generateAnalyze(LimboGlobalState globalState) {
        final StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            switch (Randomly.fromOptions(AnalyzeTarget.values())) {
            case INDEX:
                sb.append(globalState.getSchema().getRandomIndexOrBailout());
                break;
            case SCHEMA:
                sb.append(Randomly.fromOptions("main", "temp"));
                break;
            case SQL_MASTER:
                sb.append("sqlite_master");
                break;
            case TABLE:
                sb.append(globalState.getSchema().getRandomTableOrBailout().getName());
                break;
            default:
                throw new AssertionError();
            }
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("The database file is locked"));
    }

}
