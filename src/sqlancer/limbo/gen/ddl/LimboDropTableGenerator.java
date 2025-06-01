package sqlancer.limbo.gen.ddl;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;

public final class LimboDropTableGenerator {

    private LimboDropTableGenerator() {
    }

    public static SQLQueryAdapter dropTable(LimboGlobalState globalState) {
        if (globalState.getSchema().getTables(t -> !t.isView()).size() == 1) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF EXISTS ");
        }
        sb.append(globalState.getSchema().getRandomTableOrBailout(t -> !t.isView()).getName());
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
                        "Abort due to constraint violation (FOREIGN KEY constraint failed)",
                        "SQL error or missing database"),
                true);

    }

}
