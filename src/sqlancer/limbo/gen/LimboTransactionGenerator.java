package sqlancer.limbo.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;

public final class LimboTransactionGenerator {

    private LimboTransactionGenerator() {
    }

    public static SQLQueryAdapter generateCommit(LimboGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append(Randomly.fromOptions("COMMIT", "END"));
        if (Randomly.getBoolean()) {
            sb.append(" TRANSACTION");
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("no transaction is active",
                "The database file is locked", "FOREIGN KEY constraint failed"), true);
    }

    public static SQLQueryAdapter generateBeginTransaction(LimboGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("BEGIN ");
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE", "EXCLUSIVE"));
        }
        sb.append(" TRANSACTION;");
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("cannot start a transaction within a transaction", "The database file is locked"));
    }

    public static SQLQueryAdapter generateRollbackTransaction(LimboGlobalState globalState) {
        // TODO: could be extended by savepoint
        return new SQLQueryAdapter("ROLLBACK TRANSACTION;",
                ExpectedErrors.from("no transaction is active", "The database file is locked"), true);
    }

}
