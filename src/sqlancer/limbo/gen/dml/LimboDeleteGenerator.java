package sqlancer.limbo.gen.dml;

import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public final class LimboDeleteGenerator {

    private LimboDeleteGenerator() {
    }

    public static SQLQueryAdapter deleteContent(LimboGlobalState globalState) {
        LimboTable tableName = globalState.getSchema().getRandomTable(t -> !t.isView() && !t.isReadOnly());
        return deleteContent(globalState, tableName);
    }

    public static SQLQueryAdapter deleteContent(LimboGlobalState globalState, LimboTable tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(tableName.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(LimboVisitor.asString(new LimboExpressionGenerator(globalState)
                    .setColumns(tableName.getColumns()).generateExpression()));
        }
        ExpectedErrors errors = new ExpectedErrors();
        LimboErrors.addExpectedExpressionErrors(errors);
        errors.addAll(Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
                "[SQLITE_CONSTRAINT]  Abort due to constraint violation ",
                "[SQLITE_ERROR] SQL error or missing database (parser stack overflow)",
                "[SQLITE_ERROR] SQL error or missing database (no such table:", "no such column",
                "too many levels of trigger recursion", "cannot UPDATE generated column",
                "cannot INSERT into generated column", "A table in the database is locked",
                "load_extension() prohibited in triggers and views", "The database file is locked"));
        LimboErrors.addDeleteErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
