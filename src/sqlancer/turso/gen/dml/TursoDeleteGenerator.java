package sqlancer.turso.gen.dml;

import java.util.Arrays;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema.TursoTable;

public final class TursoDeleteGenerator {

    private TursoDeleteGenerator() {
    }

    public static SQLQueryAdapter deleteContent(TursoGlobalState globalState) {
        TursoTable tableName = globalState.getSchema().getRandomTable(t -> !t.isView() && !t.isReadOnly());
        return deleteContent(globalState, tableName);
    }

    public static SQLQueryAdapter deleteContent(TursoGlobalState globalState, TursoTable tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(tableName.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(TursoVisitor.asString(new TursoExpressionGenerator(globalState)
                    .setColumns(tableName.getColumns()).generateExpression()));
        }
        ExpectedErrors errors = new ExpectedErrors();
        TursoErrors.addExpectedExpressionErrors(errors);
        errors.addAll(Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
                "[SQLITE_CONSTRAINT]  Abort due to constraint violation ",
                "[SQLITE_ERROR] SQL error or missing database (parser stack overflow)",
                "[SQLITE_ERROR] SQL error or missing database (no such table:", "no such column",
                "too many levels of trigger recursion", "cannot UPDATE generated column",
                "cannot INSERT into generated column", "A table in the database is locked",
                "load_extension() prohibited in triggers and views", "The database file is locked"));
        TursoErrors.addDeleteErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
