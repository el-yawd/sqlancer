package sqlancer.limbo.gen.ddl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.gen.dml.LimboDeleteGenerator;
import sqlancer.limbo.gen.dml.LimboInsertGenerator;
import sqlancer.limbo.gen.dml.LimboUpdateGenerator;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public final class LimboCreateTriggerGenerator {

    private LimboCreateTriggerGenerator() {
    }

    private enum OnAction {
        INSERT, DELETE, UPDATE
    }

    private enum TriggerAction {
        INSERT, DELETE, UPDATE, RAISE
    }

    public static SQLQueryAdapter create(LimboGlobalState globalState) throws SQLException {
        LimboSchema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder();
        LimboTable table = s.getRandomTableOrBailout(t -> !t.isVirtual());
        sb.append("CREATE");
        if (table.isTemp()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
        }
        sb.append(" TRIGGER");
        sb.append(" IF NOT EXISTS ");
        sb.append("tr");
        sb.append(Randomly.smallNumber());
        sb.append(" ");
        if (table.isView()) {
            sb.append("INSTEAD OF");
        } else {
            sb.append(Randomly.fromOptions("BEFORE", "AFTER"));
        }
        sb.append(" ");

        OnAction randomAction = Randomly.fromOptions(OnAction.values());
        switch (randomAction) {
        case INSERT:
            sb.append("INSERT ON ");
            break;
        case DELETE:
            sb.append("DELETE ON ");
            break;
        case UPDATE:
            sb.append("UPDATE ");
            if (Randomly.getBoolean()) {
                sb.append("OF ");
                for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(table.getRandomColumn().getName());
                }
                sb.append(" ");
            }
            sb.append("ON ");
            break;
        default:
            throw new AssertionError();
        }
        appendTableNameAndWhen(globalState, sb, table);

        LimboTable randomActionTable = s.getRandomTableNoViewOrBailout();
        sb.append(" BEGIN ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            switch (Randomly.fromOptions(TriggerAction.values())) {
            case DELETE:
                sb.append(LimboDeleteGenerator.deleteContent(globalState, randomActionTable));
                break;
            case INSERT:
                sb.append(getQueryString(s, globalState));
                break;
            case UPDATE:
                sb.append(LimboUpdateGenerator.updateRow(globalState, randomActionTable));
                break;
            case RAISE:
                sb.append("SELECT RAISE(");
                if (Randomly.getBoolean()) {
                    sb.append("IGNORE");
                } else {
                    sb.append(Randomly.fromOptions("ROLLBACK", "ABORT", "FAIL"));
                    sb.append(", 'asdf'");
                }
                sb.append(")");
                sb.append(";");
                break;
            default:
                throw new AssertionError();
            }
        }
        sb.append("END");

        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("parser stack overflow", "unsupported frame specification"));
    }

    private static void appendTableNameAndWhen(LimboGlobalState globalState, StringBuilder sb, LimboTable table) {
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" FOR EACH ROW ");
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHEN ");
            sb.append(LimboVisitor.asString(
                    new LimboExpressionGenerator(globalState).setColumns(table.getColumns()).generateExpression()));
        }
    }

    private static String getQueryString(LimboSchema s, LimboGlobalState globalState) throws SQLException {
        String q;
        do {
            q = LimboInsertGenerator.insertRow(globalState, getTableNotEqualsTo(s, s.getRandomTableNoViewOrBailout()))
                    .getQueryString();
        } while (q.contains("DEFAULT VALUES"));
        return q;
    }

    private static LimboTable getTableNotEqualsTo(LimboSchema s, LimboTable table) {
        List<LimboTable> tables = new ArrayList<>(s.getDatabaseTablesWithoutViews());
        tables.remove(table);
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(tables);
    }

}
