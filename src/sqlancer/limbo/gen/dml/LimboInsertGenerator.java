package sqlancer.limbo.gen.dml;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboToStringVisitor;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public class LimboInsertGenerator {

    private final Randomly r;
    private final ExpectedErrors errors;
    private final LimboGlobalState globalState;

    public LimboInsertGenerator(LimboGlobalState globalState, Randomly r) {
        this.globalState = globalState;
        this.r = r;
        errors = new ExpectedErrors();
    }

    public static SQLQueryAdapter insertRow(LimboGlobalState globalState) throws SQLException {
        LimboTable randomTable = globalState.getSchema().getRandomTableOrBailout(t -> !t.isView() && !t.isReadOnly());
        return insertRow(globalState, randomTable);
    }

    public static SQLQueryAdapter insertRow(LimboGlobalState globalState, LimboTable randomTable) {
        LimboInsertGenerator generator = new LimboInsertGenerator(globalState, globalState.getRandomly());
        String query = generator.insertRow(randomTable);
        return new SQLQueryAdapter(query, generator.errors, true);
    }

    private String insertRow(LimboTable table) {
        LimboErrors.addInsertUpdateErrors(errors);
        errors.add("[SQLITE_FULL]");
        // // TODO: also check if the table is really missing (caused by a DROP TABLE)
        errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"); // trigger
        errors.add("values were supplied"); // trigger
        errors.add("Data type mismatch (datatype mismatch)"); // trigger

        errors.add("load_extension() prohibited in triggers and views");
        LimboErrors.addInsertNowErrors(errors);
        LimboErrors.addExpectedExpressionErrors(errors);
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT ");
        if (Randomly.getBoolean()) {
            sb.append("OR IGNORE "); // TODO: try to generate REPLACE
        } else {
            String fromOptions = Randomly.fromOptions("OR REPLACE ", "OR ABORT ", "OR FAIL ", "OR ROLLBACK ");
            sb.append(fromOptions);
        }
        boolean defaultValues = false;
        sb.append("INTO ");
        sb.append(table.getName());
        List<LimboColumn> cols = table.getRandomNonEmptyColumnSubset();
        if (cols.size() != table.getColumns().size() || Randomly.getBoolean()) {
            sb.append("(");
            appendColumnNames(cols, sb);
            sb.append(")");
        } else {
            // If the column-name list after table-name is omitted then the number of values
            // inserted into each row must be the same as the number of columns in the
            // table.
            cols = table.getColumns(); // get them again in sorted order
            assert cols.size() == table.getColumns().size();
        }
        sb.append(" VALUES ");
        int nrRows = 1 + Randomly.smallNumber();
        appendNrValues(sb, cols, nrRows);
        boolean columnsInConflictClause = Randomly.getBoolean();
        if (!defaultValues && Randomly.getBooleanWithSmallProbability() && !table.isVirtual()) {
            sb.append(" ON CONFLICT");
            if (columnsInConflictClause) {
                sb.append("(");
                sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> c.getName())
                        .collect(Collectors.joining(", ")));
                sb.append(")");
                errors.add("ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint");
            }
            sb.append(" DO ");
            if (Randomly.getBoolean() || !columnsInConflictClause) {
                sb.append("NOTHING");
            } else {
                sb.append("UPDATE SET ");
                List<LimboColumn> columns = table.getRandomNonEmptyColumnSubset();
                for (int i = 0; i < columns.size(); i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(columns.get(i).getName());
                    sb.append("=");
                    if (Randomly.getBoolean()) {
                        sb.append(
                                LimboVisitor.asString(LimboExpressionGenerator.getRandomLiteralValue(globalState)));
                    } else {
                        if (Randomly.getBoolean()) {
                            sb.append("excluded.");
                        }
                        sb.append(table.getRandomColumn().getName());
                    }

                }
                errors.add("Abort due to constraint violation");
                errors.add("Data type mismatch (datatype mismatch)");
                if (Randomly.getBoolean()) {
                    sb.append(" WHERE ");
                    sb.append(LimboVisitor.asString(new LimboExpressionGenerator(globalState)
                            .setColumns(table.getColumns()).generateExpression()));
                }
            }
        }
        return sb.toString();
    }

    private void appendNrValues(StringBuilder sb, List<LimboColumn> columns, int nrValues) {
        for (int i = 0; i < nrValues; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("(");
            appendValue(sb, columns);
            sb.append(")");
        }
    }

    private void appendValue(StringBuilder sb, List<LimboColumn> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            LimboExpression literal;
            if (columns.get(i).isIntegerPrimaryKey()) {
                literal = LimboConstant.createIntConstant(r.getInteger(0, 1000));
            } else {
                if (Randomly.getBooleanWithSmallProbability()) {
                    literal = new LimboExpressionGenerator(globalState).generateExpression();
                } else {
                    literal = LimboExpressionGenerator.getRandomLiteralValue(globalState);
                }
            }
            LimboToStringVisitor visitor = new LimboToStringVisitor();
            visitor.visit(literal);
            sb.append(visitor.get());
        }
    }

    private static List<LimboColumn> appendColumnNames(List<LimboColumn> columns, StringBuilder sb) {
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
        }
        return columns;
    }

}
