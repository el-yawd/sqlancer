package sqlancer.limbo.gen.dml;

import java.util.List;
import java.util.stream.Collectors;
import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public class LimboUpdateGenerator extends AbstractUpdateGenerator<LimboColumn> {

    private final LimboGlobalState globalState;
    private final Randomly r;

    public LimboUpdateGenerator(LimboGlobalState globalState, Randomly r) {
        this.globalState = globalState;
        this.r = r;
    }

    public static SQLQueryAdapter updateRow(LimboGlobalState globalState) {
        LimboTable randomTableNoViewOrBailout = globalState
            .getSchema()
            .getRandomTableOrBailout(t -> !t.isView() && !t.isReadOnly());
        return updateRow(globalState, randomTableNoViewOrBailout);
    }

    public static SQLQueryAdapter updateRow(
        LimboGlobalState globalState,
        LimboTable table
    ) {
        LimboUpdateGenerator generator = new LimboUpdateGenerator(
            globalState,
            globalState.getRandomly()
        );
        return generator.generate(table);
    }

    private SQLQueryAdapter generate(LimboTable table) {
        List<LimboColumn> columnsToUpdate =
            Randomly.nonEmptySubsetPotentialDuplicates(table.getColumns());
        sb.append("UPDATE ");

        // TODO Beginning in SQLite version 3.15.0 (2016-10-14), an assignment in the
        // SET clause can be a parenthesized list of column names on the left and a row
        // value of the same size on the right.

        sb.append(table.getName());
        sb.append(" SET ");
        if (Randomly.getBoolean()) {
            sb.append("(");
            sb.append(
                columnsToUpdate
                    .stream()
                    .map(c -> c.getName())
                    .collect(Collectors.joining(", "))
            );
            sb.append(")");
            sb.append("=");
            sb.append("(");
            for (int i = 0; i < columnsToUpdate.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                updateValue(columnsToUpdate.get(i));
            }
            sb.append(")");
            // row values
        } else {
            updateColumns(columnsToUpdate);
        }

        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            String whereClause = LimboVisitor.asString(
                new LimboExpressionGenerator(globalState)
                    .setColumns(table.getColumns())
                    .generateExpression()
            );
            sb.append(whereClause);
        }

        // ORDER BY and LIMIT are only supported by enabling a compile-time option
        // List<Expression> expressions = QueryGenerator.generateOrderBy(table.getColumns());
        // if (!expressions.isEmpty()) {
        // sb.append(" ORDER BY ");
        // sb.append(expressions.stream().map(e -> LimboVisitor.asString(e)).collect(Collectors.joining(", ")));
        // }

        LimboErrors.addInsertUpdateErrors(errors);

        errors.add(
            "[SQLITE_ERROR] SQL error or missing database (parser stack overflow)"
        );
        errors.add(
            "[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)"
        );
        // for views
        errors.add("ORDER BY term out of range");
        errors.add("unknown function: json_type");

        LimboErrors.addInsertNowErrors(errors);
        LimboErrors.addExpectedExpressionErrors(errors);
        LimboErrors.addDeleteErrors(errors);
        return new SQLQueryAdapter(
            sb.toString(),
            errors,
            true/* column could have an ON UPDATE clause */
        );
    }

    @Override
    protected void updateValue(LimboColumn column) {
        if (column.isIntegerPrimaryKey()) {
            sb.append(
                LimboVisitor.asString(
                    LimboConstant.createIntConstant(r.getInteger())
                )
            );
        } else {
            sb.append(
                LimboVisitor.asString(
                    LimboExpressionGenerator.getRandomLiteralValue(globalState)
                )
            );
        }
    }
}
