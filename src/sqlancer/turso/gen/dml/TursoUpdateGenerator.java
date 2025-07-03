package sqlancer.turso.gen.dml;

import java.util.List;
import java.util.stream.Collectors;
import sqlancer.Randomly;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;

public class TursoUpdateGenerator extends AbstractUpdateGenerator<TursoColumn> {

    private final TursoGlobalState globalState;
    private final Randomly r;

    public TursoUpdateGenerator(TursoGlobalState globalState, Randomly r) {
        this.globalState = globalState;
        this.r = r;
    }

    public static SQLQueryAdapter updateRow(TursoGlobalState globalState) {
        TursoTable randomTableNoViewOrBailout = globalState
            .getSchema()
            .getRandomTableOrBailout(t -> !t.isView() && !t.isReadOnly());
        return updateRow(globalState, randomTableNoViewOrBailout);
    }

    public static SQLQueryAdapter updateRow(
        TursoGlobalState globalState,
        TursoTable table
    ) {
        TursoUpdateGenerator generator = new TursoUpdateGenerator(
            globalState,
            globalState.getRandomly()
        );
        return generator.generate(table);
    }

    private SQLQueryAdapter generate(TursoTable table) {
        List<TursoColumn> columnsToUpdate =
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
            String whereClause = TursoVisitor.asString(
                new TursoExpressionGenerator(globalState)
                    .setColumns(table.getColumns())
                    .generateExpression()
            );
            sb.append(whereClause);
        }

        // ORDER BY and LIMIT are only supported by enabling a compile-time option
        // List<Expression> expressions = QueryGenerator.generateOrderBy(table.getColumns());
        // if (!expressions.isEmpty()) {
        // sb.append(" ORDER BY ");
        // sb.append(expressions.stream().map(e -> TursoVisitor.asString(e)).collect(Collectors.joining(", ")));
        // }

        TursoErrors.addInsertUpdateErrors(errors);

        errors.add(
            "[SQLITE_ERROR] SQL error or missing database (parser stack overflow)"
        );
        errors.add(
            "[SQLITE_ERROR] SQL error or missing database (second argument to likelihood() must be a constant between 0.0 and 1.0)"
        );
        // for views
        errors.add("ORDER BY term out of range");
        errors.add("unknown function: json_type");

        TursoErrors.addInsertNowErrors(errors);
        TursoErrors.addExpectedExpressionErrors(errors);
        TursoErrors.addDeleteErrors(errors);
        return new SQLQueryAdapter(
            sb.toString(),
            errors,
            true/* column could have an ON UPDATE clause */
        );
    }

    @Override
    protected void updateValue(TursoColumn column) {
        if (column.isIntegerPrimaryKey()) {
            sb.append(
                TursoVisitor.asString(
                    TursoConstant.createIntConstant(r.getInteger())
                )
            );
        } else {
            sb.append(
                TursoVisitor.asString(
                    TursoExpressionGenerator.getRandomLiteralValue(globalState)
                )
            );
        }
    }
}
