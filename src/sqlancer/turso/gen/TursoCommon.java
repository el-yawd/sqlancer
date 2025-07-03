package sqlancer.turso.gen;

import java.util.ArrayList;
import java.util.List;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoExpression.TursoTableReference;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;

public final class TursoCommon {

    private TursoCommon() {}

    public static String getRandomCollate() {
        return Randomly.fromOptions(
            " COLLATE BINARY",
            " COLLATE RTRIM",
            " COLLATE NOCASE"/* , " COLLATE UINT" */
        );
    }

    public static String getCheckConstraint(
        TursoGlobalState globalState,
        List<TursoColumn> columns
    ) {
        TursoExpression expression = new TursoExpressionGenerator(globalState)
            .setColumns(columns)
            .generateExpression();
        return " CHECK ( " + TursoVisitor.asString(expression) + ")";
    }

    // TODO: refactor others to use this method
    // https://www.sqlite.org/syntax/ordering-term.html
    public static String getOrderingTerm(
        List<TursoColumn> columns,
        TursoGlobalState globalState
    ) {
        TursoExpression randExpr = new TursoExpressionGenerator(globalState)
            .setColumns(columns)
            .generateExpression();
        StringBuilder sb = new StringBuilder(TursoVisitor.asString(randExpr));
        sb.append(" ");
        if (Randomly.getBoolean()) {
            sb.append(TursoCommon.getRandomCollate());
        }
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" ASC");
            } else {
                sb.append(" DESC");
            }
        }
        return sb.toString();
    }

    public static String getIndexedClause(String indexName) {
        StringBuilder sb = new StringBuilder();
        sb.append("NOT INDEXED");
        return sb.toString();
    }

    public static String getFreeTableName(TursoSchema s) {
        int nr = 0;
        String[] name = new String[1];
        do {
            name[0] = DBMSCommon.createTableName(nr++);
        } while (
            s
                .getDatabaseTables()
                .stream()
                .anyMatch(tab -> tab.getName().contentEquals(name[0]))
        );
        return name[0];
    }

    public static String getFreeViewName(TursoSchema s) {
        int nr = 0;
        String[] name = new String[1];
        do {
            name[0] = "v" + nr++;
        } while (
            s
                .getDatabaseTables()
                .stream()
                .anyMatch(tab -> tab.getName().contentEquals(name[0]))
        );
        return name[0];
    }

    public static String getFreeColumnName(TursoTable t) {
        List<TursoColumn> indexNames = t.getColumns();
        final String[] candidateName = new String[1];
        do {
            candidateName[0] = DBMSCommon.createColumnName(
                (int) Randomly.getNotCachedInteger(0, 100)
            );
        } while (
            indexNames
                .stream()
                .anyMatch(c -> c.getName().contentEquals(candidateName[0]))
        );
        return candidateName[0];
    }

    public static String getOrderByAsString(
        List<TursoColumn> columns,
        TursoGlobalState globalState
    ) {
        StringBuilder sb = new StringBuilder();
        TursoExpressionGenerator gen = new TursoExpressionGenerator(
            globalState
        ).setColumns(columns);
        sb.append(" ORDER BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(TursoVisitor.asString(gen.generateOrderingTerm()));
        }
        return sb.toString();
    }

    public static List<TursoExpression> getOrderBy(
        List<TursoColumn> columns,
        TursoGlobalState globalState
    ) {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(
            globalState
        ).setColumns(columns);
        List<TursoExpression> list = new ArrayList<>();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            list.add(gen.generateOrderingTerm());
        }
        return list;
    }

    public static TursoColumn createColumn(int i) {
        return new TursoColumn(
            DBMSCommon.createColumnName(i),
            TursoDataType.NONE,
            false,
            false,
            null
        );
    }

    public static List<TursoExpression> getTableRefs(
        List<TursoTable> tables,
        TursoSchema s
    ) {
        List<TursoExpression> tableRefs = new ArrayList<>();
        for (TursoTable t : tables) {
            tableRefs.add(new TursoTableReference(t));
        }
        return tableRefs;
    }
}
