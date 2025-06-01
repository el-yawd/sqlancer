package sqlancer.limbo.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.LimboTableReference;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public final class LimboCommon {

    private LimboCommon() {
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions(" COLLATE BINARY", " COLLATE RTRIM", " COLLATE NOCASE"/* , " COLLATE UINT" */);
    }

    public static String getCheckConstraint(LimboGlobalState globalState, List<LimboColumn> columns) {
        LimboExpression expression = new LimboExpressionGenerator(globalState).setColumns(columns)
                .generateExpression();
        return " CHECK ( " + LimboVisitor.asString(expression) + ")";
    }

    // TODO: refactor others to use this method
    // https://www.sqlite.org/syntax/ordering-term.html
    public static String getOrderingTerm(List<LimboColumn> columns, LimboGlobalState globalState) {
        LimboExpression randExpr = new LimboExpressionGenerator(globalState).setColumns(columns)
                .generateExpression();
        StringBuilder sb = new StringBuilder(LimboVisitor.asString(randExpr));
        sb.append(" ");
        if (Randomly.getBoolean()) {
            sb.append(LimboCommon.getRandomCollate());
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
        if (Randomly.getBoolean()) {
            sb.append("INDEXED BY ");
            sb.append(indexName);
        } else {
            sb.append("NOT INDEXED");
        }
        return sb.toString();
    }

    public static String getFreeTableName(LimboSchema s) {
        int nr = 0;
        String[] name = new String[1];
        do {
            name[0] = DBMSCommon.createTableName(nr++);
        } while (s.getDatabaseTables().stream().anyMatch(tab -> tab.getName().contentEquals(name[0])));
        return name[0];
    }

    public static String getFreeViewName(LimboSchema s) {
        int nr = 0;
        String[] name = new String[1];
        do {
            name[0] = "v" + nr++;
        } while (s.getDatabaseTables().stream().anyMatch(tab -> tab.getName().contentEquals(name[0])));
        return name[0];
    }

    public static String getFreeIndexName(LimboSchema s) {
        List<String> indexNames = s.getIndexNames();
        String candidateName;
        do {
            candidateName = DBMSCommon.createIndexName((int) Randomly.getNotCachedInteger(0, 100));
        } while (indexNames.contains(candidateName));
        return candidateName;
    }

    public static String getFreeColumnName(LimboTable t) {
        List<LimboColumn> indexNames = t.getColumns();
        final String[] candidateName = new String[1];
        do {
            candidateName[0] = DBMSCommon.createColumnName((int) Randomly.getNotCachedInteger(0, 100));
        } while (indexNames.stream().anyMatch(c -> c.getName().contentEquals(candidateName[0])));
        return candidateName[0];
    }

    public static String getOrderByAsString(List<LimboColumn> columns, LimboGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        LimboExpressionGenerator gen = new LimboExpressionGenerator(globalState).setColumns(columns);
        sb.append(" ORDER BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(LimboVisitor.asString(gen.generateOrderingTerm()));
        }
        return sb.toString();
    }

    public static List<LimboExpression> getOrderBy(List<LimboColumn> columns, LimboGlobalState globalState) {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(globalState).setColumns(columns);
        List<LimboExpression> list = new ArrayList<>();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            list.add(gen.generateOrderingTerm());
        }
        return list;

    }

    public static LimboColumn createColumn(int i) {
        return new LimboColumn(DBMSCommon.createColumnName(i), LimboDataType.NONE, false, false, null);
    }

    public static List<LimboExpression> getTableRefs(List<LimboTable> tables, LimboSchema s) {
        List<LimboExpression> tableRefs = new ArrayList<>();
        for (LimboTable t : tables) {
            LimboTableReference tableRef;
            if (Randomly.getBooleanWithSmallProbability() && !s.getIndexNames().isEmpty()) {
                tableRef = new LimboTableReference(s.getRandomIndexOrBailout(), t);
            } else {
                tableRef = new LimboTableReference(t);
            }
            tableRefs.add(tableRef);
        }
        return tableRefs;
    }

}
