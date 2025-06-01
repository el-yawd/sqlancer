package sqlancer.limbo.gen;

import java.util.ArrayList;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;

public final class LimboCreateVirtualRtreeTabelGenerator {

    private LimboCreateVirtualRtreeTabelGenerator() {
    }

    public static SQLQueryAdapter createRandomTableStatement(LimboGlobalState globalState) {
        if (globalState.getSchema().getTables().getTables()
                .size() > globalState.getDbmsSpecificOptions().maxNumTables) {
            throw new IgnoreMeException();
        }
        return createTableStatement(globalState.getSchema().getFreeRtreeTableName(), globalState);
    }

    public static SQLQueryAdapter createTableStatement(String rTreeTableName, LimboGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        List<LimboColumn> columns = new ArrayList<>();
        StringBuilder sb = new StringBuilder("CREATE VIRTUAL TABLE ");
        sb.append(rTreeTableName);
        sb.append(" USING ");
        sb.append(Randomly.fromOptions("rtree_i32", "rtree"));
        sb.append("(");
        int size = 3 + Randomly.smallNumber();
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            LimboColumn c = LimboCommon.createColumn(i);
            columns.add(c);
            sb.append(c.getName());
        }
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            sb.append(", ");
            sb.append("+");
            String columnName = DBMSCommon.createColumnName(size + i);
            LimboColumnBuilder columnBuilder = new LimboColumnBuilder().allowPrimaryKey(false).allowNotNull(false)
                    .allowUnique(false).allowCheck(false);
            String c = columnBuilder.createColumn(columnName, globalState, columns);
            sb.append(c);
            sb.append(" ");
        }
        errors.add("virtual tables cannot use computed columns");
        sb.append(")");

        errors.add("Wrong number of columns for an rtree table");
        errors.add("Too many columns for an rtree table");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
