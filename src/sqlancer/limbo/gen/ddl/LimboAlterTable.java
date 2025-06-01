package sqlancer.limbo.gen.ddl;

import java.sql.SQLException;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.gen.LimboColumnBuilder;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public class LimboAlterTable {

    private final StringBuilder sb = new StringBuilder();
    private final LimboGlobalState globalState;

    public static SQLQueryAdapter alterTable(LimboGlobalState globalState) throws SQLException {
        LimboAlterTable alterTable = new LimboAlterTable(globalState);
        return alterTable.getQuery(globalState.getSchema(), alterTable);
    }

    private enum Option {
        RENAME_TABLE, RENAME_COLUMN, ADD_COLUMN
    }

    public LimboAlterTable(LimboGlobalState globalState) {
        this.globalState = globalState;
    }

    private SQLQueryAdapter getQuery(LimboSchema s, LimboAlterTable alterTable) throws AssertionError {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("error in view");
        errors.add("no such column"); // trigger
        errors.add("error in trigger"); // trigger

        errors.add("operator prohibited in generated columns");
        errors.add("subqueries prohibited in generated columns");
        errors.add("duplicate column name");
        errors.add("non-deterministic functions prohibited in generated columns");
        errors.add("non-deterministic functions prohibited in CHECK constraints");
        errors.add("second argument to likelihood");
        errors.add("subqueries prohibited in CHECK constraints");
        errors.add("subqueries prohibited in index expressions");
        errors.add("parser stack overflow");
        Option option = Randomly.fromOptions(Option.values());
        LimboTable t = s.getRandomTableOrBailout(tab -> !tab.isView() && !tab.isVirtual() && !tab.isReadOnly());
        sb.append("ALTER TABLE ");
        sb.append(t.getName());
        switch (option) {
        case RENAME_TABLE:
            sb.append(" RENAME TO ");
            sb.append(LimboCommon.getFreeTableName(s));
            break;
        case RENAME_COLUMN:
            LimboColumn c = t.getRandomColumn();
            sb.append(" RENAME COLUMN ");
            sb.append(c.getName());
            sb.append(" TO ");
            sb.append(LimboCommon.getFreeColumnName(t));
            break;
        case ADD_COLUMN:
            sb.append(" ADD COLUMN ");
            String name = LimboCommon.getFreeColumnName(t);
            // The column may not have a PRIMARY KEY or UNIQUE constraint.
            // The column may not have a default value of CURRENT_TIME, CURRENT_DATE,
            // CURRENT_TIMESTAMP, or an expression in parentheses.
            // If a NOT NULL constraint is specified, then the column must have a default
            // value other than NULL.
            // If foreign key constraints are enabled and a column with a REFERENCES clause
            // is added, the column must have a default value of NULL.
            sb.append(new LimboColumnBuilder().allowPrimaryKey(false).allowUnique(false).allowNotNull(false)
                    .allowDefaultValue(false).createColumn(name, globalState, t.getColumns()));
            errors.add("subqueries prohibited in CHECK constraints");
            errors.add("Cannot add a NOT NULL column with default value NULL");
            errors.add("unsupported frame specification");
            break;
        default:
            throw new AssertionError();
        }
        return new SQLQueryAdapter(alterTable.sb.toString(), errors, true);
    }

}
