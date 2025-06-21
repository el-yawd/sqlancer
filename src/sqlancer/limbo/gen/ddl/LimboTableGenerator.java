package sqlancer.limbo.gen.ddl;

import java.util.ArrayList;
import java.util.List;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboOracleFactory;
import sqlancer.limbo.gen.LimboColumnBuilder;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;

/**
 * See https://www.sqlite.org/lang_createtable.html
 *
 * TODO What's missing:
 * <ul>
 * <li>CREATE TABLE ... AS SELECT Statements</li>
 * </ul>
 */
public class LimboTableGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private int columnId;
    private boolean containsPrimaryKey;
    private boolean containsAutoIncrement;
    private final List<String> columnNames = new ArrayList<>();
    private final List<LimboColumn> columns = new ArrayList<>();
    private final LimboGlobalState globalState;

    public LimboTableGenerator(String tableName, LimboGlobalState globalState) {
        this.tableName = tableName;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter createRandomTableStatement(
        LimboGlobalState globalState
    ) {
        if (
            globalState.getSchema().getTables().getTables().size() >
            globalState.getDbmsSpecificOptions().maxNumTables
        ) {
            throw new IgnoreMeException();
        }
        return createTableStatement(
            globalState.getSchema().getFreeTableName(),
            globalState
        );
    }

    public static SQLQueryAdapter createTableStatement(
        String tableName,
        LimboGlobalState globalState
    ) {
        LimboTableGenerator sqLite3TableGenerator = new LimboTableGenerator(
            tableName,
            globalState
        );
        sqLite3TableGenerator.start();
        ExpectedErrors errors = new ExpectedErrors();
        LimboErrors.addTableManipulationErrors(errors);
        errors.add(
            "second argument to likelihood() must be a constant between 0.0 and 1.0"
        );
        errors.add(
            "non-deterministic functions prohibited in generated columns"
        );
        errors.add("subqueries prohibited in generated columns");
        errors.add("parser stack overflow");
        errors.add("malformed JSON");
        errors.add("JSON cannot hold BLOB values");
        return new SQLQueryAdapter(
            sqLite3TableGenerator.sb.toString(),
            errors,
            true
        );
    }

    public void start() {
        sb.append("CREATE TABLE ");
        if (Randomly.getBoolean()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(tableName);
        sb.append(" (");
        boolean allowPrimaryKeyInColumn = Randomly.getBoolean();
        int nrColumns = 1 + Randomly.smallNumber();
        for (int i = 0; i < nrColumns; i++) {
            columns.add(
                LimboColumn.createDummy(DBMSCommon.createColumnName(i))
            );
        }
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String columnName = DBMSCommon.createColumnName(columnId);
            LimboColumnBuilder columnBuilder = new LimboColumnBuilder()
                .allowPrimaryKey(
                    allowPrimaryKeyInColumn && !containsPrimaryKey
                );
            sb.append(
                columnBuilder.createColumn(columnName, globalState, columns)
            );
            sb.append(" ");
            if (columnBuilder.isContainsAutoIncrement()) {
                this.containsAutoIncrement = true;
            }
            if (columnBuilder.isContainsPrimaryKey()) {
                this.containsPrimaryKey = true;
            }

            columnNames.add(columnName);
            columnId++;
        }
        if (!containsPrimaryKey && Randomly.getBooleanWithSmallProbability()) {
            addColumnConstraints("PRIMARY KEY");
            containsPrimaryKey = true;
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                addColumnConstraints("UNIQUE");
            }
        }

        if (
            globalState.getDbmsSpecificOptions().testCheckConstraints &&
            globalState.getDbmsSpecificOptions().oracles !=
            LimboOracleFactory.PQS &&
            /*
             * we are currently lacking a parser to
             * read column definitions, and would
             * interpret a COLLATE in the check
             * constraint as belonging to the column
             */
            Randomly.getBooleanWithRatherLowProbability()
        ) {
            sb.append(LimboCommon.getCheckConstraint(globalState, columns));
        }

        sb.append(")");
    }

    private void addColumnConstraints(String s) {
        sb.append(", " + s + " (");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(Randomly.fromList(columnNames));
            if (Randomly.getBoolean()) {
                sb.append(Randomly.fromOptions(" ASC", " DESC"));
            }
        }
        sb.append(")");
    }
}
