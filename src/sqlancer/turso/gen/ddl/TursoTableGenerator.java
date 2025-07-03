package sqlancer.turso.gen.ddl;

import java.util.ArrayList;
import java.util.List;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoOracleFactory;
import sqlancer.turso.gen.TursoColumnBuilder;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.schema.TursoSchema.TursoColumn;

/**
 * See https://www.sqlite.org/lang_createtable.html
 *
 * TODO What's missing:
 * <ul>
 * <li>CREATE TABLE ... AS SELECT Statements</li>
 * </ul>
 */
public class TursoTableGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private int columnId;
    private boolean containsPrimaryKey;
    private boolean containsAutoIncrement;
    private final List<String> columnNames = new ArrayList<>();
    private final List<TursoColumn> columns = new ArrayList<>();
    private final TursoGlobalState globalState;

    public TursoTableGenerator(String tableName, TursoGlobalState globalState) {
        this.tableName = tableName;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter createRandomTableStatement(
        TursoGlobalState globalState
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
        TursoGlobalState globalState
    ) {
        TursoTableGenerator sqLite3TableGenerator = new TursoTableGenerator(
            tableName,
            globalState
        );
        sqLite3TableGenerator.start();
        ExpectedErrors errors = new ExpectedErrors();
        TursoErrors.addTableManipulationErrors(errors);
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
        sb.append("CREATE TABLE IF NOT EXISTS ");
        if (Randomly.getBoolean()) {
            sb.append(" ");
        }
        sb.append(tableName);
        sb.append(" (");
        boolean allowPrimaryKeyInColumn = Randomly.getBoolean();
        int nrColumns = 1 + Randomly.smallNumber();
        for (int i = 0; i < nrColumns; i++) {
            columns.add(
                TursoColumn.createDummy(DBMSCommon.createColumnName(i))
            );
        }
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String columnName = DBMSCommon.createColumnName(columnId);
            TursoColumnBuilder columnBuilder =
                new TursoColumnBuilder().allowPrimaryKey(
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

        if (
            globalState.getDbmsSpecificOptions().testCheckConstraints &&
            globalState.getDbmsSpecificOptions().oracles !=
            TursoOracleFactory.PQS &&
            /*
             * we are currently lacking a parser to
             * read column definitions, and would
             * interpret a COLLATE in the check
             * constraint as belonging to the column
             */
            Randomly.getBooleanWithRatherLowProbability()
        ) {
            sb.append(TursoCommon.getCheckConstraint(globalState, columns));
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
