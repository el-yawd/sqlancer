package sqlancer.limbo;

import com.google.auto.service.AutoService;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.limbo.gen.LimboExplainGenerator;
import sqlancer.limbo.gen.LimboPragmaGenerator;
import sqlancer.limbo.gen.LimboTransactionGenerator;
import sqlancer.limbo.gen.ddl.LimboAlterTable;
import sqlancer.limbo.gen.ddl.LimboDropTableGenerator;
import sqlancer.limbo.gen.ddl.LimboTableGenerator;
import sqlancer.limbo.gen.dml.LimboDeleteGenerator;
import sqlancer.limbo.gen.dml.LimboInsertGenerator;
import sqlancer.limbo.gen.dml.LimboUpdateGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

@AutoService(DatabaseProvider.class)
public class LimboProvider
    extends SQLProviderAdapter<LimboGlobalState, LimboOptions> {

    public static boolean allowFloatingPointFp = true;
    public static boolean mustKnowResult;

    public LimboProvider() {
        super(LimboGlobalState.class, LimboOptions.class);
    }

    public enum Action implements AbstractAction<LimboGlobalState> {
        PRAGMA(LimboPragmaGenerator::insertPragma),
        CREATE_TABLE(LimboTableGenerator::createRandomTableStatement),
        INSERT(LimboInsertGenerator::insertRow),
        DELETE(LimboDeleteGenerator::deleteContent),
        ALTER(LimboAlterTable::alterTable),
        UPDATE(LimboUpdateGenerator::updateRow),
        DROP_TABLE(LimboDropTableGenerator::dropTable),
        EXPLAIN(LimboExplainGenerator::explain);

        private final SQLQueryProvider<LimboGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<LimboGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(LimboGlobalState state)
            throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private enum TableType {
        NORMAL,
    }

    private static int mapActions(LimboGlobalState globalState, Action a) {
        int nrPerformed = 0;
        Randomly r = globalState.getRandomly();
        switch (a) {
            case DELETE:
                nrPerformed = r.getInteger(0, 0);
                break;
            case ALTER:
                nrPerformed = r.getInteger(0, 0);
                break;
            case EXPLAIN:
            case DROP_TABLE:
                nrPerformed = r.getInteger(0, 0);
                break;
            case INSERT:
                nrPerformed = r.getInteger(
                    0,
                    globalState.getOptions().getMaxNumberInserts()
                );
                break;
            case UPDATE:
                nrPerformed = r.getInteger(0, 30);
                break;
            case PRAGMA:
                nrPerformed = r.getInteger(0, 20);
                break;
            case CREATE_TABLE:
            default:
                nrPerformed = r.getInteger(1, 10);
                break;
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(LimboGlobalState globalState)
        throws Exception {
        Randomly r = new Randomly(LimboSpecialStringGenerator::generate);
        globalState.setRandomly(r);
        if (globalState.getDbmsSpecificOptions().generateDatabase) {
            int nrTablesToCreate = 1;
            if (Randomly.getBoolean()) {
                nrTablesToCreate++;
            }
            while (Randomly.getBooleanWithSmallProbability()) {
                nrTablesToCreate++;
            }
            int i = 0;

            do {
                SQLQueryAdapter tableQuery = getTableQuery(globalState, i++);
                globalState.executeStatement(tableQuery);
            } while (
                globalState.getSchema().getDatabaseTables().size() <
                nrTablesToCreate
            );
            assert globalState.getSchema().getTables().getTables().size() ==
            nrTablesToCreate;
            checkTablesForGeneratedColumnLoops(globalState);
            if (
                globalState.getDbmsSpecificOptions().testDBStats &&
                Randomly.getBooleanWithSmallProbability()
            ) {
                SQLQueryAdapter tableQuery = new SQLQueryAdapter(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS stat USING dbstat(main)"
                );
                globalState.executeStatement(tableQuery);
            }
            StatementExecutor<LimboGlobalState, Action> se =
                new StatementExecutor<>(
                    globalState,
                    Action.values(),
                    LimboProvider::mapActions,
                    q -> {
                        if (
                            q.couldAffectSchema() &&
                            globalState
                                .getSchema()
                                .getDatabaseTables()
                                .isEmpty()
                        ) {
                            throw new IgnoreMeException();
                        }
                    }
                );
            se.executeStatements();

            SQLQueryAdapter query = LimboTransactionGenerator.generateCommit(
                globalState
            );
            globalState.executeStatement(query);

            // also do an abort for DEFERRABLE INITIALLY DEFERRED
            query = LimboTransactionGenerator.generateRollbackTransaction(
                globalState
            );
            globalState.executeStatement(query);
        }
    }

    private void checkTablesForGeneratedColumnLoops(
        LimboGlobalState globalState
    ) throws Exception {
        for (LimboTable table : globalState.getSchema().getDatabaseTables()) {
            SQLQueryAdapter q = new SQLQueryAdapter(
                "SELECT * FROM " + table.getName(),
                ExpectedErrors.from(
                    "needs an odd number of arguments",
                    " requires an even number of arguments",
                    "generated column loop",
                    "integer overflow",
                    "malformed JSON",
                    "JSON cannot hold BLOB values",
                    "JSON path error",
                    "labels must be TEXT",
                    "table does not support scanning"
                )
            );
            if (!q.execute(globalState)) {
                throw new IgnoreMeException();
            }
        }
    }

    private SQLQueryAdapter getTableQuery(LimboGlobalState globalState, int i)
        throws AssertionError {
        SQLQueryAdapter tableQuery;
        List<TableType> options = new ArrayList<>(
            Arrays.asList(TableType.values())
        );
        switch (Randomly.fromList(options)) {
            case NORMAL:
                String tableName = DBMSCommon.createTableName(i);
                tableQuery = LimboTableGenerator.createTableStatement(
                    tableName,
                    globalState
                );
                break;
            default:
                throw new AssertionError();
        }
        return tableQuery;
    }

    @Override
    public SQLConnection createDatabase(LimboGlobalState globalState)
        throws SQLException {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, globalState.getDatabaseName() + ".db");
        if (
            dataBase.exists() &&
            ((LimboGlobalState) globalState).getDbmsSpecificOptions()
                .deleteIfExists
        ) {
            dataBase.delete();
        }
        String url = "jdbc:sqlite:" + dataBase.getAbsolutePath();

        Connection connection = DriverManager.getConnection(url);
        return new SQLConnection(connection);
    }

    @Override
    public String getDBMSName() {
        return "limbo";
    }

    @Override
    public String getQueryPlan(String selectStr, LimboGlobalState globalState)
        throws Exception {
        String queryPlan = "";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(selectStr);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // Set up the expected errors for NoREC oracle.
        ExpectedErrors errors = new ExpectedErrors();
        LimboErrors.addExpectedExpressionErrors(errors);
        LimboErrors.addMatchQueryErrors(errors);
        LimboErrors.addQueryErrors(errors);
        LimboErrors.addInsertUpdateErrors(errors);

        SQLQueryAdapter q = new SQLQueryAdapter(
            LimboExplainGenerator.explain(selectStr),
            errors
        );
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    queryPlan += rs.getString(4) + ";";
                }
            }
        } catch (SQLException | AssertionError e) {
            queryPlan = "";
        }
        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, LimboGlobalState globalState)
        throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(
                globalState
            );
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    protected boolean addRowsToAllTables(LimboGlobalState globalState)
        throws Exception {
        List<LimboTable> tablesNoRow = globalState
            .getSchema()
            .getDatabaseTables()
            .stream()
            .filter(t -> t.getNrRows(globalState) == 0)
            .collect(Collectors.toList());
        for (LimboTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = LimboInsertGenerator.insertRow(
                globalState,
                table
            );
            globalState.executeStatement(queryAddRows);
        }

        return true;
    }
}
