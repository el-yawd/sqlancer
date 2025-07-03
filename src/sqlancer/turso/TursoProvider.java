package sqlancer.turso;

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
import sqlancer.turso.gen.TursoExplainGenerator;
import sqlancer.turso.gen.TursoPragmaGenerator;
import sqlancer.turso.gen.TursoTransactionGenerator;
import sqlancer.turso.gen.ddl.TursoAlterTable;
import sqlancer.turso.gen.ddl.TursoDropTableGenerator;
import sqlancer.turso.gen.ddl.TursoTableGenerator;
import sqlancer.turso.gen.dml.TursoDeleteGenerator;
import sqlancer.turso.gen.dml.TursoInsertGenerator;
import sqlancer.turso.gen.dml.TursoUpdateGenerator;
import sqlancer.turso.schema.TursoSchema.TursoTable;

@AutoService(DatabaseProvider.class)
public class TursoProvider
    extends SQLProviderAdapter<TursoGlobalState, TursoOptions> {

    public static boolean allowFloatingPointFp = true;
    public static boolean mustKnowResult;

    public TursoProvider() {
        super(TursoGlobalState.class, TursoOptions.class);
    }

    public enum Action implements AbstractAction<TursoGlobalState> {
        PRAGMA(TursoPragmaGenerator::insertPragma),
        CREATE_TABLE(TursoTableGenerator::createRandomTableStatement),
        INSERT(TursoInsertGenerator::insertRow),
        DELETE(TursoDeleteGenerator::deleteContent),
        ALTER(TursoAlterTable::alterTable),
        UPDATE(TursoUpdateGenerator::updateRow),
        DROP_TABLE(TursoDropTableGenerator::dropTable),
        EXPLAIN(TursoExplainGenerator::explain);

        private final SQLQueryProvider<TursoGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TursoGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TursoGlobalState state)
            throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private enum TableType {
        NORMAL,
    }

    private static int mapActions(TursoGlobalState globalState, Action a) {
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
    public void generateDatabase(TursoGlobalState globalState)
        throws Exception {
        System.out.println("Generating database...");
        Randomly r = new Randomly(TursoSpecialStringGenerator::generate);
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
                System.out.println(
                    "Current Size: " +
                    globalState.getSchema().getDatabaseTables().size() +
                    ", Tables to Create: " +
                    nrTablesToCreate
                );
                SQLQueryAdapter tableQuery = getTableQuery(globalState, i++);
                globalState.executeStatement(tableQuery);
            } while (
                globalState.getSchema().getDatabaseTables().size() <
                nrTablesToCreate
            );
            System.out.println("Debug 3");
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
            StatementExecutor<TursoGlobalState, Action> se =
                new StatementExecutor<>(
                    globalState,
                    Action.values(),
                    TursoProvider::mapActions,
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

            System.out.println("Debug 4");
            se.executeStatements();

            System.out.println("Debug 5");
            SQLQueryAdapter query = TursoTransactionGenerator.generateCommit(
                globalState
            );
            globalState.executeStatement(query);

            System.out.println("Debug 6");
            // also do an abort for DEFERRABLE INITIALLY DEFERRED
            query = TursoTransactionGenerator.generateRollbackTransaction(
                globalState
            );
            globalState.executeStatement(query);

            System.out.println("Debug 7");
        }
    }

    private void checkTablesForGeneratedColumnLoops(
        TursoGlobalState globalState
    ) throws Exception {
        for (TursoTable table : globalState.getSchema().getDatabaseTables()) {
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

    private SQLQueryAdapter getTableQuery(TursoGlobalState globalState, int i)
        throws AssertionError {
        SQLQueryAdapter tableQuery;
        List<TableType> options = new ArrayList<>(
            Arrays.asList(TableType.values())
        );
        switch (Randomly.fromList(options)) {
            case NORMAL:
                String tableName = DBMSCommon.createTableName(i);
                tableQuery = TursoTableGenerator.createTableStatement(
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
    public SQLConnection createDatabase(TursoGlobalState globalState)
        throws SQLException {
        File dir = new File("." + File.separator + "databases");
        if (!dir.exists()) {
            dir.mkdir();
        }
        File dataBase = new File(dir, globalState.getDatabaseName() + ".db");
        if (
            dataBase.exists() &&
            ((TursoGlobalState) globalState).getDbmsSpecificOptions()
                .deleteIfExists
        ) {
            dataBase.delete();
        }
        String url = "jdbc:turso:" + dataBase.getAbsolutePath();

        Connection connection = DriverManager.getConnection(url);
        return new SQLConnection(connection);
    }

    @Override
    public String getDBMSName() {
        return "turso";
    }

    @Override
    public String getQueryPlan(String selectStr, TursoGlobalState globalState)
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
        TursoErrors.addExpectedExpressionErrors(errors);
        TursoErrors.addMatchQueryErrors(errors);
        TursoErrors.addQueryErrors(errors);
        TursoErrors.addInsertUpdateErrors(errors);

        SQLQueryAdapter q = new SQLQueryAdapter(
            TursoExplainGenerator.explain(selectStr),
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
    protected void executeMutator(int index, TursoGlobalState globalState)
        throws Exception {
        SQLQueryAdapter queryMutateTable = Action.values()[index].getQuery(
                globalState
            );
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    protected boolean addRowsToAllTables(TursoGlobalState globalState)
        throws Exception {
        List<TursoTable> tablesNoRow = globalState
            .getSchema()
            .getDatabaseTables()
            .stream()
            .filter(t -> t.getNrRows(globalState) == 0)
            .collect(Collectors.toList());
        for (TursoTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = TursoInsertGenerator.insertRow(
                globalState,
                table
            );
            globalState.executeStatement(queryAddRows);
        }

        return true;
    }
}
