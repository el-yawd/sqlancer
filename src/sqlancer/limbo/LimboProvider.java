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
import sqlancer.limbo.gen.LimboAnalyzeGenerator;
import sqlancer.limbo.gen.LimboCreateVirtualRtreeTabelGenerator;
import sqlancer.limbo.gen.LimboExplainGenerator;
import sqlancer.limbo.gen.LimboPragmaGenerator;
import sqlancer.limbo.gen.LimboReindexGenerator;
import sqlancer.limbo.gen.LimboTransactionGenerator;
import sqlancer.limbo.gen.LimboVacuumGenerator;
import sqlancer.limbo.gen.LimboVirtualFTSTableCommandGenerator;
import sqlancer.limbo.gen.ddl.LimboAlterTable;
import sqlancer.limbo.gen.ddl.LimboCreateTriggerGenerator;
import sqlancer.limbo.gen.ddl.LimboCreateVirtualFTSTableGenerator;
import sqlancer.limbo.gen.ddl.LimboDropIndexGenerator;
import sqlancer.limbo.gen.ddl.LimboDropTableGenerator;
import sqlancer.limbo.gen.ddl.LimboIndexGenerator;
import sqlancer.limbo.gen.ddl.LimboTableGenerator;
import sqlancer.limbo.gen.ddl.LimboViewGenerator;
import sqlancer.limbo.gen.dml.LimboDeleteGenerator;
import sqlancer.limbo.gen.dml.LimboInsertGenerator;
import sqlancer.limbo.gen.dml.LimboStatTableGenerator;
import sqlancer.limbo.gen.dml.LimboUpdateGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

@AutoService(DatabaseProvider.class)
public class LimboProvider
    extends SQLProviderAdapter<LimboGlobalState, LimboOptions> {

    public static boolean allowFloatingPointFp = true;
    public static boolean mustKnowResult;

    // PRAGMAS to achieve good performance
    private static final List<String> DEFAULT_PRAGMAS = Arrays.asList(
        "PRAGMA cache_size = 50000;",
        "PRAGMA temp_store=MEMORY;",
        "PRAGMA synchronous=off;"
    );

    public LimboProvider() {
        super(LimboGlobalState.class, LimboOptions.class);
    }

    public enum Action implements AbstractAction<LimboGlobalState> {
        PRAGMA(LimboPragmaGenerator::insertPragma), // 0
        CREATE_INDEX(LimboIndexGenerator::insertIndex), // 1
        CREATE_VIEW(LimboViewGenerator::generate), // 2
        CREATE_TRIGGER(LimboCreateTriggerGenerator::create), // 3
        CREATE_TABLE(LimboTableGenerator::createRandomTableStatement), // 4
        CREATE_VIRTUALTABLE(
            LimboCreateVirtualFTSTableGenerator::createRandomTableStatement
        ), // 5
        CREATE_RTREETABLE(
            LimboCreateVirtualRtreeTabelGenerator::createRandomTableStatement
        ), // 6
        INSERT(LimboInsertGenerator::insertRow), // 7
        DELETE(LimboDeleteGenerator::deleteContent), // 8
        ALTER(LimboAlterTable::alterTable), // 9
        UPDATE(LimboUpdateGenerator::updateRow), // 10
        DROP_INDEX(LimboDropIndexGenerator::dropIndex), // 11
        DROP_TABLE(LimboDropTableGenerator::dropTable), // 12
        DROP_VIEW(LimboViewGenerator::dropView), // 13
        VACUUM(LimboVacuumGenerator::executeVacuum), // 14
        REINDEX(LimboReindexGenerator::executeReindex), // 15
        ANALYZE(LimboAnalyzeGenerator::generateAnalyze), // 16
        EXPLAIN(LimboExplainGenerator::explain), // 17
        CHECK_RTREE_TABLE(g -> {
            LimboTable table = g
                .getSchema()
                .getRandomTableOrBailout(t -> t.getName().startsWith("r"));
            String format = String.format(
                "SELECT rtreecheck('%s');",
                table.getName()
            );
            return new SQLQueryAdapter(
                format,
                ExpectedErrors.from("The database file is locked")
            );
        }), // 18
        VIRTUAL_TABLE_ACTION(LimboVirtualFTSTableCommandGenerator::create), // 19
        MANIPULATE_STAT_TABLE(LimboStatTableGenerator::getQuery), // 20
        TRANSACTION_START(LimboTransactionGenerator::generateBeginTransaction) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        }, // 21
        ROLLBACK_TRANSACTION(
            LimboTransactionGenerator::generateRollbackTransaction
        ) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        }, // 22
        COMMIT(LimboTransactionGenerator::generateCommit) {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        }; // 23

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
        FTS,
        RTREE,
    }

    private static int mapActions(LimboGlobalState globalState, Action a) {
        int nrPerformed = 0;
        Randomly r = globalState.getRandomly();
        switch (a) {
            case CREATE_VIEW:
                nrPerformed = r.getInteger(0, 2);
                break;
            case DELETE:
            case DROP_VIEW:
            case DROP_INDEX:
                nrPerformed = r.getInteger(0, 0);
                break;
            case ALTER:
                nrPerformed = r.getInteger(0, 0);
                break;
            case EXPLAIN:
            case CREATE_TRIGGER:
            case DROP_TABLE:
                nrPerformed = r.getInteger(0, 0);
                break;
            case VACUUM:
            case CHECK_RTREE_TABLE:
                nrPerformed = r.getInteger(0, 3);
                break;
            case INSERT:
                nrPerformed = r.getInteger(
                    0,
                    globalState.getOptions().getMaxNumberInserts()
                );
                break;
            case MANIPULATE_STAT_TABLE:
                nrPerformed = r.getInteger(0, 5);
                break;
            case CREATE_INDEX:
                nrPerformed = r.getInteger(0, 5);
                break;
            case VIRTUAL_TABLE_ACTION:
            case UPDATE:
                nrPerformed = r.getInteger(0, 30);
                break;
            case PRAGMA:
                nrPerformed = r.getInteger(0, 20);
                break;
            case CREATE_TABLE:
            case CREATE_VIRTUALTABLE:
            case CREATE_RTREETABLE:
                nrPerformed = 0;
                break;
            case TRANSACTION_START:
            case REINDEX:
            case ANALYZE:
            case ROLLBACK_TRANSACTION:
            case COMMIT:
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
            // addSensiblePragmaDefaults(globalState);
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
        if (!globalState.getDbmsSpecificOptions().testFts) {
            options.remove(TableType.FTS);
        }
        if (!globalState.getDbmsSpecificOptions().testRtree) {
            options.remove(TableType.RTREE);
        }
        switch (Randomly.fromList(options)) {
            case NORMAL:
                String tableName = DBMSCommon.createTableName(i);
                tableQuery = LimboTableGenerator.createTableStatement(
                    tableName,
                    globalState
                );
                break;
            case FTS:
                String ftsTableName = "v" + DBMSCommon.createTableName(i);
                tableQuery =
                    LimboCreateVirtualFTSTableGenerator.createTableStatement(
                        ftsTableName,
                        globalState.getRandomly()
                    );
                break;
            case RTREE:
                String rTreeTableName = "rt" + i;
                tableQuery =
                    LimboCreateVirtualRtreeTabelGenerator.createTableStatement(
                        rTreeTableName,
                        globalState
                    );
                break;
            default:
                throw new AssertionError();
        }
        return tableQuery;
    }

    private void addSensiblePragmaDefaults(LimboGlobalState globalState)
        throws Exception {
        List<String> pragmasToExecute = new ArrayList<>();
        if (!Randomly.getBooleanWithSmallProbability()) {
            pragmasToExecute.addAll(DEFAULT_PRAGMAS);
        }
        if (
            Randomly.getBoolean() &&
            globalState.getDbmsSpecificOptions().oracles !=
            LimboOracleFactory.PQS
        ) {
            // the PQS implementation currently assumes the default behavior of LIKE
            pragmasToExecute.add("PRAGMA case_sensitive_like=ON;");
        }
        if (
            Randomly.getBoolean() &&
            globalState.getDbmsSpecificOptions().oracles !=
            LimboOracleFactory.PQS
        ) {
            // the encoding has an influence how binary strings are cast
            pragmasToExecute.add(
                String.format(
                    "PRAGMA encoding = '%s';",
                    Randomly.fromOptions(
                        "UTF-8",
                        "UTF-16",
                        "UTF-16le",
                        "UTF-16be"
                    )
                )
            );
        }
        for (String s : pragmasToExecute) {
            globalState.executeStatement(new SQLQueryAdapter(s));
        }
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
