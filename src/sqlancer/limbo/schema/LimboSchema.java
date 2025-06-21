package sqlancer.limbo.schema;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;
import sqlancer.limbo.schema.LimboSchema.LimboTable;
import sqlancer.limbo.schema.LimboSchema.LimboTable.TableKind;

public class LimboSchema extends AbstractSchema<LimboGlobalState, LimboTable> {

    /**
     * All possible aliases for the rowid column.
     */
    public static final List<String> ROWID_STRINGS =
        Collections.unmodifiableList(Arrays.asList("rowid", "_rowid_", "oid"));
    private final List<String> indexNames;

    public List<String> getIndexNames() {
        return indexNames;
    }

    public String getRandomIndexOrBailout() {
        if (indexNames.isEmpty()) {
            throw new IgnoreMeException();
        } else {
            return Randomly.fromList(indexNames);
        }
    }

    public static class LimboColumn
        extends AbstractTableColumn<LimboTable, LimboDataType> {

        private final boolean isInteger; // "INTEGER" type, not "INT"
        private final LimboCollateSequence collate;
        boolean generated;
        private final boolean isPrimaryKey;

        public enum LimboCollateSequence {
            NOCASE,
            RTRIM,
            BINARY;

            public static LimboCollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public LimboColumn(
            String name,
            LimboDataType columnType,
            boolean isInteger,
            boolean isPrimaryKey,
            LimboCollateSequence collate
        ) {
            super(name, null, columnType);
            this.isInteger = isInteger;
            this.isPrimaryKey = isPrimaryKey;
            this.collate = collate;
            this.generated = false;
            assert !isInteger || columnType == LimboDataType.INT;
        }

        public LimboColumn(
            String rowId,
            LimboDataType columnType,
            boolean isInteger,
            LimboCollateSequence collate,
            boolean generated
        ) {
            this(rowId, columnType, isInteger, generated, collate);
            this.generated = generated;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isOnlyPrimaryKey() {
            return (
                isPrimaryKey &&
                getTable()
                    .getColumns()
                    .stream()
                    .filter(c -> c.isPrimaryKey())
                    .count() ==
                1
            );
        }

        // see https://www.sqlite.org/lang_createtable.html#rowid
        /**
         * If a table has a single column primary key and the declared type of that column is "INTEGER" and the table is
         * not a WITHOUT ROWID table, then the column is known as an INTEGER PRIMARY KEY.
         *
         * @return whether the column is an INTEGER PRIMARY KEY
         */
        public boolean isIntegerPrimaryKey() {
            return (
                isInteger && isOnlyPrimaryKey() && !getTable().hasWithoutRowid()
            );
        }

        public LimboCollateSequence getCollateSequence() {
            return collate;
        }

        public boolean isGenerated() {
            return generated;
        }

        public static LimboColumn createDummy(String name) {
            return new LimboColumn(name, LimboDataType.INT, false, false, null);
        }
    }

    public static LimboConstant getConstant(
        ResultSet randomRowValues,
        int columnIndex,
        LimboDataType valueType
    ) throws SQLException, AssertionError {
        Object value;
        LimboConstant constant;
        switch (valueType) {
            case INT:
                value = randomRowValues.getLong(columnIndex);
                constant = LimboConstant.createIntConstant((long) value);
                break;
            case REAL:
                value = randomRowValues.getDouble(columnIndex);
                if (!Double.isFinite((double) value)) {
                    // TODO: the JDBC driver seems to sometimes return infinity for NULL values
                    throw new IgnoreMeException();
                }
                constant = LimboConstant.createRealConstant((double) value);
                break;
            case TEXT:
            case NONE:
                value = randomRowValues.getString(columnIndex);
                constant = LimboConstant.createTextConstant((String) value);
                break;
            case BINARY:
                value = randomRowValues.getBytes(columnIndex);
                constant = LimboConstant.createBinaryConstant((byte[]) value);
                if (((byte[]) value).length == 0) {
                    // TODO: the JDBC driver seems to sometimes return a zero-length array for NULL values
                    throw new IgnoreMeException();
                }
                break;
            case NULL:
                return LimboConstant.createNullConstant();
            default:
                throw new AssertionError(valueType);
        }
        return constant;
    }

    public static class LimboTables
        extends AbstractTables<LimboTable, LimboColumn> {

        public LimboTables(List<LimboTable> tables) {
            super(tables);
        }

        public LimboRowValue getRandomRowValue(SQLConnection con)
            throws SQLException {
            String randomRow = String.format(
                "SELECT %s, %s FROM %s ORDER BY RANDOM() LIMIT 1",
                columnNamesAsString(
                    c ->
                        c.getTable().getName() +
                        "." +
                        c.getName() +
                        " AS " +
                        c.getTable().getName() +
                        c.getName()
                ),
                columnNamesAsString(
                    c ->
                        "typeof(" +
                        c.getTable().getName() +
                        "." +
                        c.getName() +
                        ")"
                ),
                tableNamesAsString()
            );
            Map<LimboColumn, LimboConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues;
                try {
                    randomRowValues = s.executeQuery(randomRow);
                } catch (SQLException e) {
                    throw new IgnoreMeException();
                }
                if (!randomRowValues.next()) {
                    throw new IgnoreMeException();
                    // throw new AssertionError("could not find random row! " + randomRow);
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    LimboColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(
                        column.getTable().getName() + column.getName()
                    );
                    assert columnIndex == i + 1;
                    String typeString = randomRowValues.getString(
                        columnIndex + getColumns().size()
                    );
                    LimboDataType valueType = getColumnType(typeString);
                    LimboConstant constant = getConstant(
                        randomRowValues,
                        columnIndex,
                        valueType
                    );
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new LimboRowValue(this, values);
            }
        }
    }

    public static class LimboTable
        extends AbstractRelationalTable<
            LimboColumn,
            TableIndex,
            LimboGlobalState
        > {

        // TODO: why does the SQLite implementation have no table indexes?

        public enum TableKind {
            MAIN,
            TEMP,
        }

        private final TableKind tableType;
        private LimboColumn rowid;
        private final boolean withoutRowid;
        private final boolean isVirtual;
        private final boolean isReadOnly;

        public LimboTable(
            String tableName,
            List<LimboColumn> columns,
            TableKind tableType,
            boolean withoutRowid,
            boolean isView,
            boolean isVirtual,
            boolean isReadOnly
        ) {
            super(tableName, columns, Collections.emptyList(), isView);
            this.tableType = tableType;
            this.withoutRowid = withoutRowid;
            this.isVirtual = isVirtual;
            this.isReadOnly = isReadOnly;
        }

        public boolean hasWithoutRowid() {
            return withoutRowid;
        }

        public void addRowid(LimboColumn rowid) {
            this.rowid = rowid;
        }

        public LimboColumn getRowid() {
            return rowid;
        }

        public TableKind getTableType() {
            return tableType;
        }

        public boolean isVirtual() {
            return isVirtual;
        }

        public boolean isSystemTable() {
            return getName().startsWith("sqlit");
        }

        public boolean isTemp() {
            return tableType == TableKind.TEMP;
        }

        public boolean isReadOnly() {
            return isReadOnly;
        }
    }

    public static class LimboRowValue
        extends AbstractRowValue<LimboTables, LimboColumn, LimboConstant> {

        LimboRowValue(
            LimboTables tables,
            Map<LimboColumn, LimboConstant> values
        ) {
            super(tables, values);
        }
    }

    public LimboSchema(
        List<LimboTable> databaseTables,
        List<String> indexNames
    ) {
        super(databaseTables);
        this.indexNames = indexNames;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (LimboTable t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static LimboSchema fromConnection(LimboGlobalState globalState)
        throws SQLException {
        List<LimboTable> databaseTables = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();
        SQLConnection con = globalState.getConnection();

        try (Statement s = con.createStatement()) {
            try (
                ResultSet rs = s.executeQuery(
                    "SELECT name, type as category, sql FROM sqlite_schema;"
                )
            ) {
                while (rs.next()) {
                    String tableName = rs.getString("name");
                    String tableType = rs.getString("category");
                    boolean isReadOnly;
                    if (
                        databaseTables
                            .stream()
                            .anyMatch(t -> t.getName().contentEquals(tableName))
                    ) {
                        continue;
                    }
                    String sqlString = rs.getString("sql") == null
                        ? ""
                        : rs.getString("sql").toLowerCase();
                    if (
                        tableName.startsWith("sqlite_") ||
                        tableType.equals("index") ||
                        tableType.equals("trigger") ||
                        tableName.endsWith("_idx") ||
                        tableName.endsWith("_docsize") ||
                        tableName.endsWith("_content") ||
                        tableName.endsWith("_data") ||
                        tableName.endsWith("_config") ||
                        tableName.endsWith("_segdir") ||
                        tableName.endsWith("_stat") ||
                        tableName.endsWith("_segments") ||
                        tableName.contains("_")
                    ) {
                        continue; // TODO
                    } else if (sqlString.contains("using dbstat")) {
                        isReadOnly = true;
                    } else if (sqlString.contains("content=''")) {
                        isReadOnly = true;
                    } else {
                        isReadOnly = false;
                    }
                    boolean withoutRowid = sqlString.contains("without rowid");
                    boolean isView = tableType.contentEquals("view");
                    boolean isVirtual = sqlString.contains("virtual");
                    boolean isDbStatsTable = sqlString.contains("using dbstat");
                    List<LimboColumn> databaseColumns = getTableColumns(
                        con,
                        tableName,
                        sqlString,
                        isView,
                        isDbStatsTable
                    );
                    LimboTable t = new LimboTable(
                        tableName,
                        databaseColumns,
                        tableType.contentEquals("temp_table")
                            ? TableKind.TEMP
                            : TableKind.MAIN,
                        withoutRowid,
                        isView,
                        isVirtual,
                        isReadOnly
                    );
                    if (isRowIdTable(withoutRowid, isView, isVirtual)) {
                        String rowId = Randomly.fromList(ROWID_STRINGS);
                        LimboColumn rowid = new LimboColumn(
                            rowId,
                            LimboDataType.INT,
                            true,
                            null,
                            true
                        );
                        t.addRowid(rowid);
                        rowid.setTable(t);
                    }
                    for (LimboColumn c : databaseColumns) {
                        c.setTable(t);
                    }
                    databaseTables.add(t);
                }
            } catch (SQLException e) {
                // ignore
            }
            try (
                ResultSet rs = s.executeQuery(
                    "SELECT name FROM sqlite_schema WHERE type = 'index';"
                )
            ) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name.contains("_autoindex")) {
                        continue;
                    }
                    indexNames.add(name);
                }
            } catch (SQLException e) {
                if (!e.getMessage().contains("The database file is locked")) {
                    throw new AssertionError(e);
                }
            }
        }

        return new LimboSchema(databaseTables, indexNames);
    }

    // https://www.sqlite.org/rowidtable.html
    private static boolean isRowIdTable(
        boolean withoutRowid,
        boolean isView,
        boolean isVirtual
    ) {
        return !isView && !isVirtual && !withoutRowid;
    }

    private static List<LimboColumn> getTableColumns(
        SQLConnection con,
        String tableName,
        String sql,
        boolean isView,
        boolean isDbStatsTable
    ) throws SQLException {
        List<LimboColumn> databaseColumns = new ArrayList<>();
        try (Statement s2 = con.createStatement()) {
            String tableInfoStr = String.format(
                "PRAGMA table_xinfo(%s)",
                tableName
            );
            try (ResultSet columnRs = s2.executeQuery(tableInfoStr)) {
                String[] columnCreates = sql.split(",");
                int columnCreateIndex = 0;
                while (columnRs.next()) {
                    String columnName = columnRs.getString("name");
                    if (
                        columnName.contentEquals("docid") ||
                        columnName.contentEquals("rank") ||
                        columnName.contentEquals(tableName) ||
                        columnName.contentEquals("__langid")
                    ) {
                        continue; // internal column names of FTS tables
                    }
                    if (
                        isDbStatsTable && columnName.contentEquals("aggregate")
                    ) {
                        // see https://www.sqlite.org/src/tktview?name=a3713a5fca
                        continue;
                    }
                    String columnTypeString = columnRs.getString("type");
                    boolean isPrimaryKey = columnRs.getBoolean("pk");
                    LimboDataType columnType = getColumnType(columnTypeString);
                    LimboCollateSequence collate;
                    if (!isDbStatsTable) {
                        String columnSql = columnCreates[columnCreateIndex++];
                        collate = getCollate(columnSql, isView);
                    } else {
                        collate = LimboCollateSequence.BINARY;
                    }
                    databaseColumns.add(
                        new LimboColumn(
                            columnName,
                            columnType,
                            columnTypeString.contentEquals("INTEGER"),
                            isPrimaryKey,
                            collate
                        )
                    );
                }
            }
        } catch (SQLException e) {}
        if (databaseColumns.isEmpty()) {
            // only generated columns
            throw new IgnoreMeException();
        }
        assert !databaseColumns.isEmpty() : tableName;
        return databaseColumns;
    }

    private static LimboCollateSequence getCollate(String sql, boolean isView) {
        LimboCollateSequence collate;
        if (isView) {
            collate = LimboCollateSequence.BINARY;
        } else {
            if (sql.contains("collate binary")) {
                collate = LimboCollateSequence.BINARY;
            } else if (sql.contains("collate rtrim")) {
                collate = LimboCollateSequence.RTRIM;
            } else if (sql.contains("collate nocase")) {
                collate = LimboCollateSequence.NOCASE;
            } else {
                collate = LimboCollateSequence.BINARY;
            }
        }
        return collate;
    }

    public static LimboDataType getColumnType(String columnTypeString) {
        String trimmedTypeString = columnTypeString
            .toUpperCase()
            .replace(" GENERATED ALWAYS", "");
        LimboDataType columnType;
        switch (trimmedTypeString) {
            case "TEXT":
                columnType = LimboDataType.TEXT;
                break;
            case "INTEGER":
                columnType = LimboDataType.INT;
                break;
            case "INT":
            case "BOOLEAN":
                columnType = LimboDataType.INT;
                break;
            case "":
                columnType = LimboDataType.NONE;
                break;
            case "BLOB":
                columnType = LimboDataType.BINARY;
                break;
            case "REAL":
            case "NUM":
                columnType = LimboDataType.REAL;
                break;
            case "NULL":
                columnType = LimboDataType.NULL;
                break;
            default:
                throw new AssertionError(trimmedTypeString);
        }
        return columnType;
    }

    public LimboTable getRandomVirtualTable() {
        return getRandomTable(p -> p.isVirtual);
    }

    public LimboTables getTables() {
        return new LimboTables(getDatabaseTables());
    }

    public LimboTables getRandomTableNonEmptyTables() {
        if (getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        return new LimboTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public LimboTable getRandomTableNoViewNoVirtualTable() {
        return Randomly.fromList(
            getDatabaseTablesWithoutViewsWithoutVirtualTables()
        );
    }

    public List<
        LimboTable
    > getDatabaseTablesWithoutViewsWithoutVirtualTables() {
        return getDatabaseTables()
            .stream()
            .filter(t -> !t.isView() && !t.isVirtual)
            .collect(Collectors.toList());
    }

    public String getFreeVirtualTableName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("vt%d", i++);
            if (
                getDatabaseTables()
                    .stream()
                    .noneMatch(t -> t.getName().equalsIgnoreCase(tableName))
            ) {
                return tableName;
            }
        } while (true);
    }

    public String getFreeRtreeTableName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String tableName = String.format("rt%d", i++);
            if (
                getDatabaseTables()
                    .stream()
                    .noneMatch(t -> t.getName().equalsIgnoreCase(tableName))
            ) {
                return tableName;
            }
        } while (true);
    }
}
