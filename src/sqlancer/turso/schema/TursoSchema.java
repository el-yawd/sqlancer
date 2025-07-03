package sqlancer.turso.schema;

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
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoTable.TableKind;

public class TursoSchema extends AbstractSchema<TursoGlobalState, TursoTable> {

    /**
     * All possible aliases for the rowid column.
     */
    public static final List<String> ROWID_STRINGS =
        Collections.unmodifiableList(Arrays.asList("rowid", "_rowid_", "oid"));

    public static class TursoColumn
        extends AbstractTableColumn<TursoTable, TursoDataType> {

        private final boolean isInteger; // "INTEGER" type, not "INT"
        private final TursoCollateSequence collate;
        boolean generated;
        private final boolean isPrimaryKey;

        public enum TursoCollateSequence {
            NOCASE,
            RTRIM,
            BINARY;

            public static TursoCollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public TursoColumn(
            String name,
            TursoDataType columnType,
            boolean isInteger,
            boolean isPrimaryKey,
            TursoCollateSequence collate
        ) {
            super(name, null, columnType);
            this.isInteger = isInteger;
            this.isPrimaryKey = isPrimaryKey;
            this.collate = collate;
            this.generated = false;
            assert !isInteger || columnType == TursoDataType.INT;
        }

        public TursoColumn(
            String rowId,
            TursoDataType columnType,
            boolean isInteger,
            TursoCollateSequence collate,
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

        public TursoCollateSequence getCollateSequence() {
            return collate;
        }

        public boolean isGenerated() {
            return generated;
        }

        public static TursoColumn createDummy(String name) {
            return new TursoColumn(name, TursoDataType.INT, false, false, null);
        }
    }

    public static TursoConstant getConstant(
        ResultSet randomRowValues,
        int columnIndex,
        TursoDataType valueType
    ) throws SQLException, AssertionError {
        Object value;
        TursoConstant constant;
        switch (valueType) {
            case INT:
                value = randomRowValues.getLong(columnIndex);
                constant = TursoConstant.createIntConstant((long) value);
                break;
            case REAL:
                value = randomRowValues.getDouble(columnIndex);
                if (!Double.isFinite((double) value)) {
                    // TODO: the JDBC driver seems to sometimes return infinity for NULL values
                    throw new IgnoreMeException();
                }
                constant = TursoConstant.createRealConstant((double) value);
                break;
            case TEXT:
            case NONE:
                value = randomRowValues.getString(columnIndex);
                constant = TursoConstant.createTextConstant((String) value);
                break;
            case BINARY:
                value = randomRowValues.getBytes(columnIndex);
                constant = TursoConstant.createBinaryConstant((byte[]) value);
                if (((byte[]) value).length == 0) {
                    // TODO: the JDBC driver seems to sometimes return a zero-length array for NULL values
                    throw new IgnoreMeException();
                }
                break;
            case NULL:
                return TursoConstant.createNullConstant();
            default:
                throw new AssertionError(valueType);
        }
        return constant;
    }

    public static class TursoTables
        extends AbstractTables<TursoTable, TursoColumn> {

        public TursoTables(List<TursoTable> tables) {
            super(tables);
        }

        public TursoRowValue getRandomRowValue(SQLConnection con)
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
            Map<TursoColumn, TursoConstant> values = new HashMap<>();
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
                    TursoColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(
                        column.getTable().getName() + column.getName()
                    );
                    assert columnIndex == i + 1;
                    String typeString = randomRowValues.getString(
                        columnIndex + getColumns().size()
                    );
                    TursoDataType valueType = getColumnType(typeString);
                    TursoConstant constant = getConstant(
                        randomRowValues,
                        columnIndex,
                        valueType
                    );
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new TursoRowValue(this, values);
            }
        }
    }

    public static class TursoTable
        extends AbstractRelationalTable<
            TursoColumn,
            TableIndex,
            TursoGlobalState
        > {

        // TODO: why does the SQLite implementation have no table indexes?

        public enum TableKind {
            MAIN,
            TEMP,
        }

        private final TableKind tableType;
        private TursoColumn rowid;
        private final boolean withoutRowid;
        private final boolean isVirtual;
        private final boolean isReadOnly;

        public TursoTable(
            String tableName,
            List<TursoColumn> columns,
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

        public void addRowid(TursoColumn rowid) {
            this.rowid = rowid;
        }

        public TursoColumn getRowid() {
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

    public static class TursoRowValue
        extends AbstractRowValue<TursoTables, TursoColumn, TursoConstant> {

        TursoRowValue(
            TursoTables tables,
            Map<TursoColumn, TursoConstant> values
        ) {
            super(tables, values);
        }
    }

    public TursoSchema(
        List<TursoTable> databaseTables,
        List<String> indexNames
    ) {
        super(databaseTables);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (TursoTable t : getDatabaseTables()) {
            sb.append(t);
            sb.append("\n");
        }
        return sb.toString();
    }

    public static TursoSchema fromConnection(TursoGlobalState globalState)
        throws SQLException {
        List<TursoTable> databaseTables = new ArrayList<>();
        List<String> indexNames = new ArrayList<>();
        SQLConnection con = globalState.getConnection();

        try (Statement s = con.createStatement()) {
            try (
                ResultSet rs = s.executeQuery(
                    "SELECT name, type as category, sql FROM sqlite_schema;"
                );
            ) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.println(
                        "Column " + i + ": " + rs.getMetaData().getColumnName(i)
                    );
                }
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
                    List<TursoColumn> databaseColumns = getTableColumns(
                        con,
                        tableName,
                        sqlString,
                        isView,
                        isDbStatsTable
                    );
                    TursoTable t = new TursoTable(
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
                        TursoColumn rowid = new TursoColumn(
                            rowId,
                            TursoDataType.INT,
                            true,
                            null,
                            true
                        );
                        t.addRowid(rowid);
                        rowid.setTable(t);
                    }
                    for (TursoColumn c : databaseColumns) {
                        c.setTable(t);
                    }
                    System.out.println("Table created: " + t.getName());
                    databaseTables.add(t);
                }
            } catch (SQLException e) {
                System.out.println(e);
                // ignore
            }
            try (
                ResultSet rs = s.executeQuery(
                    "SELECT name FROM sqlite_schema WHERE type = 'index';"
                );
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

        return new TursoSchema(databaseTables, indexNames);
    }

    // https://www.sqlite.org/rowidtable.html
    private static boolean isRowIdTable(
        boolean withoutRowid,
        boolean isView,
        boolean isVirtual
    ) {
        return !isView && !isVirtual && !withoutRowid;
    }

    private static List<TursoColumn> getTableColumns(
        SQLConnection con,
        String tableName,
        String sql,
        boolean isView,
        boolean isDbStatsTable
    ) throws SQLException {
        List<TursoColumn> databaseColumns = new ArrayList<>();
        try (Statement s2 = con.createStatement()) {
            String tableInfoStr = String.format(
                "PRAGMA table_info(%s)",
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
                    TursoDataType columnType = getColumnType(columnTypeString);
                    TursoCollateSequence collate;
                    if (!isDbStatsTable) {
                        String columnSql = columnCreates[columnCreateIndex++];
                        collate = getCollate(columnSql, isView);
                    } else {
                        collate = TursoCollateSequence.BINARY;
                    }
                    databaseColumns.add(
                        new TursoColumn(
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

    private static TursoCollateSequence getCollate(String sql, boolean isView) {
        TursoCollateSequence collate;
        if (isView) {
            collate = TursoCollateSequence.BINARY;
        } else {
            if (sql.contains("collate binary")) {
                collate = TursoCollateSequence.BINARY;
            } else if (sql.contains("collate rtrim")) {
                collate = TursoCollateSequence.RTRIM;
            } else if (sql.contains("collate nocase")) {
                collate = TursoCollateSequence.NOCASE;
            } else {
                collate = TursoCollateSequence.BINARY;
            }
        }
        return collate;
    }

    public static TursoDataType getColumnType(String columnTypeString) {
        String trimmedTypeString = columnTypeString
            .toUpperCase()
            .replace(" GENERATED ALWAYS", "");
        TursoDataType columnType;
        switch (trimmedTypeString) {
            case "TEXT":
                columnType = TursoDataType.TEXT;
                break;
            case "INTEGER":
                columnType = TursoDataType.INT;
                break;
            case "INT":
            case "BOOLEAN":
                columnType = TursoDataType.INT;
                break;
            case "":
                columnType = TursoDataType.NONE;
                break;
            case "BLOB":
                columnType = TursoDataType.BINARY;
                break;
            case "REAL":
            case "NUM":
                columnType = TursoDataType.REAL;
                break;
            case "NULL":
                columnType = TursoDataType.NULL;
                break;
            default:
                throw new AssertionError(trimmedTypeString);
        }
        return columnType;
    }

    public TursoTable getRandomVirtualTable() {
        return getRandomTable(p -> p.isVirtual);
    }

    public TursoTables getTables() {
        return new TursoTables(getDatabaseTables());
    }

    public TursoTables getRandomTableNonEmptyTables() {
        if (getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException();
        }
        return new TursoTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public TursoTable getRandomTableNoViewNoVirtualTable() {
        return Randomly.fromList(
            getDatabaseTablesWithoutViewsWithoutVirtualTables()
        );
    }

    public List<
        TursoTable
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
