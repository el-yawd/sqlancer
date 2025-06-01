package sqlancer.limbo.oracle;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboProvider;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboAggregate;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboConstant.LimboTextConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.InOperation;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.ast.LimboExpression.Join.JoinType;
import sqlancer.limbo.ast.LimboExpression.LimboAlias;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboExpression.LimboExist;
import sqlancer.limbo.ast.LimboExpression.LimboExpressionBag;
import sqlancer.limbo.ast.LimboExpression.LimboOrderingTerm;
import sqlancer.limbo.ast.LimboExpression.LimboOrderingTerm.Ordering;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixText;
import sqlancer.limbo.ast.LimboExpression.LimboResultMap;
import sqlancer.limbo.ast.LimboExpression.LimboTableAndColumnRef;
import sqlancer.limbo.ast.LimboExpression.LimboTableReference;
import sqlancer.limbo.ast.LimboExpression.LimboTypeof;
import sqlancer.limbo.ast.LimboExpression.LimboValues;
import sqlancer.limbo.ast.LimboExpression.LimboWithClause;
import sqlancer.limbo.ast.LimboExpression.LimboBinaryOperation.BinaryOperator;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;
import sqlancer.limbo.schema.LimboSchema.LimboTables;

public class LimboCODDTestOracle extends CODDTestBase<LimboGlobalState> implements TestOracle<LimboGlobalState> {

    private final LimboSchema s;
    private LimboExpressionGenerator gen;
    private Reproducer<LimboGlobalState> reproducer;

    private static final String TEMP_TABLE_NAME = "temp_table";

    private LimboExpression foldedExpr;
    private LimboExpression constantResOfFoldedExpr;

    private List<LimboTable> tablesFromOuterContext = new ArrayList<>();
    private List<Join> joinsInExpr;

    Map<String, List<LimboConstant>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<LimboConstant>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    public LimboCODDTestOracle(LimboGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        LimboErrors.addExpectedExpressionErrors(errors);
        LimboErrors.addMatchQueryErrors(errors);
        LimboErrors.addQueryErrors(errors);
        // errors.add("misuse of aggregate");
        // errors.add("misuse of window function");
        // errors.add("second argument to nth_value must be a positive integer");
        // errors.add("no such table");
        // errors.add("no query solution");
        // errors.add("unable to use function MATCH in the requested context");
        // errors.add("[SQLITE_ERROR] SQL error or missing database (unrecognized token:");
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;

        joinsInExpr = null;
        tablesFromOuterContext.clear();

        useSubqueryAsFoldedExpr = useSubquery();
        useCorrelatedSubqueryAsFoldedExpr = useCorrelatedSubquery();

        LimboSelect auxiliaryQuery = null;
        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) {
                auxiliaryQuery = genSelectWithCorrelatedSubquery();
                auxiliaryQueryString = LimboVisitor.asString(auxiliaryQuery);

                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null);
                auxiliaryQueryString = LimboVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect();
            auxiliaryQueryString = LimboVisitor.asString(auxiliaryQuery);

            auxiliaryQueryResult.putAll(selectResult);
        }

        LimboSelect originalQuery = null;

        Map<String, List<LimboConstant>> foldedResult = new HashMap<>();
        Map<String, List<LimboConstant>> originalResult = new HashMap<>();

        // dependent expression
        if (!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr) {
            // original query
            LimboExpressionBag specificCondition = new LimboExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = LimboVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = LimboVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } else if (auxiliaryQueryResult.isEmpty()
                || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).isEmpty()) {
            // independent expression
            // empty result, put the inner query in (NOT) EXIST
            boolean isNegated = !Randomly.getBoolean();
            // original query
            LimboExist existExpr = new LimboExist(new LimboSelect(auxiliaryQuery), isNegated);
            LimboExpressionBag specificCondition = new LimboExpressionBag(existExpr);

            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = LimboVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            LimboExpression equivalentExpr = isNegated ? LimboConstant.createTrue() : LimboConstant.createFalse();
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = LimboVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } else if (auxiliaryQueryResult.size() == 1
                && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1
                && Randomly.getBoolean()) {
            // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
            // original query
            LimboExpressionBag specificCondition = new LimboExpressionBag(auxiliaryQuery);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = LimboVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            LimboExpression equivalentExpr = auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0])
                    .get(0);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = LimboVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } else if (auxiliaryQueryResult.size() == 1 && Randomly.getBooleanWithRatherLowProbability()
                && enableInOperator()) {
            // one column
            // original query
            List<LimboColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            LimboColumnName selectedColumn = new LimboColumnName(Randomly.fromList(columns), null);
            LimboTable selectedTable = selectedColumn.getColumn().getTable();
            InOperation inOperation = new InOperation(selectedColumn, new LimboSelect(auxiliaryQuery));
            LimboExpressionBag specificCondition = new LimboExpressionBag(inOperation);

            originalQuery = this.genSelectExpression(selectedTable, specificCondition);
            originalQueryString = LimboVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            // folded query
            // can not use IN VALUES here, because there is no affinity for the right operand of IN when right operand
            // is a list
            try {
                LimboTable t = this.createTemporaryTable(auxiliaryQuery, "intable");
                LimboTableReference equivalentTable = new LimboTableReference(t);
                inOperation = new InOperation(selectedColumn, equivalentTable);
                specificCondition.updateInnerExpr(inOperation);
                foldedQueryString = LimboVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } finally {
                dropTemporaryTable("intable");
            }
        } else {
            // There is not `ANY` and `ALL` operator in Limbo
            // Row Subquery
            // original query
            LimboTable temporaryTable = this.genTemporaryTable(auxiliaryQuery, LimboCODDTestOracle.TEMP_TABLE_NAME);
            originalQuery = this.genSelectExpression(temporaryTable, null);
            LimboTableAndColumnRef tableAndColumnRef = new LimboTableAndColumnRef(temporaryTable);
            LimboWithClause withClause = new LimboWithClause(tableAndColumnRef, new LimboSelect(auxiliaryQuery));
            originalQuery.setWithClause(withClause);
            originalQueryString = LimboVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            // folded query
            if (Randomly.getBoolean() && this.enableCommonTableExpression()) {
                // there are too many false positives
                // common table expression
                // folded query: WITH table AS VALUES ()
                LimboValues values = new LimboValues(auxiliaryQueryResult, temporaryTable.getColumns());
                originalQuery.updateWithClauseRight(values);
                foldedQueryString = LimboVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean() && this.enableDerivedTable()) {
                // derived table
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClause(null);
                LimboTableReference tempTableRef = new LimboTableReference(temporaryTable);
                LimboAlias alias = new LimboAlias(new LimboSelect(auxiliaryQuery), tempTableRef);
                originalQuery.replaceFromTable(LimboCODDTestOracle.TEMP_TABLE_NAME, alias);
                foldedQueryString = LimboVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (this.enableInsert()) {
                // there are too many false positives
                // folded query: CREATE the table and INSERT INTO table subquery
                try {
                    this.createTemporaryTable(auxiliaryQuery, LimboCODDTestOracle.TEMP_TABLE_NAME);
                    originalQuery.setWithClause(null);
                    foldedQueryString = LimboVisitor.asString(originalQuery);
                    foldedResult = getQueryResult(foldedQueryString, state);
                } finally {
                    dropTemporaryTable(LimboCODDTestOracle.TEMP_TABLE_NAME);
                }
            } else {
                throw new IgnoreMeException();
            }
        }
        if (foldedResult == null || originalResult == null) {
            throw new IgnoreMeException();
        }
        if (foldedQueryString.equals(originalQueryString)) {
            throw new IgnoreMeException();
        }
        if (!compareResult(foldedResult, originalResult)) {
            reproducer = null; // TODO
            state.getState().getLocalState()
                    .log(auxiliaryQueryString + ";\n" + foldedQueryString + ";\n" + originalQueryString + ";");
            throw new AssertionError(
                    auxiliaryQueryResult.toString() + " " + foldedResult.toString() + " " + originalResult.toString());
        }
    }

    private LimboSelect genSelectExpression(LimboTable tempTable, LimboExpression specificCondition) {
        LimboTables randomTables = s.getRandomTableNonEmptyTables();
        if (tempTable != null) {
            randomTables.addTable(tempTable);
        }
        if (!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr) {
            for (LimboTable t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (Join j : this.joinsInExpr) {
                    LimboTable t = j.getTable();
                    randomTables.removeTable(t);
                }
            }
        }

        List<LimboColumn> columns = randomTables.getColumns();
        if ((!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)
                && this.joinsInExpr != null) {
            for (Join j : this.joinsInExpr) {
                LimboTable t = j.getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new LimboExpressionGenerator(state).setColumns(columns);
        List<LimboTable> tables = randomTables.getTables();
        List<Join> joinStatements = new ArrayList<>();
        if (!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr) {
            if (this.joinsInExpr != null) {
                joinStatements.addAll(this.joinsInExpr);
                this.joinsInExpr = null;
            }
        } else if (Randomly.getBoolean()) {
            joinStatements = genJoinExpression(gen, tables,
                    Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null, false);
        }
        List<LimboExpression> tableRefs = LimboCommon.getTableRefs(tables, s);
        LimboSelect select = new LimboSelect();
        select.setFromList(tableRefs);
        if (!joinStatements.isEmpty()) {
            select.setJoinClauses(joinStatements);
        }

        LimboExpression randomWhereCondition = gen.generateExpression();
        LimboExpression whereCondition = null;
        if (specificCondition != null) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            whereCondition = new LimboExpression.LimboBinaryOperation(randomWhereCondition, specificCondition,
                    operator);
        } else {
            whereCondition = randomWhereCondition;
        }
        select.setWhereClause(whereCondition);

        if (Randomly.getBoolean()) {
            select.setOrderByClauses(genOrderBysExpression(gen,
                    Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null));
        }

        if (Randomly.getBoolean()) {
            List<LimboColumn> selectedColumns = Randomly.nonEmptySubset(columns);
            List<LimboExpression> selectedAlias = new LinkedList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                LimboColumnName originalName = new LimboColumnName(selectedColumns.get(i), null);
                LimboColumnName aliasName = new LimboColumnName(LimboColumn.createDummy("c" + i), null);
                LimboAlias columnAlias = new LimboAlias(originalName, aliasName);
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            LimboColumnName aggr = new LimboColumnName(Randomly.fromList(columns), null);
            LimboProvider.mustKnowResult = true;
            LimboExpression originalName = new LimboAggregate(Arrays.asList(aggr),
                    LimboAggregate.LimboAggregateFunction.getRandom());
            LimboColumnName aliasName = new LimboColumnName(LimboColumn.createDummy("c0"), null);
            LimboAlias columnAlias = new LimboAlias(originalName, aliasName);
            select.setFetchColumns(Arrays.asList(columnAlias));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                List<LimboExpression> groupByClause = genGroupByClause(columns, specificCondition);
                select.setGroupByClause(groupByClause);
                if (!groupByClause.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
                    select.setHavingClause(genHavingClause(columns, specificCondition));
                }
            }
        }
        return select;
    }

    // For expression test
    private LimboSelect genSimpleSelect() {
        LimboTables randomTables = s.getRandomTableNonEmptyTables();
        List<LimboColumn> columns = randomTables.getColumns();

        gen = new LimboExpressionGenerator(state).setColumns(columns);
        List<LimboTable> tables = randomTables.getTables();
        tablesFromOuterContext = randomTables.getTables();

        if (Randomly.getBooleanWithRatherLowProbability()) {
            joinsInExpr = genJoinExpression(gen, tables, null, true);
        } else {
            joinsInExpr = new ArrayList<>();
        }

        List<LimboExpression> tableRefs = LimboCommon.getTableRefs(tables, s);
        LimboSelect select = new LimboSelect();
        select.setFromList(tableRefs);
        if (joinsInExpr != null && !joinsInExpr.isEmpty()) {
            select.setJoinClauses(joinsInExpr);
        }

        LimboExpression whereCondition = gen.generateExpression();
        this.foldedExpr = whereCondition;

        List<LimboExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (LimboColumn c : randomTables.getColumns()) {
            LimboColumnName cRef = new LimboColumnName(c, null);
            LimboColumnName aliasName = new LimboColumnName(LimboColumn.createDummy("c" + columnIdx), null);
            LimboAlias columnAlias = new LimboAlias(cRef, aliasName);
            fetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        LimboColumnName aliasName = new LimboColumnName(LimboColumn.createDummy("c" + columnIdx), null);
        LimboAlias columnAlias = new LimboAlias(whereCondition, aliasName);
        fetchColumns.add(columnAlias);

        select.setFetchColumns(fetchColumns);

        Map<String, List<LimboConstant>> queryRes = null;
        try {
            queryRes = getQueryResult(LimboVisitor.asString(select), state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        }
        if (queryRes.get("c0").size() == 0) {
            throw new IgnoreMeException();
        }

        // save the result first
        selectResult.clear();
        selectResult.putAll(queryRes);

        // get the summary from results
        List<LimboConstant> summary = queryRes.remove("c" + columnIdx);

        List<LimboColumn> tempColumnList = new ArrayList<>();

        for (int i = 0; i < fetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            LimboAlias cAlias = (LimboAlias) fetchColumns.get(i);
            LimboColumnName cRef = (LimboColumnName) cAlias.getOriginalExpression();
            LimboColumn column = cRef.getColumn();
            String columnName = LimboVisitor.asString(cAlias.getAliasExpression());
            LimboColumn newColumn = new LimboColumn(columnName, column.getType(), false, false, null);
            tempColumnList.add(newColumn);
        }
        List<LimboColumnName> columnRef = new ArrayList<>();
        for (LimboColumn c : randomTables.getColumns()) {
            columnRef.add(new LimboColumnName(c, null));
        }
        if (tempColumnList.size() != queryRes.size()) {
            throw new AssertionError();
        }
        LimboValues values = new LimboValues(queryRes, tempColumnList);
        this.constantResOfFoldedExpr = new LimboResultMap(values, columnRef, summary, null);

        return select;
    }

    private LimboSelect genSelectWithCorrelatedSubquery() {
        LimboTables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        LimboTables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        List<LimboExpression> innerQueryFromTables = new ArrayList<>();
        for (LimboTable t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t)) {
                innerQueryFromTables.add(new LimboTableReference(t));
            }
        }
        for (LimboTable t : outerQueryRandomTables.getTables()) {
            if (innerQueryRandomTables.isContained(t)) {
                innerQueryRandomTables.removeTable(t);

                List<LimboColumn> newColumns = new ArrayList<>();
                for (LimboColumn c : t.getColumns()) {
                    LimboColumn newColumn = new LimboColumn(c.getName(), c.getType(), false, null, false);
                    newColumns.add(newColumn);
                }
                LimboTable newTable = new LimboTable(t.getName() + "a", newColumns, null, true, false, false,
                        false);
                for (LimboColumn c : newColumns) {
                    c.setTable(newTable);
                }
                innerQueryRandomTables.addTable(newTable);

                LimboAlias alias = new LimboAlias(new LimboTableReference(t),
                        new LimboTableReference(newTable));
                innerQueryFromTables.add(alias);
            }
        }

        List<LimboColumn> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new LimboExpressionGenerator(state).setColumns(innerQueryColumns);

        LimboSelect innerQuery = new LimboSelect();
        innerQuery.setFromList(innerQueryFromTables);

        LimboExpression innerQueryWhereCondition = gen.generateExpression();
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        LimboColumnName innerQueryAggr = new LimboColumnName(Randomly.fromList(innerQueryRandomTables.getColumns()),
                null);
        LimboProvider.mustKnowResult = true;
        LimboExpression innerQueryAggrName = new LimboAggregate(Arrays.asList(innerQueryAggr),
                LimboAggregate.LimboAggregateFunction.getRandom());
        innerQuery.setFetchColumns(Arrays.asList(innerQueryAggrName));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<LimboExpression> groupByClause = genGroupByClause(innerQueryColumns, null);
            innerQuery.setGroupByClause(groupByClause);
            if (!groupByClause.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
                innerQuery.setHavingClause(genHavingClause(innerQueryColumns, null));
            }
        }

        this.foldedExpr = innerQuery;

        // outer query
        LimboSelect outerQuery = new LimboSelect();
        outerQuery.setFromList(LimboCommon.getTableRefs(outerQueryRandomTables.getTables(), s));
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<LimboExpression> outerQueryFetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (LimboColumn c : outerQueryRandomTables.getColumns()) {
            LimboColumnName cRef = new LimboColumnName(c, null);
            LimboColumnName aliasName = new LimboColumnName(LimboColumn.createDummy("c" + columnIdx), null);
            LimboAlias columnAlias = new LimboAlias(cRef, aliasName);
            outerQueryFetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        LimboColumnName aliasName = new LimboColumnName(LimboColumn.createDummy("c" + columnIdx), null);
        LimboAlias columnAlias = new LimboAlias(innerQuery, aliasName);
        outerQueryFetchColumns.add(columnAlias);

        outerQuery.setFetchColumns(outerQueryFetchColumns);

        originalQueryString = LimboVisitor.asString(outerQuery);

        Map<String, List<LimboConstant>> queryRes = null;
        try {
            queryRes = getQueryResult(originalQueryString, state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        }
        if (queryRes.get("c0").size() == 0) {
            throw new IgnoreMeException();
        }

        // save the result first
        selectResult.clear();
        selectResult.putAll(queryRes);

        // get the summary from results
        List<LimboConstant> summary = queryRes.remove("c" + columnIdx);

        List<LimboColumn> tempColumnList = new ArrayList<>();

        for (int i = 0; i < outerQueryFetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            LimboAlias cAlias = (LimboAlias) outerQueryFetchColumns.get(i);
            LimboColumnName cRef = (LimboColumnName) cAlias.getOriginalExpression();
            LimboColumn column = cRef.getColumn();
            String columnName = LimboVisitor.asString(cAlias.getAliasExpression());
            LimboColumn newColumn = new LimboColumn(columnName, column.getType(), false, false, null);
            tempColumnList.add(newColumn);
        }
        List<LimboColumnName> columnRef = new ArrayList<>();
        for (LimboColumn c : outerQueryRandomTables.getColumns()) {
            columnRef.add(new LimboColumnName(c, null));
        }
        if (tempColumnList.size() != queryRes.size()) {
            throw new AssertionError();
        }
        LimboValues values = new LimboValues(queryRes, tempColumnList);
        this.constantResOfFoldedExpr = new LimboResultMap(values, columnRef, summary, null);

        return outerQuery;
    }

    private List<Join> genJoinExpression(LimboExpressionGenerator gen, List<LimboTable> tables,
            LimboExpression specificCondition, boolean joinForExperssion) {
        List<Join> joinStatements = new ArrayList<>();
        if (!state.getDbmsSpecificOptions().testJoins) {
            return joinStatements;
        }
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values()));
        if (Randomly.getBoolean() && tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1 || joinForExperssion) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) {
                LimboExpression randomOnCondition = gen.generateExpression();
                LimboExpression onCondition = null;
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    BinaryOperator operator = BinaryOperator.getRandomOperator();
                    onCondition = new LimboExpression.LimboBinaryOperation(randomOnCondition, specificCondition,
                            operator);
                } else {
                    onCondition = randomOnCondition;
                }

                LimboTable table = Randomly.fromList(tables);
                tables.remove(table);
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    onCondition = null;
                }
                Join j = new LimboExpression.Join(table, onCondition, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }

    private List<LimboExpression> genOrderBysExpression(LimboExpressionGenerator gen,
            LimboExpression specificCondition) {
        List<LimboExpression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(
                    genOrderingTerm(gen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null));
        }
        return expressions;
    }

    private LimboExpression genOrderingTerm(LimboExpressionGenerator gen, LimboExpression specificCondition) {
        LimboExpression expr = gen.generateExpression();
        if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            expr = new LimboExpression.LimboBinaryOperation(expr, specificCondition, operator);
        }
        // COLLATE is potentially already generated
        if (Randomly.getBoolean()) {
            expr = new LimboOrderingTerm(expr, Ordering.getRandomValue());
        }
        if (state.getDbmsSpecificOptions().testNullsFirstLast && Randomly.getBoolean()) {
            expr = new LimboPostfixText(expr, Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
                    null /* expr.getExpectedValue() */) {
                @Override
                public boolean omitBracketsWhenPrinting() {
                    return true;
                }
            };
        }
        return expr;
    }

    private List<LimboExpression> genGroupByClause(List<LimboColumn> columns, LimboExpression specificCondition) {
        errors.add("GROUP BY term out of range");
        if (Randomly.getBoolean()) {
            List<LimboExpression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                LimboExpression expr = new LimboExpressionGenerator(state).setColumns(columns).generateExpression();
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    BinaryOperator operator = BinaryOperator.getRandomOperator();
                    expr = new LimboExpression.LimboBinaryOperation(expr, specificCondition, operator);
                }
                collect.add(expr);
            }
            return collect;
        }
        return Collections.emptyList();
    }

    private LimboExpression genHavingClause(List<LimboColumn> columns, LimboExpression specificCondition) {
        LimboExpression expr = new LimboExpressionGenerator(state).setColumns(columns).generateExpression();
        if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            expr = new LimboExpression.LimboBinaryOperation(expr, specificCondition, operator);
        }
        return expr;
    }

    private Map<String, List<LimboConstant>> getQueryResult(String queryString, LimboGlobalState state)
            throws SQLException {
        Map<String, List<LimboConstant>> result = new LinkedHashMap<>();
        if (options.logEachSelect()) {
            logger.writeCurrentNoLineBreak(queryString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            stmt.setQueryTimeout(600);
            ResultSet rs = null;
            try {
                rs = stmt.executeQuery(queryString);
                ResultSetMetaData metaData = rs.getMetaData();
                Integer columnCount = metaData.getColumnCount();
                Map<Integer, String> idxNameMap = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    result.put("c" + (i - 1), new ArrayList<>());
                    idxNameMap.put(i, "c" + (i - 1));
                }

                int resultRows = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        try {
                            Object value = rs.getObject(i);
                            LimboConstant constant;
                            if (rs.wasNull()) {
                                constant = LimboConstant.createNullConstant();
                            } else if (value instanceof Integer) {
                                constant = LimboConstant.createIntConstant(Long.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = LimboConstant.createIntConstant(Long.valueOf((Short) value));
                            } else if (value instanceof Long) {
                                constant = LimboConstant.createIntConstant((Long) value);
                            } else if (value instanceof Double) {
                                constant = LimboConstant.createRealConstant((double) value);
                            } else if (value instanceof Float) {
                                constant = LimboConstant.createRealConstant(((Float) value).doubleValue());
                            } else if (value instanceof BigDecimal) {
                                constant = LimboConstant.createRealConstant(((BigDecimal) value).doubleValue());
                            } else if (value instanceof byte[]) {
                                constant = LimboConstant.createBinaryConstant((byte[]) value);
                            } else if (value instanceof Boolean) {
                                constant = LimboConstant.createBoolean((boolean) value);
                            } else if (value instanceof String) {
                                constant = LimboConstant.createTextConstant((String) value);
                            } else if (value == null) {
                                constant = LimboConstant.createNullConstant();
                            } else {
                                throw new IgnoreMeException();
                            }
                            List<LimboConstant> v = result.get(idxNameMap.get(i));
                            v.add(constant);
                        } catch (SQLException e) {
                            System.out.println(e.getMessage());
                            throw new IgnoreMeException();
                        }
                    }
                    ++resultRows;
                    if (resultRows > 100) {
                        throw new IgnoreMeException();
                    }
                }
                Main.nrSuccessfulActions.addAndGet(1);
                rs.close();
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                if (errors.errorIsExpected(e.getMessage())) {
                    throw new IgnoreMeException();
                } else {
                    state.getState().getLocalState().log(queryString);
                    throw new AssertionError(e.getMessage());
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return result;
    }

    private LimboTable genTemporaryTable(LimboSelect select, String tableName) {
        List<LimboExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, LimboDataType> idxTypeMap = getColumnTypeFromSelect(select);

        List<LimboColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + i;
            LimboColumn column = new LimboColumn(columnName, idxTypeMap.get(i), false, false, null);
            databaseColumns.add(column);
        }
        LimboTable table = new LimboTable(tableName, databaseColumns, null, false, false, false, false);
        for (LimboColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private LimboTable createTemporaryTable(LimboSelect select, String tableName) throws SQLException {
        String selectString = LimboVisitor.asString(select);
        Map<Integer, LimboDataType> idxTypeMap = getColumnTypeFromSelect(select);

        Integer columnNumber = idxTypeMap.size();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE " + tableName + " (");
        for (int i = 0; i < columnNumber; ++i) {
            String columnTypeName = "";
            if (idxTypeMap.get(i) != null) {
                switch (idxTypeMap.get(i)) {
                case INT:
                case TEXT:
                case REAL:
                    columnTypeName = idxTypeMap.get(i).name();
                    break;
                case BINARY:
                    columnTypeName = "";
                    break;
                default:
                    columnTypeName = "";
                }
            }
            sb.append("c" + i + " " + columnTypeName);
            if (i < columnNumber - 1) {
                sb.append(", ");
            }
        }
        sb.append(");");
        String crateTableString = sb.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(crateTableString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                stmt.execute(crateTableString);
                Main.nrSuccessfulActions.addAndGet(1);
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        StringBuilder sb2 = new StringBuilder();
        sb2.append("INSERT INTO " + tableName + " " + selectString);
        String insertValueString = sb2.toString();
        if (options.logEachSelect()) {
            logger.writeCurrent(insertValueString);
        }
        stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                Main.nrSuccessfulActions.addAndGet(1);
                stmt.execute(insertValueString);
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        List<LimboColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + i;
            LimboColumn column = new LimboColumn(columnName, idxTypeMap.get(i), false, false, null);
            databaseColumns.add(column);
        }
        LimboTable table = new LimboTable(tableName, databaseColumns, null, false, false, false, false);
        for (LimboColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private void dropTemporaryTable(String tableName) throws SQLException {
        String dropString = "DROP TABLE " + tableName + ";";
        if (options.logEachSelect()) {
            logger.writeCurrent(dropString);
        }
        Statement stmt = null;
        try {
            stmt = this.con.createStatement();
            try {
                stmt.execute(dropString);
                Main.nrSuccessfulActions.addAndGet(1);
            } catch (SQLException e) {
                Main.nrUnsuccessfulActions.addAndGet(1);
                throw new IgnoreMeException();
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    private boolean compareResult(Map<String, List<LimboConstant>> r1, Map<String, List<LimboConstant>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry<String, List<LimboConstant>> entry : r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            }
            List<LimboConstant> v1 = entry.getValue();
            List<LimboConstant> v2 = r2.get(currentKey);
            if (v1.size() != v2.size()) {
                return false;
            }
            List<String> v1Value = new ArrayList<>(v1.stream().map(c -> c.toString()).collect(Collectors.toList()));
            List<String> v2Value = new ArrayList<>(v2.stream().map(c -> c.toString()).collect(Collectors.toList()));
            Collections.sort(v1Value);
            Collections.sort(v2Value);
            if (!v1Value.equals(v2Value)) {
                return false;
            }
        }
        return true;
    }

    private Map<Integer, LimboDataType> getColumnTypeFromSelect(LimboSelect select) {
        List<LimboExpression> fetchColumns = select.getFetchColumns();
        List<LimboExpression> newFetchColumns = new ArrayList<>();
        for (LimboExpression column : fetchColumns) {
            newFetchColumns.add(column);
            LimboAlias columnAlias = (LimboAlias) column;
            LimboExpression typeofColumn = new LimboTypeof(columnAlias.getOriginalExpression());
            newFetchColumns.add(typeofColumn);
        }
        LimboSelect newSelect = new LimboSelect(select);
        newSelect.setFetchColumns(newFetchColumns);
        Map<String, List<LimboConstant>> typeResult = null;
        try {
            typeResult = getQueryResult(LimboVisitor.asString(newSelect), state);
        } catch (SQLException e) {
            if (errors.errorIsExpected(e.getMessage())) {
                throw new IgnoreMeException();
            } else {
                throw new AssertionError(e.getMessage());
            }
        }

        if (typeResult == null) {
            throw new IgnoreMeException();
        }
        Map<Integer, LimboDataType> idxTypeMap = new HashMap<>();
        for (int i = 0; i * 2 < typeResult.size(); ++i) {
            String columnName = "c" + (i * 2 + 1);
            LimboExpression t = typeResult.get(columnName).get(0);
            LimboTextConstant tString = (LimboTextConstant) t;
            String typeName = tString.asString();
            LimboDataType cType = LimboDataType.getTypeFromName(typeName);
            idxTypeMap.put(i, cType);
        }

        return idxTypeMap;
    }

    public boolean useSubquery() {
        if (this.state.getDbmsSpecificOptions().coddTestModel.isRandom()) {
            return Randomly.getBoolean();
        } else if (this.state.getDbmsSpecificOptions().coddTestModel.isExpression()) {
            return false;
        } else if (this.state.getDbmsSpecificOptions().coddTestModel.isSubquery()) {
            return true;
        } else {
            System.out.printf("Wrong option of --coddtest-model, should be one of: RANDOM, EXPRESSION, SUBQUERY");
            System.exit(1);
            return false;
        }
    }

    public boolean useCorrelatedSubquery() {
        return Randomly.getBoolean();
    }

    public boolean enableCommonTableExpression() {
        return false;
    }

    public boolean enableDerivedTable() {
        return true;
    }

    public boolean enableInsert() {
        return false;
    }

    public boolean enableInOperator() {
        return false;
    }

    @Override
    public String getLastQueryString() {
        return originalQueryString;
    }

    @Override
    public Reproducer<LimboGlobalState> getLastReproducer() {
        return reproducer;
    }
}
