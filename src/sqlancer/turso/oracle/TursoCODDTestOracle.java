package sqlancer.turso.oracle;

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
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoProvider;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoAggregate;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoConstant.TursoTextConstant;
import sqlancer.turso.ast.TursoExpression.InOperation;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.TursoAlias;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoExpression.TursoExist;
import sqlancer.turso.ast.TursoExpression.TursoExpressionBag;
import sqlancer.turso.ast.TursoExpression.TursoOrderingTerm;
import sqlancer.turso.ast.TursoExpression.TursoPostfixText;
import sqlancer.turso.ast.TursoExpression.TursoResultMap;
import sqlancer.turso.ast.TursoExpression.TursoTableAndColumnRef;
import sqlancer.turso.ast.TursoExpression.TursoTableReference;
import sqlancer.turso.ast.TursoExpression.TursoTypeof;
import sqlancer.turso.ast.TursoExpression.TursoValues;
import sqlancer.turso.ast.TursoExpression.TursoWithClause;
import sqlancer.turso.ast.TursoExpression.Join.JoinType;
import sqlancer.turso.ast.TursoExpression.TursoBinaryOperation.BinaryOperator;
import sqlancer.turso.ast.TursoExpression.TursoOrderingTerm.Ordering;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoTables;

public class TursoCODDTestOracle extends CODDTestBase<TursoGlobalState> implements TestOracle<TursoGlobalState> {

    private final TursoSchema s;
    private TursoExpressionGenerator gen;
    private Reproducer<TursoGlobalState> reproducer;

    private static final String TEMP_TABLE_NAME = "temp_table";

    private TursoExpression foldedExpr;
    private TursoExpression constantResOfFoldedExpr;

    private List<TursoTable> tablesFromOuterContext = new ArrayList<>();
    private List<Join> joinsInExpr;

    Map<String, List<TursoConstant>> auxiliaryQueryResult = new HashMap<>();
    Map<String, List<TursoConstant>> selectResult = new HashMap<>();

    Boolean useSubqueryAsFoldedExpr;
    Boolean useCorrelatedSubqueryAsFoldedExpr;

    public TursoCODDTestOracle(TursoGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        TursoErrors.addExpectedExpressionErrors(errors);
        TursoErrors.addMatchQueryErrors(errors);
        TursoErrors.addQueryErrors(errors);
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

        TursoSelect auxiliaryQuery = null;
        if (useSubqueryAsFoldedExpr) {
            if (useCorrelatedSubqueryAsFoldedExpr) {
                auxiliaryQuery = genSelectWithCorrelatedSubquery();
                auxiliaryQueryString = TursoVisitor.asString(auxiliaryQuery);

                auxiliaryQueryResult.putAll(selectResult);
            } else {
                auxiliaryQuery = genSelectExpression(null, null);
                auxiliaryQueryString = TursoVisitor.asString(auxiliaryQuery);
                auxiliaryQueryResult = getQueryResult(auxiliaryQueryString, state);
            }
        } else {
            auxiliaryQuery = genSimpleSelect();
            auxiliaryQueryString = TursoVisitor.asString(auxiliaryQuery);

            auxiliaryQueryResult.putAll(selectResult);
        }

        TursoSelect originalQuery = null;

        Map<String, List<TursoConstant>> foldedResult = new HashMap<>();
        Map<String, List<TursoConstant>> originalResult = new HashMap<>();

        // dependent expression
        if (!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr) {
            // original query
            TursoExpressionBag specificCondition = new TursoExpressionBag(this.foldedExpr);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = TursoVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            specificCondition.updateInnerExpr(this.constantResOfFoldedExpr);
            foldedQueryString = TursoVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } else if (auxiliaryQueryResult.isEmpty()
                || auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().iterator().next()).isEmpty()) {
            // independent expression
            // empty result, put the inner query in (NOT) EXIST
            boolean isNegated = !Randomly.getBoolean();
            // original query
            TursoExist existExpr = new TursoExist(new TursoSelect(auxiliaryQuery), isNegated);
            TursoExpressionBag specificCondition = new TursoExpressionBag(existExpr);

            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = TursoVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            TursoExpression equivalentExpr = isNegated ? TursoConstant.createTrue() : TursoConstant.createFalse();
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = TursoVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } else if (auxiliaryQueryResult.size() == 1
                && auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0]).size() == 1
                && Randomly.getBoolean()) {
            // Scalar Subquery: 1 column and 1 row, consider the inner query as a constant
            // original query
            TursoExpressionBag specificCondition = new TursoExpressionBag(auxiliaryQuery);
            originalQuery = this.genSelectExpression(null, specificCondition);
            originalQueryString = TursoVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);

            // folded query
            TursoExpression equivalentExpr = auxiliaryQueryResult.get(auxiliaryQueryResult.keySet().toArray()[0])
                    .get(0);
            specificCondition.updateInnerExpr(equivalentExpr);
            foldedQueryString = TursoVisitor.asString(originalQuery);
            foldedResult = getQueryResult(foldedQueryString, state);
        } else if (auxiliaryQueryResult.size() == 1 && Randomly.getBooleanWithRatherLowProbability()
                && enableInOperator()) {
            // one column
            // original query
            List<TursoColumn> columns = s.getRandomTableNonEmptyTables().getColumns();
            TursoColumnName selectedColumn = new TursoColumnName(Randomly.fromList(columns), null);
            TursoTable selectedTable = selectedColumn.getColumn().getTable();
            InOperation inOperation = new InOperation(selectedColumn, new TursoSelect(auxiliaryQuery));
            TursoExpressionBag specificCondition = new TursoExpressionBag(inOperation);

            originalQuery = this.genSelectExpression(selectedTable, specificCondition);
            originalQueryString = TursoVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            // folded query
            // can not use IN VALUES here, because there is no affinity for the right operand of IN when right operand
            // is a list
            try {
                TursoTable t = this.createTemporaryTable(auxiliaryQuery, "intable");
                TursoTableReference equivalentTable = new TursoTableReference(t);
                inOperation = new InOperation(selectedColumn, equivalentTable);
                specificCondition.updateInnerExpr(inOperation);
                foldedQueryString = TursoVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } finally {
                dropTemporaryTable("intable");
            }
        } else {
            // There is not `ANY` and `ALL` operator in Turso
            // Row Subquery
            // original query
            TursoTable temporaryTable = this.genTemporaryTable(auxiliaryQuery, TursoCODDTestOracle.TEMP_TABLE_NAME);
            originalQuery = this.genSelectExpression(temporaryTable, null);
            TursoTableAndColumnRef tableAndColumnRef = new TursoTableAndColumnRef(temporaryTable);
            TursoWithClause withClause = new TursoWithClause(tableAndColumnRef, new TursoSelect(auxiliaryQuery));
            originalQuery.setWithClause(withClause);
            originalQueryString = TursoVisitor.asString(originalQuery);
            originalResult = getQueryResult(originalQueryString, state);
            // folded query
            if (Randomly.getBoolean() && this.enableCommonTableExpression()) {
                // there are too many false positives
                // common table expression
                // folded query: WITH table AS VALUES ()
                TursoValues values = new TursoValues(auxiliaryQueryResult, temporaryTable.getColumns());
                originalQuery.updateWithClauseRight(values);
                foldedQueryString = TursoVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (Randomly.getBoolean() && this.enableDerivedTable()) {
                // derived table
                // folded query: SELECT FROM () AS table
                originalQuery.setWithClause(null);
                TursoTableReference tempTableRef = new TursoTableReference(temporaryTable);
                TursoAlias alias = new TursoAlias(new TursoSelect(auxiliaryQuery), tempTableRef);
                originalQuery.replaceFromTable(TursoCODDTestOracle.TEMP_TABLE_NAME, alias);
                foldedQueryString = TursoVisitor.asString(originalQuery);
                foldedResult = getQueryResult(foldedQueryString, state);
            } else if (this.enableInsert()) {
                // there are too many false positives
                // folded query: CREATE the table and INSERT INTO table subquery
                try {
                    this.createTemporaryTable(auxiliaryQuery, TursoCODDTestOracle.TEMP_TABLE_NAME);
                    originalQuery.setWithClause(null);
                    foldedQueryString = TursoVisitor.asString(originalQuery);
                    foldedResult = getQueryResult(foldedQueryString, state);
                } finally {
                    dropTemporaryTable(TursoCODDTestOracle.TEMP_TABLE_NAME);
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

    private TursoSelect genSelectExpression(TursoTable tempTable, TursoExpression specificCondition) {
        TursoTables randomTables = s.getRandomTableNonEmptyTables();
        if (tempTable != null) {
            randomTables.addTable(tempTable);
        }
        if (!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr) {
            for (TursoTable t : this.tablesFromOuterContext) {
                randomTables.addTable(t);
            }
            if (this.joinsInExpr != null) {
                for (Join j : this.joinsInExpr) {
                    TursoTable t = j.getTable();
                    randomTables.removeTable(t);
                }
            }
        }

        List<TursoColumn> columns = randomTables.getColumns();
        if ((!useSubqueryAsFoldedExpr || useSubqueryAsFoldedExpr && useCorrelatedSubqueryAsFoldedExpr)
                && this.joinsInExpr != null) {
            for (Join j : this.joinsInExpr) {
                TursoTable t = j.getTable();
                columns.addAll(t.getColumns());
            }
        }
        gen = new TursoExpressionGenerator(state).setColumns(columns);
        List<TursoTable> tables = randomTables.getTables();
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
        List<TursoExpression> tableRefs = TursoCommon.getTableRefs(tables, s);
        TursoSelect select = new TursoSelect();
        select.setFromList(tableRefs);
        if (!joinStatements.isEmpty()) {
            select.setJoinClauses(joinStatements);
        }

        TursoExpression randomWhereCondition = gen.generateExpression();
        TursoExpression whereCondition = null;
        if (specificCondition != null) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            whereCondition = new TursoExpression.TursoBinaryOperation(randomWhereCondition, specificCondition,
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
            List<TursoColumn> selectedColumns = Randomly.nonEmptySubset(columns);
            List<TursoExpression> selectedAlias = new LinkedList<>();
            for (int i = 0; i < selectedColumns.size(); ++i) {
                TursoColumnName originalName = new TursoColumnName(selectedColumns.get(i), null);
                TursoColumnName aliasName = new TursoColumnName(TursoColumn.createDummy("c" + i), null);
                TursoAlias columnAlias = new TursoAlias(originalName, aliasName);
                selectedAlias.add(columnAlias);
            }
            select.setFetchColumns(selectedAlias);
        } else {
            TursoColumnName aggr = new TursoColumnName(Randomly.fromList(columns), null);
            TursoProvider.mustKnowResult = true;
            TursoExpression originalName = new TursoAggregate(Arrays.asList(aggr),
                    TursoAggregate.TursoAggregateFunction.getRandom());
            TursoColumnName aliasName = new TursoColumnName(TursoColumn.createDummy("c0"), null);
            TursoAlias columnAlias = new TursoAlias(originalName, aliasName);
            select.setFetchColumns(Arrays.asList(columnAlias));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                List<TursoExpression> groupByClause = genGroupByClause(columns, specificCondition);
                select.setGroupByClause(groupByClause);
                if (!groupByClause.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
                    select.setHavingClause(genHavingClause(columns, specificCondition));
                }
            }
        }
        return select;
    }

    // For expression test
    private TursoSelect genSimpleSelect() {
        TursoTables randomTables = s.getRandomTableNonEmptyTables();
        List<TursoColumn> columns = randomTables.getColumns();

        gen = new TursoExpressionGenerator(state).setColumns(columns);
        List<TursoTable> tables = randomTables.getTables();
        tablesFromOuterContext = randomTables.getTables();

        if (Randomly.getBooleanWithRatherLowProbability()) {
            joinsInExpr = genJoinExpression(gen, tables, null, true);
        } else {
            joinsInExpr = new ArrayList<>();
        }

        List<TursoExpression> tableRefs = TursoCommon.getTableRefs(tables, s);
        TursoSelect select = new TursoSelect();
        select.setFromList(tableRefs);
        if (joinsInExpr != null && !joinsInExpr.isEmpty()) {
            select.setJoinClauses(joinsInExpr);
        }

        TursoExpression whereCondition = gen.generateExpression();
        this.foldedExpr = whereCondition;

        List<TursoExpression> fetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (TursoColumn c : randomTables.getColumns()) {
            TursoColumnName cRef = new TursoColumnName(c, null);
            TursoColumnName aliasName = new TursoColumnName(TursoColumn.createDummy("c" + columnIdx), null);
            TursoAlias columnAlias = new TursoAlias(cRef, aliasName);
            fetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        TursoColumnName aliasName = new TursoColumnName(TursoColumn.createDummy("c" + columnIdx), null);
        TursoAlias columnAlias = new TursoAlias(whereCondition, aliasName);
        fetchColumns.add(columnAlias);

        select.setFetchColumns(fetchColumns);

        Map<String, List<TursoConstant>> queryRes = null;
        try {
            queryRes = getQueryResult(TursoVisitor.asString(select), state);
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
        List<TursoConstant> summary = queryRes.remove("c" + columnIdx);

        List<TursoColumn> tempColumnList = new ArrayList<>();

        for (int i = 0; i < fetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            TursoAlias cAlias = (TursoAlias) fetchColumns.get(i);
            TursoColumnName cRef = (TursoColumnName) cAlias.getOriginalExpression();
            TursoColumn column = cRef.getColumn();
            String columnName = TursoVisitor.asString(cAlias.getAliasExpression());
            TursoColumn newColumn = new TursoColumn(columnName, column.getType(), false, false, null);
            tempColumnList.add(newColumn);
        }
        List<TursoColumnName> columnRef = new ArrayList<>();
        for (TursoColumn c : randomTables.getColumns()) {
            columnRef.add(new TursoColumnName(c, null));
        }
        if (tempColumnList.size() != queryRes.size()) {
            throw new AssertionError();
        }
        TursoValues values = new TursoValues(queryRes, tempColumnList);
        this.constantResOfFoldedExpr = new TursoResultMap(values, columnRef, summary, null);

        return select;
    }

    private TursoSelect genSelectWithCorrelatedSubquery() {
        TursoTables outerQueryRandomTables = s.getRandomTableNonEmptyTables();
        TursoTables innerQueryRandomTables = s.getRandomTableNonEmptyTables();

        List<TursoExpression> innerQueryFromTables = new ArrayList<>();
        for (TursoTable t : innerQueryRandomTables.getTables()) {
            if (!outerQueryRandomTables.isContained(t)) {
                innerQueryFromTables.add(new TursoTableReference(t));
            }
        }
        for (TursoTable t : outerQueryRandomTables.getTables()) {
            if (innerQueryRandomTables.isContained(t)) {
                innerQueryRandomTables.removeTable(t);

                List<TursoColumn> newColumns = new ArrayList<>();
                for (TursoColumn c : t.getColumns()) {
                    TursoColumn newColumn = new TursoColumn(c.getName(), c.getType(), false, null, false);
                    newColumns.add(newColumn);
                }
                TursoTable newTable = new TursoTable(t.getName() + "a", newColumns, null, true, false, false,
                        false);
                for (TursoColumn c : newColumns) {
                    c.setTable(newTable);
                }
                innerQueryRandomTables.addTable(newTable);

                TursoAlias alias = new TursoAlias(new TursoTableReference(t),
                        new TursoTableReference(newTable));
                innerQueryFromTables.add(alias);
            }
        }

        List<TursoColumn> innerQueryColumns = new ArrayList<>();
        innerQueryColumns.addAll(innerQueryRandomTables.getColumns());
        innerQueryColumns.addAll(outerQueryRandomTables.getColumns());
        gen = new TursoExpressionGenerator(state).setColumns(innerQueryColumns);

        TursoSelect innerQuery = new TursoSelect();
        innerQuery.setFromList(innerQueryFromTables);

        TursoExpression innerQueryWhereCondition = gen.generateExpression();
        innerQuery.setWhereClause(innerQueryWhereCondition);

        // use aggregate function in fetch column
        TursoColumnName innerQueryAggr = new TursoColumnName(Randomly.fromList(innerQueryRandomTables.getColumns()),
                null);
        TursoProvider.mustKnowResult = true;
        TursoExpression innerQueryAggrName = new TursoAggregate(Arrays.asList(innerQueryAggr),
                TursoAggregate.TursoAggregateFunction.getRandom());
        innerQuery.setFetchColumns(Arrays.asList(innerQueryAggrName));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<TursoExpression> groupByClause = genGroupByClause(innerQueryColumns, null);
            innerQuery.setGroupByClause(groupByClause);
            if (!groupByClause.isEmpty() && Randomly.getBooleanWithRatherLowProbability()) {
                innerQuery.setHavingClause(genHavingClause(innerQueryColumns, null));
            }
        }

        this.foldedExpr = innerQuery;

        // outer query
        TursoSelect outerQuery = new TursoSelect();
        outerQuery.setFromList(TursoCommon.getTableRefs(outerQueryRandomTables.getTables(), s));
        tablesFromOuterContext = outerQueryRandomTables.getTables();

        List<TursoExpression> outerQueryFetchColumns = new ArrayList<>();
        int columnIdx = 0;
        for (TursoColumn c : outerQueryRandomTables.getColumns()) {
            TursoColumnName cRef = new TursoColumnName(c, null);
            TursoColumnName aliasName = new TursoColumnName(TursoColumn.createDummy("c" + columnIdx), null);
            TursoAlias columnAlias = new TursoAlias(cRef, aliasName);
            outerQueryFetchColumns.add(columnAlias);
            columnIdx++;
        }

        // add the expression to fetch clause
        TursoColumnName aliasName = new TursoColumnName(TursoColumn.createDummy("c" + columnIdx), null);
        TursoAlias columnAlias = new TursoAlias(innerQuery, aliasName);
        outerQueryFetchColumns.add(columnAlias);

        outerQuery.setFetchColumns(outerQueryFetchColumns);

        originalQueryString = TursoVisitor.asString(outerQuery);

        Map<String, List<TursoConstant>> queryRes = null;
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
        List<TursoConstant> summary = queryRes.remove("c" + columnIdx);

        List<TursoColumn> tempColumnList = new ArrayList<>();

        for (int i = 0; i < outerQueryFetchColumns.size() - 1; ++i) {
            // do not put the last fetch column to values
            TursoAlias cAlias = (TursoAlias) outerQueryFetchColumns.get(i);
            TursoColumnName cRef = (TursoColumnName) cAlias.getOriginalExpression();
            TursoColumn column = cRef.getColumn();
            String columnName = TursoVisitor.asString(cAlias.getAliasExpression());
            TursoColumn newColumn = new TursoColumn(columnName, column.getType(), false, false, null);
            tempColumnList.add(newColumn);
        }
        List<TursoColumnName> columnRef = new ArrayList<>();
        for (TursoColumn c : outerQueryRandomTables.getColumns()) {
            columnRef.add(new TursoColumnName(c, null));
        }
        if (tempColumnList.size() != queryRes.size()) {
            throw new AssertionError();
        }
        TursoValues values = new TursoValues(queryRes, tempColumnList);
        this.constantResOfFoldedExpr = new TursoResultMap(values, columnRef, summary, null);

        return outerQuery;
    }

    private List<Join> genJoinExpression(TursoExpressionGenerator gen, List<TursoTable> tables,
            TursoExpression specificCondition, boolean joinForExperssion) {
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
                TursoExpression randomOnCondition = gen.generateExpression();
                TursoExpression onCondition = null;
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    BinaryOperator operator = BinaryOperator.getRandomOperator();
                    onCondition = new TursoExpression.TursoBinaryOperation(randomOnCondition, specificCondition,
                            operator);
                } else {
                    onCondition = randomOnCondition;
                }

                TursoTable table = Randomly.fromList(tables);
                tables.remove(table);
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    onCondition = null;
                }
                Join j = new TursoExpression.Join(table, onCondition, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }

    private List<TursoExpression> genOrderBysExpression(TursoExpressionGenerator gen,
            TursoExpression specificCondition) {
        List<TursoExpression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(
                    genOrderingTerm(gen, Randomly.getBooleanWithRatherLowProbability() ? specificCondition : null));
        }
        return expressions;
    }

    private TursoExpression genOrderingTerm(TursoExpressionGenerator gen, TursoExpression specificCondition) {
        TursoExpression expr = gen.generateExpression();
        if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            expr = new TursoExpression.TursoBinaryOperation(expr, specificCondition, operator);
        }
        // COLLATE is potentially already generated
        if (Randomly.getBoolean()) {
            expr = new TursoOrderingTerm(expr, Ordering.getRandomValue());
        }
        if (state.getDbmsSpecificOptions().testNullsFirstLast && Randomly.getBoolean()) {
            expr = new TursoPostfixText(expr, Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
                    null /* expr.getExpectedValue() */) {
                @Override
                public boolean omitBracketsWhenPrinting() {
                    return true;
                }
            };
        }
        return expr;
    }

    private List<TursoExpression> genGroupByClause(List<TursoColumn> columns, TursoExpression specificCondition) {
        errors.add("GROUP BY term out of range");
        if (Randomly.getBoolean()) {
            List<TursoExpression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                TursoExpression expr = new TursoExpressionGenerator(state).setColumns(columns).generateExpression();
                if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
                    BinaryOperator operator = BinaryOperator.getRandomOperator();
                    expr = new TursoExpression.TursoBinaryOperation(expr, specificCondition, operator);
                }
                collect.add(expr);
            }
            return collect;
        }
        return Collections.emptyList();
    }

    private TursoExpression genHavingClause(List<TursoColumn> columns, TursoExpression specificCondition) {
        TursoExpression expr = new TursoExpressionGenerator(state).setColumns(columns).generateExpression();
        if (specificCondition != null && Randomly.getBooleanWithRatherLowProbability()) {
            BinaryOperator operator = BinaryOperator.getRandomOperator();
            expr = new TursoExpression.TursoBinaryOperation(expr, specificCondition, operator);
        }
        return expr;
    }

    private Map<String, List<TursoConstant>> getQueryResult(String queryString, TursoGlobalState state)
            throws SQLException {
        Map<String, List<TursoConstant>> result = new LinkedHashMap<>();
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
                            TursoConstant constant;
                            if (rs.wasNull()) {
                                constant = TursoConstant.createNullConstant();
                            } else if (value instanceof Integer) {
                                constant = TursoConstant.createIntConstant(Long.valueOf((Integer) value));
                            } else if (value instanceof Short) {
                                constant = TursoConstant.createIntConstant(Long.valueOf((Short) value));
                            } else if (value instanceof Long) {
                                constant = TursoConstant.createIntConstant((Long) value);
                            } else if (value instanceof Double) {
                                constant = TursoConstant.createRealConstant((double) value);
                            } else if (value instanceof Float) {
                                constant = TursoConstant.createRealConstant(((Float) value).doubleValue());
                            } else if (value instanceof BigDecimal) {
                                constant = TursoConstant.createRealConstant(((BigDecimal) value).doubleValue());
                            } else if (value instanceof byte[]) {
                                constant = TursoConstant.createBinaryConstant((byte[]) value);
                            } else if (value instanceof Boolean) {
                                constant = TursoConstant.createBoolean((boolean) value);
                            } else if (value instanceof String) {
                                constant = TursoConstant.createTextConstant((String) value);
                            } else if (value == null) {
                                constant = TursoConstant.createNullConstant();
                            } else {
                                throw new IgnoreMeException();
                            }
                            List<TursoConstant> v = result.get(idxNameMap.get(i));
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

    private TursoTable genTemporaryTable(TursoSelect select, String tableName) {
        List<TursoExpression> fetchColumns = select.getFetchColumns();
        int columnNumber = fetchColumns.size();
        Map<Integer, TursoDataType> idxTypeMap = getColumnTypeFromSelect(select);

        List<TursoColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + i;
            TursoColumn column = new TursoColumn(columnName, idxTypeMap.get(i), false, false, null);
            databaseColumns.add(column);
        }
        TursoTable table = new TursoTable(tableName, databaseColumns, null, false, false, false, false);
        for (TursoColumn c : databaseColumns) {
            c.setTable(table);
        }

        return table;
    }

    private TursoTable createTemporaryTable(TursoSelect select, String tableName) throws SQLException {
        String selectString = TursoVisitor.asString(select);
        Map<Integer, TursoDataType> idxTypeMap = getColumnTypeFromSelect(select);

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

        List<TursoColumn> databaseColumns = new ArrayList<>();
        for (int i = 0; i < columnNumber; ++i) {
            String columnName = "c" + i;
            TursoColumn column = new TursoColumn(columnName, idxTypeMap.get(i), false, false, null);
            databaseColumns.add(column);
        }
        TursoTable table = new TursoTable(tableName, databaseColumns, null, false, false, false, false);
        for (TursoColumn c : databaseColumns) {
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

    private boolean compareResult(Map<String, List<TursoConstant>> r1, Map<String, List<TursoConstant>> r2) {
        if (r1.size() != r2.size()) {
            return false;
        }
        for (Map.Entry<String, List<TursoConstant>> entry : r1.entrySet()) {
            String currentKey = entry.getKey();
            if (!r2.containsKey(currentKey)) {
                return false;
            }
            List<TursoConstant> v1 = entry.getValue();
            List<TursoConstant> v2 = r2.get(currentKey);
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

    private Map<Integer, TursoDataType> getColumnTypeFromSelect(TursoSelect select) {
        List<TursoExpression> fetchColumns = select.getFetchColumns();
        List<TursoExpression> newFetchColumns = new ArrayList<>();
        for (TursoExpression column : fetchColumns) {
            newFetchColumns.add(column);
            TursoAlias columnAlias = (TursoAlias) column;
            TursoExpression typeofColumn = new TursoTypeof(columnAlias.getOriginalExpression());
            newFetchColumns.add(typeofColumn);
        }
        TursoSelect newSelect = new TursoSelect(select);
        newSelect.setFetchColumns(newFetchColumns);
        Map<String, List<TursoConstant>> typeResult = null;
        try {
            typeResult = getQueryResult(TursoVisitor.asString(newSelect), state);
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
        Map<Integer, TursoDataType> idxTypeMap = new HashMap<>();
        for (int i = 0; i * 2 < typeResult.size(); ++i) {
            String columnName = "c" + (i * 2 + 1);
            TursoExpression t = typeResult.get(columnName).get(0);
            TursoTextConstant tString = (TursoTextConstant) t;
            String typeName = tString.asString();
            TursoDataType cType = TursoDataType.getTypeFromName(typeName);
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
    public Reproducer<TursoGlobalState> getLastReproducer() {
        return reproducer;
    }
}
