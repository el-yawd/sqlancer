package sqlancer.turso.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoAggregate;
import sqlancer.turso.ast.TursoCast;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoUnaryOperation;
import sqlancer.turso.ast.TursoWindowFunction;
import sqlancer.turso.ast.TursoAggregate.TursoAggregateFunction;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoExpression.TursoDistinct;
import sqlancer.turso.ast.TursoExpression.TursoPostfixText;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation;
import sqlancer.turso.ast.TursoExpression.Join.JoinType;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.turso.ast.TursoUnaryOperation.UnaryOperator;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoRowValue;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoTables;

public class TursoPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<TursoGlobalState, TursoRowValue, TursoExpression, SQLConnection> {

    private List<TursoColumn> fetchColumns;
    private OracleRunReproductionState localState;

    public TursoPivotedQuerySynthesisOracle(TursoGlobalState globalState) {
        super(globalState);
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        TursoSelect selectStatement = getQuery();
        TursoErrors.addExpectedExpressionErrors(errors);
        return new SQLQueryAdapter(TursoVisitor.asString(selectStatement), errors);
    }

    public TursoSelect getQuery() throws SQLException {
        assert !globalState.getSchema().getDatabaseTables().isEmpty();
        localState = globalState.getState().getLocalState();
        assert localState != null;
        TursoTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<TursoTable> tables = randomFromTables.getTables();

        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());
        TursoSelect selectStatement = new TursoSelect();
        selectStatement.setSelectType(Randomly.fromOptions(TursoSelect.SelectType.values()));
        List<TursoColumn> columns = randomFromTables.getColumns();
        // filter out row ids from the select because the hinder the reduction process
        // once a bug is found
        List<TursoColumn> columnsWithoutRowid = columns.stream()
                .filter(c -> !TursoSchema.ROWID_STRINGS.contains(c.getName())).collect(Collectors.toList());
        List<Join> joinStatements = getJoinStatements(globalState, tables, columnsWithoutRowid);
        selectStatement.setJoinClauses(joinStatements);
        selectStatement.setFromList(TursoCommon.getTableRefs(tables, globalState.getSchema()));

        fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid);
        List<TursoTable> allTables = new ArrayList<>();
        allTables.addAll(tables);
        allTables.addAll(joinStatements.stream().map(join -> join.getTable()).collect(Collectors.toList()));
        boolean allTablesContainOneRow = allTables.stream().allMatch(t -> t.getNrRows(globalState) == 1);
        boolean testAggregateFunctions = allTablesContainOneRow && globalState.getOptions().testAggregateFunctionsPQS();
        pivotRowExpression = getColExpressions(testAggregateFunctions, columnsWithoutRowid);
        selectStatement.setFetchColumns(pivotRowExpression);
        TursoExpression whereClause = generateRectifiedExpression(columnsWithoutRowid, pivotRow, false);
        selectStatement.setWhereClause(whereClause);
        List<TursoExpression> groupByClause = generateGroupByClause(columnsWithoutRowid, pivotRow,
                allTablesContainOneRow);
        selectStatement.setGroupByClause(groupByClause);
        TursoExpression limitClause = generateLimit((long) (Math.pow(globalState.getOptions().getMaxNumberInserts(),
                joinStatements.size() + randomFromTables.getTables().size())));
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            TursoExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        /* PQS does not check for ordering, so we can generate any ORDER BY clause */
        List<TursoExpression> orderBy = new TursoExpressionGenerator(globalState).generateOrderBys();
        selectStatement.setOrderByClauses(orderBy);
        if (!groupByClause.isEmpty() && Randomly.getBoolean()) {
            selectStatement.setHavingClause(generateRectifiedExpression(columns, pivotRow, true));
        }
        return selectStatement;
    }

    private List<Join> getJoinStatements(TursoGlobalState globalState, List<TursoTable> tables,
            List<TursoColumn> columns) {
        List<Join> joinStatements = new TursoExpressionGenerator(globalState).getRandomJoinClauses(tables);
        for (Join j : joinStatements) {
            if (j.getType() == JoinType.NATURAL) {
                /* NATURAL joins have no on clause and cannot be rectified */
                j.setType(JoinType.INNER);
            }
            // ensure that the join does not exclude the pivot row
            j.setOnClause(generateRectifiedExpression(columns, pivotRow, false));
        }
        errors.add("ON clause references tables to its right");
        return joinStatements;
    }

    private List<TursoExpression> getColExpressions(boolean testAggregateFunctions, List<TursoColumn> columns) {
        List<TursoExpression> colExpressions = new ArrayList<>();

        for (TursoColumn c : fetchColumns) {
            TursoExpression colName = new TursoColumnName(c, pivotRow.getValues().get(c));
            if (testAggregateFunctions && Randomly.getBoolean()) {

                /*
                 * PQS cannot detect omitted or incorrectly-fetched duplicate rows, so we can generate DISTINCT
                 * statements
                 */
                boolean generateDistinct = Randomly.getBooleanWithRatherLowProbability();
                if (generateDistinct) {
                    colName = new TursoDistinct(colName);
                }

                TursoAggregateFunction aggFunc = TursoAggregateFunction.getRandom(c.getType());
                colName = new TursoAggregate(Arrays.asList(colName), aggFunc);
                if (Randomly.getBoolean() && !generateDistinct) {
                    colName = generateWindowFunction(columns, colName, true);
                }
                errors.add("second argument to nth_value must be a positive integer");
            }
            if (Randomly.getBoolean()) {
                TursoExpression randomExpression;
                randomExpression = new TursoExpressionGenerator(globalState).setColumns(columns)
                        .generateResultKnownExpression();
                colExpressions.add(randomExpression);
            } else {
                colExpressions.add(colName);
            }
        }
        if (testAggregateFunctions) {
            TursoWindowFunction windowFunction = TursoWindowFunction.getRandom(columns, globalState);
            TursoExpression windowExpr = generateWindowFunction(columns, windowFunction, false);
            colExpressions.add(windowExpr);
        }
        for (TursoExpression expr : colExpressions) {
            if (expr.getExpectedValue() == null) {
                throw new IgnoreMeException();
            }
        }
        return colExpressions;
    }

    private TursoExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return TursoConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        String checkForContainmentValues = getGeneralizedPivotRowValues();
        sb.append(checkForContainmentValues);
        globalState.getState().getLocalState()
                .log("-- we expect the following expression to be contained in the result set: "
                        + checkForContainmentValues);
        sb.append(" INTERSECT SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(")");
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, query.getExpectedErrors());
    }

    private String getGeneralizedPivotRowValues() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pivotRowExpression.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            TursoConstant expectedValue = pivotRowExpression.get(i).getExpectedValue();
            String value = TursoVisitor.asString(expectedValue);
            if (value.contains("�") || value.contains("\0")) {
                // encoding issues || Java does not completely strings with \0 characters
                throw new IgnoreMeException();
            }
            sb.append(value);
        }
        return sb.toString();
    }

    private TursoExpression generateLimit(long l) {
        if (Randomly.getBoolean()) {
            return TursoConstant.createIntConstant(globalState.getRandomly().getLong(l, Long.MAX_VALUE));
        } else {
            return null;
        }
    }

    private List<TursoExpression> generateGroupByClause(List<TursoColumn> columns, TursoRowValue rw,
            boolean allTablesContainOneRow) {
        errors.add("GROUP BY term out of range");
        if (allTablesContainOneRow && Randomly.getBoolean()) {
            List<TursoExpression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                collect.add(new TursoExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                        .generateExpression());
            }
            return collect;
        }
        if (Randomly.getBoolean()) {
            // ensure that we GROUP BY all columns
            List<TursoExpression> collect = columns.stream().map(c -> new TursoColumnName(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
            if (Randomly.getBoolean()) {
                for (int i = 0; i < Randomly.smallNumber(); i++) {
                    collect.add(new TursoExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                            .generateExpression());
                }
            }
            return collect;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Generates a predicate that is guaranteed to evaluate to <code>true</code> for the given pivot row. PQS uses this
     * method to generate predicates used in WHERE and JOIN clauses. See step 4 of the PQS paper.
     *
     * @param columns
     * @param pivotRow
     * @param allowAggregates
     *
     * @return an expression that evaluates to <code>true</code>.
     */
    private TursoExpression generateRectifiedExpression(List<TursoColumn> columns, TursoRowValue pivotRow,
            boolean allowAggregates) {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(globalState).setRowValue(pivotRow)
                .setColumns(columns);
        if (allowAggregates) {
            gen = gen.allowAggregateFunctions();
        }
        TursoExpression expr = gen.generateResultKnownExpression();
        TursoExpression rectifiedPredicate;
        if (expr.getExpectedValue().isNull()) {
            // the expr evaluates to NULL => rectify to "expr IS NULL"
            rectifiedPredicate = new TursoPostfixUnaryOperation(PostfixUnaryOperator.ISNULL, expr);
        } else if (TursoCast.isTrue(expr.getExpectedValue()).get()) {
            // the expr evaluates to TRUE => we can directly return it
            rectifiedPredicate = expr;
        } else {
            // the expr evaluates to FALSE 0> rectify to "NOT expr"
            rectifiedPredicate = new TursoUnaryOperation(UnaryOperator.NOT, expr);
        }
        rectifiedPredicates.add(rectifiedPredicate);
        return rectifiedPredicate;
    }

    //
    private TursoExpression generateWindowFunction(List<TursoColumn> columns, TursoExpression colName,
            boolean allowFilter) {
        StringBuilder sb = new StringBuilder();
        if (Randomly.getBoolean() && allowFilter) {
            appendFilter(columns, sb);
        }
        sb.append(" OVER ");
        sb.append("(");
        if (Randomly.getBoolean()) {
            appendPartitionBy(columns, sb);
        }
        if (Randomly.getBoolean()) {
            sb.append(TursoCommon.getOrderByAsString(columns, globalState));
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("RANGE", "ROWS", "GROUPS"));
            sb.append(" ");
            switch (Randomly.fromOptions(FrameSpec.values())) {
            case BETWEEN:
                sb.append("BETWEEN");
                sb.append(" UNBOUNDED PRECEDING AND CURRENT ROW");
                break;
            case UNBOUNDED_PRECEDING:
                sb.append("UNBOUNDED PRECEDING");
                break;
            case CURRENT_ROW:
                sb.append("CURRENT ROW");
                break;
            default:
                throw new AssertionError();
            }
            if (Randomly.getBoolean()) {
                sb.append(" EXCLUDE ");
                sb.append(Randomly.fromOptions("NO OTHERS", "TIES"));
            }
        }
        sb.append(")");
        TursoPostfixText windowFunction = new TursoPostfixText(colName, sb.toString(), colName.getExpectedValue());
        errors.add("misuse of aggregate");
        return windowFunction;
    }

    private void appendFilter(List<TursoColumn> columns, StringBuilder sb) {
        sb.append(" FILTER (WHERE ");
        sb.append(TursoVisitor.asString(generateRectifiedExpression(columns, pivotRow, false)));
        sb.append(")");
    }

    private void appendPartitionBy(List<TursoColumn> columns, StringBuilder sb) {
        sb.append(" PARTITION BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String orderingTerm;
            do {
                orderingTerm = TursoCommon.getOrderingTerm(columns, globalState);
            } while (orderingTerm.contains("ASC") || orderingTerm.contains("DESC"));
            // TODO investigate
            sb.append(orderingTerm);
        }
    }

    private enum FrameSpec {
        BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW
    }

    @Override
    protected String getExpectedValues(TursoExpression expr) {
        return TursoVisitor.asExpectedValues(expr);
    }

}
