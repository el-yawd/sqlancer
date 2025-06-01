package sqlancer.limbo.oracle;

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
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboAggregate;
import sqlancer.limbo.ast.LimboAggregate.LimboAggregateFunction;
import sqlancer.limbo.ast.LimboCast;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.ast.LimboExpression.Join.JoinType;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboExpression.LimboDistinct;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixText;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboUnaryOperation;
import sqlancer.limbo.ast.LimboUnaryOperation.UnaryOperator;
import sqlancer.limbo.ast.LimboWindowFunction;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboRowValue;
import sqlancer.limbo.schema.LimboSchema.LimboTable;
import sqlancer.limbo.schema.LimboSchema.LimboTables;

public class LimboPivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<LimboGlobalState, LimboRowValue, LimboExpression, SQLConnection> {

    private List<LimboColumn> fetchColumns;
    private OracleRunReproductionState localState;

    public LimboPivotedQuerySynthesisOracle(LimboGlobalState globalState) {
        super(globalState);
    }

    @Override
    public Query<SQLConnection> getRectifiedQuery() throws SQLException {
        LimboSelect selectStatement = getQuery();
        LimboErrors.addExpectedExpressionErrors(errors);
        return new SQLQueryAdapter(LimboVisitor.asString(selectStatement), errors);
    }

    public LimboSelect getQuery() throws SQLException {
        assert !globalState.getSchema().getDatabaseTables().isEmpty();
        localState = globalState.getState().getLocalState();
        assert localState != null;
        LimboTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<LimboTable> tables = randomFromTables.getTables();

        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());
        LimboSelect selectStatement = new LimboSelect();
        selectStatement.setSelectType(Randomly.fromOptions(LimboSelect.SelectType.values()));
        List<LimboColumn> columns = randomFromTables.getColumns();
        // filter out row ids from the select because the hinder the reduction process
        // once a bug is found
        List<LimboColumn> columnsWithoutRowid = columns.stream()
                .filter(c -> !LimboSchema.ROWID_STRINGS.contains(c.getName())).collect(Collectors.toList());
        List<Join> joinStatements = getJoinStatements(globalState, tables, columnsWithoutRowid);
        selectStatement.setJoinClauses(joinStatements);
        selectStatement.setFromList(LimboCommon.getTableRefs(tables, globalState.getSchema()));

        fetchColumns = Randomly.nonEmptySubset(columnsWithoutRowid);
        List<LimboTable> allTables = new ArrayList<>();
        allTables.addAll(tables);
        allTables.addAll(joinStatements.stream().map(join -> join.getTable()).collect(Collectors.toList()));
        boolean allTablesContainOneRow = allTables.stream().allMatch(t -> t.getNrRows(globalState) == 1);
        boolean testAggregateFunctions = allTablesContainOneRow && globalState.getOptions().testAggregateFunctionsPQS();
        pivotRowExpression = getColExpressions(testAggregateFunctions, columnsWithoutRowid);
        selectStatement.setFetchColumns(pivotRowExpression);
        LimboExpression whereClause = generateRectifiedExpression(columnsWithoutRowid, pivotRow, false);
        selectStatement.setWhereClause(whereClause);
        List<LimboExpression> groupByClause = generateGroupByClause(columnsWithoutRowid, pivotRow,
                allTablesContainOneRow);
        selectStatement.setGroupByClause(groupByClause);
        LimboExpression limitClause = generateLimit((long) (Math.pow(globalState.getOptions().getMaxNumberInserts(),
                joinStatements.size() + randomFromTables.getTables().size())));
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            LimboExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        /* PQS does not check for ordering, so we can generate any ORDER BY clause */
        List<LimboExpression> orderBy = new LimboExpressionGenerator(globalState).generateOrderBys();
        selectStatement.setOrderByClauses(orderBy);
        if (!groupByClause.isEmpty() && Randomly.getBoolean()) {
            selectStatement.setHavingClause(generateRectifiedExpression(columns, pivotRow, true));
        }
        return selectStatement;
    }

    private List<Join> getJoinStatements(LimboGlobalState globalState, List<LimboTable> tables,
            List<LimboColumn> columns) {
        List<Join> joinStatements = new LimboExpressionGenerator(globalState).getRandomJoinClauses(tables);
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

    private List<LimboExpression> getColExpressions(boolean testAggregateFunctions, List<LimboColumn> columns) {
        List<LimboExpression> colExpressions = new ArrayList<>();

        for (LimboColumn c : fetchColumns) {
            LimboExpression colName = new LimboColumnName(c, pivotRow.getValues().get(c));
            if (testAggregateFunctions && Randomly.getBoolean()) {

                /*
                 * PQS cannot detect omitted or incorrectly-fetched duplicate rows, so we can generate DISTINCT
                 * statements
                 */
                boolean generateDistinct = Randomly.getBooleanWithRatherLowProbability();
                if (generateDistinct) {
                    colName = new LimboDistinct(colName);
                }

                LimboAggregateFunction aggFunc = LimboAggregateFunction.getRandom(c.getType());
                colName = new LimboAggregate(Arrays.asList(colName), aggFunc);
                if (Randomly.getBoolean() && !generateDistinct) {
                    colName = generateWindowFunction(columns, colName, true);
                }
                errors.add("second argument to nth_value must be a positive integer");
            }
            if (Randomly.getBoolean()) {
                LimboExpression randomExpression;
                randomExpression = new LimboExpressionGenerator(globalState).setColumns(columns)
                        .generateResultKnownExpression();
                colExpressions.add(randomExpression);
            } else {
                colExpressions.add(colName);
            }
        }
        if (testAggregateFunctions) {
            LimboWindowFunction windowFunction = LimboWindowFunction.getRandom(columns, globalState);
            LimboExpression windowExpr = generateWindowFunction(columns, windowFunction, false);
            colExpressions.add(windowExpr);
        }
        for (LimboExpression expr : colExpressions) {
            if (expr.getExpectedValue() == null) {
                throw new IgnoreMeException();
            }
        }
        return colExpressions;
    }

    private LimboExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return LimboConstant.createIntConstant(0);
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
            LimboConstant expectedValue = pivotRowExpression.get(i).getExpectedValue();
            String value = LimboVisitor.asString(expectedValue);
            if (value.contains("�") || value.contains("\0")) {
                // encoding issues || Java does not completely strings with \0 characters
                throw new IgnoreMeException();
            }
            sb.append(value);
        }
        return sb.toString();
    }

    private LimboExpression generateLimit(long l) {
        if (Randomly.getBoolean()) {
            return LimboConstant.createIntConstant(globalState.getRandomly().getLong(l, Long.MAX_VALUE));
        } else {
            return null;
        }
    }

    private List<LimboExpression> generateGroupByClause(List<LimboColumn> columns, LimboRowValue rw,
            boolean allTablesContainOneRow) {
        errors.add("GROUP BY term out of range");
        if (allTablesContainOneRow && Randomly.getBoolean()) {
            List<LimboExpression> collect = new ArrayList<>();
            for (int i = 0; i < Randomly.smallNumber(); i++) {
                collect.add(new LimboExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                        .generateExpression());
            }
            return collect;
        }
        if (Randomly.getBoolean()) {
            // ensure that we GROUP BY all columns
            List<LimboExpression> collect = columns.stream().map(c -> new LimboColumnName(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
            if (Randomly.getBoolean()) {
                for (int i = 0; i < Randomly.smallNumber(); i++) {
                    collect.add(new LimboExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
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
    private LimboExpression generateRectifiedExpression(List<LimboColumn> columns, LimboRowValue pivotRow,
            boolean allowAggregates) {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(globalState).setRowValue(pivotRow)
                .setColumns(columns);
        if (allowAggregates) {
            gen = gen.allowAggregateFunctions();
        }
        LimboExpression expr = gen.generateResultKnownExpression();
        LimboExpression rectifiedPredicate;
        if (expr.getExpectedValue().isNull()) {
            // the expr evaluates to NULL => rectify to "expr IS NULL"
            rectifiedPredicate = new LimboPostfixUnaryOperation(PostfixUnaryOperator.ISNULL, expr);
        } else if (LimboCast.isTrue(expr.getExpectedValue()).get()) {
            // the expr evaluates to TRUE => we can directly return it
            rectifiedPredicate = expr;
        } else {
            // the expr evaluates to FALSE 0> rectify to "NOT expr"
            rectifiedPredicate = new LimboUnaryOperation(UnaryOperator.NOT, expr);
        }
        rectifiedPredicates.add(rectifiedPredicate);
        return rectifiedPredicate;
    }

    //
    private LimboExpression generateWindowFunction(List<LimboColumn> columns, LimboExpression colName,
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
            sb.append(LimboCommon.getOrderByAsString(columns, globalState));
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
        LimboPostfixText windowFunction = new LimboPostfixText(colName, sb.toString(), colName.getExpectedValue());
        errors.add("misuse of aggregate");
        return windowFunction;
    }

    private void appendFilter(List<LimboColumn> columns, StringBuilder sb) {
        sb.append(" FILTER (WHERE ");
        sb.append(LimboVisitor.asString(generateRectifiedExpression(columns, pivotRow, false)));
        sb.append(")");
    }

    private void appendPartitionBy(List<LimboColumn> columns, StringBuilder sb) {
        sb.append(" PARTITION BY ");
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String orderingTerm;
            do {
                orderingTerm = LimboCommon.getOrderingTerm(columns, globalState);
            } while (orderingTerm.contains("ASC") || orderingTerm.contains("DESC"));
            // TODO investigate
            sb.append(orderingTerm);
        }
    }

    private enum FrameSpec {
        BETWEEN, UNBOUNDED_PRECEDING, CURRENT_ROW
    }

    @Override
    protected String getExpectedValues(LimboExpression expr) {
        return LimboVisitor.asExpectedValues(expr);
    }

}
