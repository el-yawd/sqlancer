package sqlancer.limbo.oracle.tlp;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboAggregate;
import sqlancer.limbo.ast.LimboAggregate.LimboAggregateFunction;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixText;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboUnaryOperation;
import sqlancer.limbo.ast.LimboUnaryOperation.UnaryOperator;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboTables;

public class LimboTLPAggregateOracle implements TestOracle<LimboGlobalState> {

    private final LimboGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private LimboExpressionGenerator gen;
    private String generatedQueryString;

    public LimboTLPAggregateOracle(LimboGlobalState state) {
        this.state = state;
        LimboErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        LimboSchema s = state.getSchema();
        LimboTables targetTables = s.getRandomTableNonEmptyTables();
        gen = new LimboExpressionGenerator(state).setColumns(targetTables.getColumns());
        LimboSelect select = new LimboSelect();
        LimboAggregateFunction windowFunction = Randomly.fromOptions(LimboAggregate.LimboAggregateFunction.MIN,
                LimboAggregate.LimboAggregateFunction.MAX, LimboAggregateFunction.SUM,
                LimboAggregateFunction.TOTAL);
        LimboAggregate aggregate = new LimboAggregate(gen.getRandomExpressions(1), windowFunction);
        select.setFetchColumns(Arrays.asList(aggregate));
        List<LimboExpression> from = LimboCommon.getTableRefs(targetTables.getTables(), s);
        select.setFromList(from);
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        String originalQuery = LimboVisitor.asString(select);
        generatedQueryString = originalQuery;
        LimboExpression whereClause = gen.generateExpression();
        LimboUnaryOperation negatedClause = new LimboUnaryOperation(UnaryOperator.NOT, whereClause);
        LimboPostfixUnaryOperation notNullClause = new LimboPostfixUnaryOperation(PostfixUnaryOperator.ISNULL,
                whereClause);

        LimboSelect leftSelect = getSelect(aggregate, from, whereClause);
        LimboSelect middleSelect = getSelect(aggregate, from, negatedClause);
        LimboSelect rightSelect = getSelect(aggregate, from, notNullClause);
        String aggreateMethod = aggregate.getFunc() == LimboAggregateFunction.COUNT_ALL
                ? LimboAggregateFunction.COUNT.toString() : aggregate.getFunc().toString();
        String metamorphicText = "SELECT " + aggreateMethod + "(aggr) FROM (";
        metamorphicText += LimboVisitor.asString(leftSelect) + " UNION ALL " + LimboVisitor.asString(middleSelect)
                + " UNION ALL " + LimboVisitor.asString(rightSelect);
        metamorphicText += ")";

        // String finalText = originalQuery + " INTERSECT " + metamorphicText;
        // state.getState().queryString = "--" + finalText;
        String firstResult;
        String secondResult;
        SQLQueryAdapter q = new SQLQueryAdapter(originalQuery, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            firstResult = result.getString(1);
        } catch (Exception e) {
            // TODO
            throw new IgnoreMeException();
        }

        SQLQueryAdapter q2 = new SQLQueryAdapter(metamorphicText, errors);
        try (SQLancerResultSet result = q2.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            secondResult = result.getString(1);
        } catch (Exception e) {
            // TODO
            throw new IgnoreMeException();
        }
        state.getState().getLocalState()
                .log("--" + originalQuery + "\n--" + metamorphicText + "\n-- " + firstResult + "\n-- " + secondResult);
        if ((firstResult == null && secondResult != null
                || firstResult != null && !firstResult.contentEquals(secondResult))
                && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {

            throw new AssertionError();

        }

    }

    private LimboSelect getSelect(LimboAggregate aggregate, List<LimboExpression> from,
            LimboExpression whereClause) {
        LimboSelect leftSelect = new LimboSelect();
        leftSelect.setFetchColumns(Arrays.asList(new LimboPostfixText(aggregate, " as aggr", null)));
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            leftSelect.setGroupByClause(gen.getRandomExpressions(Randomly.smallNumber() + 1));
        }
        if (Randomly.getBoolean()) {
            leftSelect.setOrderByClauses(gen.generateOrderBys());
        }
        return leftSelect;
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }

}
