package sqlancer.turso.oracle.tlp;

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
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoAggregate;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoUnaryOperation;
import sqlancer.turso.ast.TursoAggregate.TursoAggregateFunction;
import sqlancer.turso.ast.TursoExpression.TursoPostfixText;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.turso.ast.TursoUnaryOperation.UnaryOperator;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoTables;

public class TursoTLPAggregateOracle implements TestOracle<TursoGlobalState> {

    private final TursoGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private TursoExpressionGenerator gen;
    private String generatedQueryString;

    public TursoTLPAggregateOracle(TursoGlobalState state) {
        this.state = state;
        TursoErrors.addExpectedExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        TursoSchema s = state.getSchema();
        TursoTables targetTables = s.getRandomTableNonEmptyTables();
        gen = new TursoExpressionGenerator(state).setColumns(targetTables.getColumns());
        TursoSelect select = new TursoSelect();
        TursoAggregateFunction windowFunction = Randomly.fromOptions(TursoAggregate.TursoAggregateFunction.MIN,
                TursoAggregate.TursoAggregateFunction.MAX, TursoAggregateFunction.SUM,
                TursoAggregateFunction.TOTAL);
        TursoAggregate aggregate = new TursoAggregate(gen.getRandomExpressions(1), windowFunction);
        select.setFetchColumns(Arrays.asList(aggregate));
        List<TursoExpression> from = TursoCommon.getTableRefs(targetTables.getTables(), s);
        select.setFromList(from);
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(gen.generateOrderBys());
        }
        String originalQuery = TursoVisitor.asString(select);
        generatedQueryString = originalQuery;
        TursoExpression whereClause = gen.generateExpression();
        TursoUnaryOperation negatedClause = new TursoUnaryOperation(UnaryOperator.NOT, whereClause);
        TursoPostfixUnaryOperation notNullClause = new TursoPostfixUnaryOperation(PostfixUnaryOperator.ISNULL,
                whereClause);

        TursoSelect leftSelect = getSelect(aggregate, from, whereClause);
        TursoSelect middleSelect = getSelect(aggregate, from, negatedClause);
        TursoSelect rightSelect = getSelect(aggregate, from, notNullClause);
        String aggreateMethod = aggregate.getFunc() == TursoAggregateFunction.COUNT_ALL
                ? TursoAggregateFunction.COUNT.toString() : aggregate.getFunc().toString();
        String metamorphicText = "SELECT " + aggreateMethod + "(aggr) FROM (";
        metamorphicText += TursoVisitor.asString(leftSelect) + " UNION ALL " + TursoVisitor.asString(middleSelect)
                + " UNION ALL " + TursoVisitor.asString(rightSelect);
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

    private TursoSelect getSelect(TursoAggregate aggregate, List<TursoExpression> from,
            TursoExpression whereClause) {
        TursoSelect leftSelect = new TursoSelect();
        leftSelect.setFetchColumns(Arrays.asList(new TursoPostfixText(aggregate, " as aggr", null)));
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
