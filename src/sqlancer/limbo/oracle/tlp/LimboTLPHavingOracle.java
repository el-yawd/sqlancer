package sqlancer.limbo.oracle.tlp;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboSelect.SelectType;
import sqlancer.limbo.ast.LimboUnaryOperation;
import sqlancer.limbo.ast.LimboUnaryOperation.UnaryOperator;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;
import sqlancer.limbo.schema.LimboSchema.LimboTables;

public class LimboTLPHavingOracle implements TestOracle<LimboGlobalState> {

    private final LimboGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private String generatedQueryString;

    public LimboTLPHavingOracle(LimboGlobalState state) {
        this.state = state;
        LimboErrors.addExpectedExpressionErrors(errors);
        errors.add("no such column"); // FIXME why?
        errors.add("ON clause references tables to its right");
    }

    @Override
    public void check() throws SQLException {
        LimboSchema s = state.getSchema();
        LimboTables targetTables = s.getRandomTableNonEmptyTables();
        List<LimboExpression> groupByColumns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new LimboColumnName(c, null)).collect(Collectors.toList());
        List<LimboColumn> columns = targetTables.getColumns();
        LimboExpressionGenerator gen = new LimboExpressionGenerator(state).setColumns(columns);
        LimboSelect select = new LimboSelect();
        select.setFetchColumns(groupByColumns);
        List<LimboTable> tables = targetTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<LimboExpression> from = LimboCommon.getTableRefs(tables, state.getSchema());
        select.setJoinClauses(joinStatements);
        select.setSelectType(SelectType.ALL);
        select.setFromList(from);
        // TODO order by?
        select.setGroupByClause(groupByColumns);
        select.setHavingClause(null);
        String originalQueryString = LimboVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        LimboExpression predicate = gen.getHavingClause();
        select.setHavingClause(predicate);
        String firstQueryString = LimboVisitor.asString(select);
        select.setHavingClause(new LimboUnaryOperation(UnaryOperator.NOT, predicate));
        String secondQueryString = LimboVisitor.asString(select);
        select.setHavingClause(new LimboPostfixUnaryOperation(PostfixUnaryOperator.ISNULL, predicate));
        String thirdQueryString = LimboVisitor.asString(select);
        String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
        if (combinedString.contains("EXIST")) {
            throw new IgnoreMeException();
        }
        List<String> secondResultSet = ComparatorHelper.getResultSetFirstColumnAsString(combinedString, errors, state);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(originalQueryString);
            state.getLogger().writeCurrent(combinedString);
        }
        if (new HashSet<>(resultSet).size() != new HashSet<>(secondResultSet).size()) {
            throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
        }
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }
}
