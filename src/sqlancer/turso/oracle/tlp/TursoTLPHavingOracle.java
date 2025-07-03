package sqlancer.turso.oracle.tlp;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoUnaryOperation;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.turso.ast.TursoSelect.SelectType;
import sqlancer.turso.ast.TursoUnaryOperation.UnaryOperator;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoTables;

public class TursoTLPHavingOracle implements TestOracle<TursoGlobalState> {

    private final TursoGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private String generatedQueryString;

    public TursoTLPHavingOracle(TursoGlobalState state) {
        this.state = state;
        TursoErrors.addExpectedExpressionErrors(errors);
        errors.add("no such column"); // FIXME why?
        errors.add("ON clause references tables to its right");
    }

    @Override
    public void check() throws SQLException {
        TursoSchema s = state.getSchema();
        TursoTables targetTables = s.getRandomTableNonEmptyTables();
        List<TursoExpression> groupByColumns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                .map(c -> new TursoColumnName(c, null)).collect(Collectors.toList());
        List<TursoColumn> columns = targetTables.getColumns();
        TursoExpressionGenerator gen = new TursoExpressionGenerator(state).setColumns(columns);
        TursoSelect select = new TursoSelect();
        select.setFetchColumns(groupByColumns);
        List<TursoTable> tables = targetTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<TursoExpression> from = TursoCommon.getTableRefs(tables, state.getSchema());
        select.setJoinClauses(joinStatements);
        select.setSelectType(SelectType.ALL);
        select.setFromList(from);
        // TODO order by?
        select.setGroupByClause(groupByColumns);
        select.setHavingClause(null);
        String originalQueryString = TursoVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        TursoExpression predicate = gen.getHavingClause();
        select.setHavingClause(predicate);
        String firstQueryString = TursoVisitor.asString(select);
        select.setHavingClause(new TursoUnaryOperation(UnaryOperator.NOT, predicate));
        String secondQueryString = TursoVisitor.asString(select);
        select.setHavingClause(new TursoPostfixUnaryOperation(PostfixUnaryOperator.ISNULL, predicate));
        String thirdQueryString = TursoVisitor.asString(select);
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
