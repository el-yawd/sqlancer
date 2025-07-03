package sqlancer.turso.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;

public class TursoTLPGroupByOracle extends TursoTLPBase {

    private String generatedQueryString;

    public TursoTLPGroupByOracle(TursoGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setGroupByClause(select.getFetchColumns());
        select.setWhereClause(null);
        String originalQueryString = TursoVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = TursoVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = TursoVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = TursoVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSetNoDuplicates(firstQueryString,
                secondQueryString, thirdQueryString, combinedString, true, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    List<TursoExpression> generateFetchColumns() {
        return Randomly.nonEmptySubset(targetTables.getColumns()).stream().map(c -> new TursoColumnName(c, null))
                .collect(Collectors.toList());
    }

    @Override
    public String getLastQueryString() {
        return generatedQueryString;
    }

}
