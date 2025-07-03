package sqlancer.turso.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoSelect.SelectType;

public class TursoTLPDistinctOracle extends TursoTLPBase {

    private String generatedQueryString;

    public TursoTLPDistinctOracle(TursoGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setSelectType(SelectType.DISTINCT);
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
    public String getLastQueryString() {
        return generatedQueryString;
    }

}
