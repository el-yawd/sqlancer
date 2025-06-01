package sqlancer.limbo.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboSelect.SelectType;

public class LimboTLPDistinctOracle extends LimboTLPBase {

    private String generatedQueryString;

    public LimboTLPDistinctOracle(LimboGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        select.setSelectType(SelectType.DISTINCT);
        select.setWhereClause(null);
        String originalQueryString = LimboVisitor.asString(select);
        generatedQueryString = originalQueryString;
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        select.setWhereClause(predicate);
        String firstQueryString = LimboVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = LimboVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = LimboVisitor.asString(select);
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
