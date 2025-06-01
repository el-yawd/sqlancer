package sqlancer.limbo.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.newast.Select;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public class LimboSelect extends LimboExpression
        implements Select<Join, LimboExpression, LimboTable, LimboColumn> {

    private SelectType fromOptions = SelectType.ALL;
    private List<LimboExpression> fromList = Collections.emptyList();
    private LimboExpression whereClause;
    private List<LimboExpression> groupByClause = Collections.emptyList();
    private LimboExpression limitClause;
    private List<LimboExpression> orderByClause = Collections.emptyList();
    private LimboExpression offsetClause;
    private List<LimboExpression> fetchColumns = Collections.emptyList();
    private List<Join> joinStatements = Collections.emptyList();
    private LimboExpression havingClause;
    private LimboWithClause withClause;

    public LimboSelect() {
    }

    public LimboSelect(LimboSelect other) {
        fromOptions = other.fromOptions;
        fromList = new ArrayList<>(other.fromList);
        whereClause = other.whereClause;
        groupByClause = other.groupByClause;
        limitClause = other.limitClause;
        orderByClause = new ArrayList<>(other.orderByClause);
        offsetClause = other.offsetClause;
        fetchColumns = new ArrayList<>(other.fetchColumns);
        joinStatements = new ArrayList<>();
        for (Join j : other.joinStatements) {
            joinStatements.add(new Join(j));
        }
        havingClause = other.havingClause;
        withClause = other.withClause;
    }

    public enum SelectType {
        DISTINCT, ALL;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    @Override
    public List<LimboExpression> getFromList() {
        return fromList;
    }

    @Override
    public void setFromList(List<LimboExpression> fromList) {
        this.fromList = fromList;
    }

    @Override
    public LimboExpression getWhereClause() {
        return whereClause;
    }

    @Override
    public void setWhereClause(LimboExpression whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public void setGroupByClause(List<LimboExpression> groupByClause) {
        this.groupByClause = groupByClause;
    }

    @Override
    public List<LimboExpression> getGroupByClause() {
        return groupByClause;
    }

    @Override
    public void setLimitClause(LimboExpression limitClause) {
        this.limitClause = limitClause;
    }

    @Override
    public LimboExpression getLimitClause() {
        return limitClause;
    }

    @Override
    public List<LimboExpression> getOrderByClauses() {
        return orderByClause;
    }

    @Override
    public void setOrderByClauses(List<LimboExpression> orderBy) {
        this.orderByClause = orderBy;
    }

    @Override
    public void setOffsetClause(LimboExpression offsetClause) {
        this.offsetClause = offsetClause;
    }

    @Override
    public LimboExpression getOffsetClause() {
        return offsetClause;
    }

    @Override
    public void setFetchColumns(List<LimboExpression> fetchColumns) {
        this.fetchColumns = fetchColumns;
    }

    @Override
    public List<LimboExpression> getFetchColumns() {
        return fetchColumns;
    }

    @Override
    public void setJoinClauses(List<Join> joinStatements) {
        this.joinStatements = joinStatements;
    }

    @Override
    public List<Join> getJoinClauses() {
        return joinStatements;
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        // TODO implement?
        return null;
    }

    @Override
    public void setHavingClause(LimboExpression havingClause) {
        this.havingClause = havingClause;
    }

    @Override
    public LimboExpression getHavingClause() {
        assert orderByClause != null;
        return havingClause;
    }

    @Override
    public String asString() {
        return LimboVisitor.asString(this);
    }

    public void setWithClause(LimboWithClause withClause) {
        this.withClause = withClause;
    }

    public void updateWithClauseRight(LimboExpression withClauseRight) {
        this.withClause.updateRight(withClauseRight);
    }

    public LimboExpression getWithClause() {
        return this.withClause;
    }

    // This method is used in CODDTest to test subquery by replacing a table name
    // in the SELECT clause with a derived table expression.
    public void replaceFromTable(String tableName, LimboExpression newFromExpression) {
        int replaceIdx = -1;
        for (int i = 0; i < fromList.size(); ++i) {
            LimboExpression f = fromList.get(i);
            if (f instanceof LimboTableReference) {
                LimboTableReference tableRef = (LimboTableReference) f;
                if (tableRef.getTable().getName().equals(tableName)) {
                    replaceIdx = i;
                }
            }
        }
        if (replaceIdx == -1) {
            throw new IgnoreMeException();
        }
        fromList.set(replaceIdx, newFromExpression);
    }
}
