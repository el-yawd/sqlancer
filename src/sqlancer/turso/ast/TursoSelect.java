package sqlancer.turso.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.newast.Select;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoSelect extends TursoExpression
        implements Select<Join, TursoExpression, TursoTable, TursoColumn> {

    private SelectType fromOptions = SelectType.ALL;
    private List<TursoExpression> fromList = Collections.emptyList();
    private TursoExpression whereClause;
    private List<TursoExpression> groupByClause = Collections.emptyList();
    private TursoExpression limitClause;
    private List<TursoExpression> orderByClause = Collections.emptyList();
    private TursoExpression offsetClause;
    private List<TursoExpression> fetchColumns = Collections.emptyList();
    private List<Join> joinStatements = Collections.emptyList();
    private TursoExpression havingClause;
    private TursoWithClause withClause;

    public TursoSelect() {
    }

    public TursoSelect(TursoSelect other) {
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
    public List<TursoExpression> getFromList() {
        return fromList;
    }

    @Override
    public void setFromList(List<TursoExpression> fromList) {
        this.fromList = fromList;
    }

    @Override
    public TursoExpression getWhereClause() {
        return whereClause;
    }

    @Override
    public void setWhereClause(TursoExpression whereClause) {
        this.whereClause = whereClause;
    }

    @Override
    public void setGroupByClause(List<TursoExpression> groupByClause) {
        this.groupByClause = groupByClause;
    }

    @Override
    public List<TursoExpression> getGroupByClause() {
        return groupByClause;
    }

    @Override
    public void setLimitClause(TursoExpression limitClause) {
        this.limitClause = limitClause;
    }

    @Override
    public TursoExpression getLimitClause() {
        return limitClause;
    }

    @Override
    public List<TursoExpression> getOrderByClauses() {
        return orderByClause;
    }

    @Override
    public void setOrderByClauses(List<TursoExpression> orderBy) {
        this.orderByClause = orderBy;
    }

    @Override
    public void setOffsetClause(TursoExpression offsetClause) {
        this.offsetClause = offsetClause;
    }

    @Override
    public TursoExpression getOffsetClause() {
        return offsetClause;
    }

    @Override
    public void setFetchColumns(List<TursoExpression> fetchColumns) {
        this.fetchColumns = fetchColumns;
    }

    @Override
    public List<TursoExpression> getFetchColumns() {
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
    public TursoCollateSequence getExplicitCollateSequence() {
        // TODO implement?
        return null;
    }

    @Override
    public void setHavingClause(TursoExpression havingClause) {
        this.havingClause = havingClause;
    }

    @Override
    public TursoExpression getHavingClause() {
        assert orderByClause != null;
        return havingClause;
    }

    @Override
    public String asString() {
        return TursoVisitor.asString(this);
    }

    public void setWithClause(TursoWithClause withClause) {
        this.withClause = withClause;
    }

    public void updateWithClauseRight(TursoExpression withClauseRight) {
        this.withClause.updateRight(withClauseRight);
    }

    public TursoExpression getWithClause() {
        return this.withClause;
    }

    // This method is used in CODDTest to test subquery by replacing a table name
    // in the SELECT clause with a derived table expression.
    public void replaceFromTable(String tableName, TursoExpression newFromExpression) {
        int replaceIdx = -1;
        for (int i = 0; i < fromList.size(); ++i) {
            TursoExpression f = fromList.get(i);
            if (f instanceof TursoTableReference) {
                TursoTableReference tableRef = (TursoTableReference) f;
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
