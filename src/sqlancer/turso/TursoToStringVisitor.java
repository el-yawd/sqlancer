package sqlancer.turso;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import sqlancer.Randomly;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.turso.ast.TursoAggregate;
import sqlancer.turso.ast.TursoCast;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoFunction;
import sqlancer.turso.ast.TursoRowValueExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoSetClause;
import sqlancer.turso.ast.TursoWindowFunction;
import sqlancer.turso.ast.TursoWindowFunctionExpression;
import sqlancer.turso.ast.TursoAggregate.TursoAggregateFunction;
import sqlancer.turso.ast.TursoCase.CasePair;
import sqlancer.turso.ast.TursoCase.TursoCaseWithBaseExpression;
import sqlancer.turso.ast.TursoCase.TursoCaseWithoutBaseExpression;
import sqlancer.turso.ast.TursoConstant.TursoNullConstant;
import sqlancer.turso.ast.TursoExpression.BetweenOperation;
import sqlancer.turso.ast.TursoExpression.Cast;
import sqlancer.turso.ast.TursoExpression.CollateOperation;
import sqlancer.turso.ast.TursoExpression.Function;
import sqlancer.turso.ast.TursoExpression.InOperation;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.MatchOperation;
import sqlancer.turso.ast.TursoExpression.Subquery;
import sqlancer.turso.ast.TursoExpression.TursoAlias;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoExpression.TursoDistinct;
import sqlancer.turso.ast.TursoExpression.TursoExist;
import sqlancer.turso.ast.TursoExpression.TursoExpressionBag;
import sqlancer.turso.ast.TursoExpression.TursoOrderingTerm;
import sqlancer.turso.ast.TursoExpression.TursoResultMap;
import sqlancer.turso.ast.TursoExpression.TursoTableAndColumnRef;
import sqlancer.turso.ast.TursoExpression.TursoTableReference;
import sqlancer.turso.ast.TursoExpression.TursoText;
import sqlancer.turso.ast.TursoExpression.TursoTypeof;
import sqlancer.turso.ast.TursoExpression.TursoValues;
import sqlancer.turso.ast.TursoExpression.TursoWithClause;
import sqlancer.turso.ast.TursoExpression.TypeLiteral;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoWindowFunctionFrameSpecBetween;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoWindowFunctionFrameSpecTerm;
import sqlancer.turso.schema.TursoDataType;

public class TursoToStringVisitor
    extends ToStringVisitor<TursoExpression>
    implements TursoVisitor {

    public boolean fullyQualifiedNames = true;

    @Override
    public void visitSpecific(TursoExpression expr) {
        TursoVisitor.super.visit(expr);
    }

    protected void asHexString(long intVal) {
        String hexVal = Long.toHexString(intVal);
        String prefix;
        if (Randomly.getBoolean()) {
            prefix = "0x";
        } else {
            prefix = "0X";
        }
        sb.append(prefix);
        sb.append(hexVal);
    }

    @Override
    public void visit(BetweenOperation op) {
        sb.append("(");
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        if (op.isNegated()) {
            sb.append(" NOT");
        }
        sb.append(" BETWEEN ");
        sb.append("(");
        visit(op.getLeft());
        sb.append(")");
        sb.append(" AND ");
        sb.append("(");
        visit(op.getRight());
        sb.append(")");
        sb.append(")");
    }

    @Override
    public void visit(TursoColumnName c) {
        if (fullyQualifiedNames && c.getColumn().getTable() != null) {
            sb.append(c.getColumn().getTable().getName());
            sb.append('.');
        }
        sb.append(c.getColumn().getName());
    }

    @Override
    public void visit(Function f) {
        sb.append(f.getName());
        sb.append("(");
        visit(f.getArguments());
        sb.append(")");
    }

    @Override
    public void visit(TursoSelect s, boolean inner) {
        if (inner) {
            sb.append("(");
        }
        if (s.getWithClause() != null) {
            visit(s.getWithClause());
            sb.append(" ");
        }
        sb.append("SELECT ");
        switch (s.getFromOptions()) {
            case DISTINCT:
                sb.append("DISTINCT ");
                break;
            case ALL:
                sb.append(Randomly.fromOptions("ALL ", ""));
                break;
            default:
                throw new AssertionError(s.getFromOptions());
        }
        if (s.getFetchColumns() == null) {
            sb.append("*");
        } else {
            visit(s.getFetchColumns());
        }
        sb.append(" FROM ");
        for (int i = 0; i < s.getFromList().size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            if (s.getFromList().get(i) instanceof TursoSelect) {
                sb.append("(");
                // TODO: fix this workaround
                visit(s.getFromList().get(i));
                sb.append(")");
            } else {
                visit(s.getFromList().get(i));
            }
        }
        for (Join j : s.getJoinClauses()) {
            visit(j);
        }

        if (s.getWhereClause() != null) {
            TursoExpression whereClause = s.getWhereClause();
            sb.append(" WHERE (");
            visit(whereClause);
            sb.append(")");
        }
        if (!s.getGroupByClause().isEmpty()) {
            sb.append(" ");
            sb.append("GROUP BY ");
            visit(s.getGroupByClause());
        }
        if (s.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(s.getHavingClause());
        }
        if (!s.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(s.getOrderByClauses());
        }
        if (s.getLimitClause() != null) {
            sb.append(" LIMIT ");
            visit(s.getLimitClause());
        }

        if (s.getOffsetClause() != null) {
            sb.append(" OFFSET ");
            visit(s.getOffsetClause());
        }
        if (inner) {
            sb.append(")");
        }
    }

    @Override
    public void visit(TursoConstant c) {
        if (c.isNull()) {
            sb.append("NULL");
        } else {
            switch (c.getDataType()) {
                case INT:
                    // if ((c.asInt() == 0 || c.asInt() == 1) && Randomly.getBoolean()) {
                    // sb.append(c.asInt() == 1 ? "TRUE" : "FALSE");
                    // } else {
                    // - 0X8000000000000000 results in an error message otherwise
                    if (!c.isHex() || c.asInt() == Long.MIN_VALUE) {
                        sb.append(c.asInt());
                    } else {
                        long intVal = c.asInt();
                        asHexString(intVal);
                    }
                    // }
                    break;
                case REAL:
                    double asDouble = c.asDouble();
                    if (Double.POSITIVE_INFINITY == asDouble) {
                        sb.append("1e500");
                    } else if (Double.NEGATIVE_INFINITY == asDouble) {
                        sb.append("-1e500");
                    } else if (Double.isNaN(asDouble)) {
                        // throw new IgnoreMeException();
                        sb.append("1e500 / 1e500");
                    } else {
                        sb.append(asDouble);
                    }
                    break;
                case TEXT:
                    sb.append("'");
                    sb.append(c.asString().replace("'", "''"));
                    sb.append("'");
                    break;
                case BINARY:
                    sb.append('x');
                    sb.append("'");
                    byte[] arr;
                    if (c.getValue() instanceof byte[]) {
                        arr = c.asBinary();
                    } else {
                        arr = c.asString().getBytes(TursoCast.DEFAULT_ENCODING);
                    }
                    sb.append(TursoVisitor.byteArrayToHex(arr));
                    sb.append("'");
                    break;
                default:
                    throw new AssertionError(c.getDataType());
            }
        }
    }

    @Override
    public void visit(Join join) {
        sb.append(" ");
        switch (join.getType()) {
            case CROSS:
                sb.append("CROSS");
                break;
            case INNER:
                sb.append("INNER");
                break;
            case NATURAL:
                sb.append("NATURAL");
                break;
            case OUTER:
                sb.append("LEFT OUTER");
                break;
            case RIGHT:
                sb.append("RIGHT OUTER");
                break;
            case FULL:
                sb.append("FULL OUTER");
                break;
            default:
                throw new AssertionError(join.getType());
        }
        sb.append(" JOIN ");
        sb.append(join.getTable().getName());
        if (join.getOnClause() != null) {
            sb.append(" ON ");
            visit(join.getOnClause());
        }
    }

    @Override
    public void visit(TursoOrderingTerm term) {
        visit(term.getExpression());
        // TODO make order optional?
        sb.append(" ");
        sb.append(term.getOrdering().toString());
    }

    @Override
    public void visit(CollateOperation op) {
        visit(op.getExpression());
        sb.append(" COLLATE ");
        sb.append(op.getCollate());
    }

    @Override
    public void visit(Cast cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        visit(cast.getType());
        sb.append(")");
    }

    @Override
    public void visit(TypeLiteral literal) {
        sb.append(literal.getType());
    }

    @Override
    public void visit(InOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(" IN ");
        if (op.getRightExpressionList() != null) {
            sb.append("(");
            visit(op.getRightExpressionList());
            sb.append(")");
        } else {
            if (
                op.getRightSelect() instanceof
                TursoExpression.TursoTableReference
            ) {
                visit(op.getRightSelect());
            } else {
                sb.append("(");
                visit(op.getRightSelect());
                sb.append(")");
            }
        }

        sb.append(")");
    }

    @Override
    public void visit(Subquery query) {
        sb.append(query.getQuery());
    }

    @Override
    public void visit(TursoExist exist) {
        if (exist.getNegated()) {
            sb.append(" NOT");
        }
        sb.append(" EXISTS ");
        if (exist.getExpression() instanceof TursoSetClause) {
            sb.append("(");
        }
        visit(exist.getExpression());
        if (exist.getExpression() instanceof TursoSetClause) {
            sb.append(")");
        }
        sb.append("");
    }

    @Override
    public void visit(TursoAggregate aggr) {
        if (aggr.getFunc() == TursoAggregateFunction.COUNT_ALL) {
            sb.append("COUNT(*)");
        } else {
            sb.append(aggr.getFunc());
            sb.append("(");
            visit(aggr.getExpr());
            sb.append(")");
        }
    }

    @Override
    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(TursoFunction func) {
        sb.append(func.getFunc());
        sb.append("(");
        visit(func.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(TursoDistinct distinct) {
        sb.append("DISTINCT ");
        visit(distinct.getExpression());
    }

    @Override
    public void visit(TursoCaseWithoutBaseExpression casExpr) {
        sb.append("CASE");
        for (CasePair pair : casExpr.getPairs()) {
            sb.append(" WHEN ");
            visit(pair.getCond());
            sb.append(" THEN ");
            visit(pair.getThen());
        }
        if (casExpr.getElseExpr() != null) {
            sb.append(" ELSE ");
            visit(casExpr.getElseExpr());
        }
        sb.append(" END");
    }

    @Override
    public void visit(TursoCaseWithBaseExpression casExpr) {
        sb.append("CASE ");
        visit(casExpr.getBaseExpr());
        sb.append(" ");
        for (CasePair pair : casExpr.getPairs()) {
            sb.append(" WHEN ");
            visit(pair.getCond());
            sb.append(" THEN ");
            visit(pair.getThen());
        }
        if (casExpr.getElseExpr() != null) {
            sb.append(" ELSE ");
            visit(casExpr.getElseExpr());
        }
        sb.append(" END");
    }

    @Override
    public void visit(TursoWindowFunction func) {
        sb.append(func.getFunc());
        sb.append("(");
        visit(func.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(MatchOperation match) {
        visit(match.getLeft());
        sb.append(" MATCH ");
        visit(match.getRight());
    }

    @Override
    public void visit(TursoRowValueExpression rw) {
        sb.append("(");
        visit(rw.getExpressions());
        sb.append(")");
    }

    @Override
    public void visit(TursoText func) {
        sb.append(func.getText());
    }

    @Override
    public void visit(TursoWindowFunctionExpression windowFunction) {
        visit(windowFunction.getBaseWindowFunction());
        if (windowFunction.getFilterClause() != null) {
            sb.append(" FILTER(WHERE ");
            visit(windowFunction.getFilterClause());
            sb.append(")");
        }
        sb.append(" OVER (");
        if (!windowFunction.getPartitionBy().isEmpty()) {
            sb.append(" PARTITION BY ");
            visit(windowFunction.getPartitionBy());
        }
        if (!windowFunction.getOrderBy().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(windowFunction.getOrderBy());
        }
        if (windowFunction.getFrameSpec() != null) {
            sb.append(" ");
            sb.append(windowFunction.getFrameSpecKind());
            sb.append(" ");
            visit(windowFunction.getFrameSpec());
            if (windowFunction.getExclude() != null) {
                sb.append(" ");
                sb.append(windowFunction.getExclude().getString());
            }
        }
        sb.append(")");
    }

    @Override
    public void visit(TursoWindowFunctionFrameSpecTerm term) {
        if (term.getExpression() != null) {
            visit(term.getExpression());
        }
        sb.append(" ");
        sb.append(term.getKind().getString());
    }

    @Override
    public void visit(TursoWindowFunctionFrameSpecBetween between) {
        sb.append("BETWEEN ");
        visit(between.getLeft());
        sb.append(" AND ");
        visit(between.getRight());
    }

    @Override
    public void visit(TursoTableReference tableReference) {
        sb.append(tableReference.getTable().getName());
        // if (tableReference.getIndexedBy() == null) {
        //     if (Randomly.getBooleanWithSmallProbability()) {
        //         sb.append(" NOT INDEXED");
        //     }
        // } else {
        //     sb.append(" INDEXED BY ");
        //     sb.append(tableReference.getIndexedBy());
        // }
    }

    private void visit(TursoExpression... expressions) {
        visit(Arrays.asList(expressions));
    }

    @Override
    public void visit(TursoSetClause set) {
        // do not print parentheses
        sb.append(TursoVisitor.asString(set.getLeft()));
        sb.append(" ");
        sb.append(set.getType().getTextRepresentation());
        sb.append(" ");
        sb.append(TursoVisitor.asString(set.getRight()));
    }

    @Override
    public void visit(TursoAlias alias) {
        sb.append("(");
        visit(alias.getOriginalExpression());
        sb.append(")");
        sb.append(" AS ");
        visit(alias.getAliasExpression());
    }

    @Override
    public void visit(TursoWithClause withClause) {
        sb.append("WITH ");
        visit(withClause.getLeft());
        sb.append(" AS ");
        visit(withClause.getRight());
    }

    @Override
    public void visit(TursoTableAndColumnRef tableAndColumnRef) {
        sb.append(tableAndColumnRef.getString());
    }

    @Override
    public void visit(TursoValues values) {
        Map<String, List<TursoConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        List<String> columnNames = values
            .getColumns()
            .stream()
            .map(c -> c.getName())
            .collect(Collectors.toList());
        sb.append("(VALUES ");
        for (int i = 0; i < size; i++) {
            sb.append("(");
            Boolean isFirstColumn = true;
            for (String name : columnNames) {
                if (!isFirstColumn) {
                    sb.append(", ");
                }
                if (vs.get(name).get(i).getDataType() == TursoDataType.NULL) {
                    visit(vs.get(name).get(i));
                } else {
                    sb.append("(CAST(");
                    visit(vs.get(name).get(i));
                    sb.append(" AS ");
                    sb.append(vs.get(name).get(i).getDataType().toString());
                    sb.append("))");
                }
                isFirstColumn = false;
            }
            sb.append(")");
            if (i < size - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
    }

    @Override
    public void visit(TursoExpressionBag expr) {
        visit(expr.getInnerExpr());
    }

    @Override
    public void visit(TursoTypeof expr) {
        sb.append("typeof(");
        visit(expr.getInnerExpr());
        sb.append(")");
    }

    @Override
    public void visit(TursoResultMap tableSummary) {
        // We use the CASE WHEN THEN END expression to represent the result of an expression for each row in the table.
        TursoValues values = tableSummary.getValues();
        List<TursoColumnName> columnRefs = tableSummary.getColumns();
        List<TursoConstant> summary = tableSummary.getSummary();

        Map<String, List<TursoConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        if (size == 0) {
            throw new AssertionError(
                "The result of the expression must not be empty."
            );
        }
        List<String> columnNames = values
            .getColumns()
            .stream()
            .map(c -> c.getName())
            .collect(Collectors.toList());
        sb.append(" CASE ");
        for (int i = 0; i < size; i++) {
            sb.append("WHEN ");
            for (int j = 0; j < columnNames.size(); ++j) {
                visit(columnRefs.get(j));
                if (
                    vs.get(columnNames.get(j)).get(i) instanceof
                    TursoNullConstant
                ) {
                    sb.append(" IS NULL");
                } else {
                    sb.append(" = ");
                    sb.append(vs.get(columnNames.get(j)).get(i).toString());
                }
                if (j < columnNames.size() - 1) {
                    sb.append(" AND ");
                }
            }
            sb.append(" THEN ");
            visit(summary.get(i));
            sb.append(" ");
        }
        sb.append("END ");
    }
}
