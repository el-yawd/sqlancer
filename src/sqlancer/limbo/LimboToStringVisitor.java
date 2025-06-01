package sqlancer.limbo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.visitor.ToStringVisitor;
import sqlancer.limbo.ast.LimboAggregate;
import sqlancer.limbo.ast.LimboAggregate.LimboAggregateFunction;
import sqlancer.limbo.ast.LimboCase.CasePair;
import sqlancer.limbo.ast.LimboCase.LimboCaseWithBaseExpression;
import sqlancer.limbo.ast.LimboCase.LimboCaseWithoutBaseExpression;
import sqlancer.limbo.ast.LimboCast;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboConstant.LimboNullConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.BetweenOperation;
import sqlancer.limbo.ast.LimboExpression.Cast;
import sqlancer.limbo.ast.LimboExpression.CollateOperation;
import sqlancer.limbo.ast.LimboExpression.Function;
import sqlancer.limbo.ast.LimboExpression.InOperation;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.ast.LimboExpression.MatchOperation;
import sqlancer.limbo.ast.LimboExpression.LimboAlias;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboExpression.LimboDistinct;
import sqlancer.limbo.ast.LimboExpression.LimboExist;
import sqlancer.limbo.ast.LimboExpression.LimboExpressionBag;
import sqlancer.limbo.ast.LimboExpression.LimboOrderingTerm;
import sqlancer.limbo.ast.LimboExpression.LimboResultMap;
import sqlancer.limbo.ast.LimboExpression.LimboTableAndColumnRef;
import sqlancer.limbo.ast.LimboExpression.LimboTableReference;
import sqlancer.limbo.ast.LimboExpression.LimboText;
import sqlancer.limbo.ast.LimboExpression.LimboTypeof;
import sqlancer.limbo.ast.LimboExpression.LimboValues;
import sqlancer.limbo.ast.LimboExpression.LimboWithClause;
import sqlancer.limbo.ast.LimboExpression.Subquery;
import sqlancer.limbo.ast.LimboExpression.TypeLiteral;
import sqlancer.limbo.ast.LimboFunction;
import sqlancer.limbo.ast.LimboRowValueExpression;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboSetClause;
import sqlancer.limbo.ast.LimboWindowFunction;
import sqlancer.limbo.ast.LimboWindowFunctionExpression;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecBetween;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecTerm;
import sqlancer.limbo.schema.LimboDataType;

public class LimboToStringVisitor extends ToStringVisitor<LimboExpression> implements LimboVisitor {

    public boolean fullyQualifiedNames = true;

    @Override
    public void visitSpecific(LimboExpression expr) {
        LimboVisitor.super.visit(expr);
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
    public void visit(LimboColumnName c) {
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
    public void visit(LimboSelect s, boolean inner) {
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
            if (s.getFromList().get(i) instanceof LimboSelect) {
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
            LimboExpression whereClause = s.getWhereClause();
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
    public void visit(LimboConstant c) {
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
                    arr = c.asString().getBytes(LimboCast.DEFAULT_ENCODING);
                }
                sb.append(LimboVisitor.byteArrayToHex(arr));
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
    public void visit(LimboOrderingTerm term) {
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
            if (op.getRightSelect() instanceof LimboExpression.LimboTableReference) {
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
    public void visit(LimboExist exist) {
        if (exist.getNegated()) {
            sb.append(" NOT");
        }
        sb.append(" EXISTS ");
        if (exist.getExpression() instanceof LimboSetClause) {
            sb.append("(");
        }
        visit(exist.getExpression());
        if (exist.getExpression() instanceof LimboSetClause) {
            sb.append(")");
        }
        sb.append("");
    }

    @Override
    public void visit(LimboAggregate aggr) {
        if (aggr.getFunc() == LimboAggregateFunction.COUNT_ALL) {
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
    public void visit(LimboFunction func) {
        sb.append(func.getFunc());
        sb.append("(");
        visit(func.getArgs());
        sb.append(")");
    }

    @Override
    public void visit(LimboDistinct distinct) {
        sb.append("DISTINCT ");
        visit(distinct.getExpression());
    }

    @Override
    public void visit(LimboCaseWithoutBaseExpression casExpr) {
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
    public void visit(LimboCaseWithBaseExpression casExpr) {
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
    public void visit(LimboWindowFunction func) {
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
    public void visit(LimboRowValueExpression rw) {
        sb.append("(");
        visit(rw.getExpressions());
        sb.append(")");
    }

    @Override
    public void visit(LimboText func) {
        sb.append(func.getText());
    }

    @Override
    public void visit(LimboWindowFunctionExpression windowFunction) {
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
    public void visit(LimboWindowFunctionFrameSpecTerm term) {
        if (term.getExpression() != null) {
            visit(term.getExpression());
        }
        sb.append(" ");
        sb.append(term.getKind().getString());
    }

    @Override
    public void visit(LimboWindowFunctionFrameSpecBetween between) {
        sb.append("BETWEEN ");
        visit(between.getLeft());
        sb.append(" AND ");
        visit(between.getRight());
    }

    @Override
    public void visit(LimboTableReference tableReference) {
        sb.append(tableReference.getTable().getName());
        if (tableReference.getIndexedBy() == null) {
            if (Randomly.getBooleanWithSmallProbability()) {
                sb.append(" NOT INDEXED");
            }
        } else {
            sb.append(" INDEXED BY ");
            sb.append(tableReference.getIndexedBy());
        }
    }

    private void visit(LimboExpression... expressions) {
        visit(Arrays.asList(expressions));
    }

    @Override
    public void visit(LimboSetClause set) {
        // do not print parentheses
        sb.append(LimboVisitor.asString(set.getLeft()));
        sb.append(" ");
        sb.append(set.getType().getTextRepresentation());
        sb.append(" ");
        sb.append(LimboVisitor.asString(set.getRight()));
    }

    @Override
    public void visit(LimboAlias alias) {
        sb.append("(");
        visit(alias.getOriginalExpression());
        sb.append(")");
        sb.append(" AS ");
        visit(alias.getAliasExpression());
    }

    @Override
    public void visit(LimboWithClause withClause) {
        sb.append("WITH ");
        visit(withClause.getLeft());
        sb.append(" AS ");
        visit(withClause.getRight());
    }

    @Override
    public void visit(LimboTableAndColumnRef tableAndColumnRef) {
        sb.append(tableAndColumnRef.getString());
    }

    @Override
    public void visit(LimboValues values) {
        Map<String, List<LimboConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        List<String> columnNames = values.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList());
        sb.append("(VALUES ");
        for (int i = 0; i < size; i++) {
            sb.append("(");
            Boolean isFirstColumn = true;
            for (String name : columnNames) {
                if (!isFirstColumn) {
                    sb.append(", ");
                }
                if (vs.get(name).get(i).getDataType() == LimboDataType.NULL) {
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
    public void visit(LimboExpressionBag expr) {
        visit(expr.getInnerExpr());
    }

    @Override
    public void visit(LimboTypeof expr) {
        sb.append("typeof(");
        visit(expr.getInnerExpr());
        sb.append(")");
    }

    @Override
    public void visit(LimboResultMap tableSummary) {
        // We use the CASE WHEN THEN END expression to represent the result of an expression for each row in the table.
        LimboValues values = tableSummary.getValues();
        List<LimboColumnName> columnRefs = tableSummary.getColumns();
        List<LimboConstant> summary = tableSummary.getSummary();

        Map<String, List<LimboConstant>> vs = values.getValues();
        int size = vs.get(vs.keySet().iterator().next()).size();
        if (size == 0) {
            throw new AssertionError("The result of the expression must not be empty.");
        }
        List<String> columnNames = values.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList());
        sb.append(" CASE ");
        for (int i = 0; i < size; i++) {
            sb.append("WHEN ");
            for (int j = 0; j < columnNames.size(); ++j) {
                visit(columnRefs.get(j));
                if (vs.get(columnNames.get(j)).get(i) instanceof LimboNullConstant) {
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
