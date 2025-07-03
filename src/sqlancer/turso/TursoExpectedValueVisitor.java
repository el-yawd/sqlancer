package sqlancer.turso;

import sqlancer.turso.ast.TursoAggregate;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoFunction;
import sqlancer.turso.ast.TursoRowValueExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoSetClause;
import sqlancer.turso.ast.TursoUnaryOperation;
import sqlancer.turso.ast.TursoWindowFunction;
import sqlancer.turso.ast.TursoWindowFunctionExpression;
import sqlancer.turso.ast.TursoCase.CasePair;
import sqlancer.turso.ast.TursoCase.TursoCaseWithBaseExpression;
import sqlancer.turso.ast.TursoCase.TursoCaseWithoutBaseExpression;
import sqlancer.turso.ast.TursoExpression.BetweenOperation;
import sqlancer.turso.ast.TursoExpression.BinaryComparisonOperation;
import sqlancer.turso.ast.TursoExpression.Cast;
import sqlancer.turso.ast.TursoExpression.CollateOperation;
import sqlancer.turso.ast.TursoExpression.Function;
import sqlancer.turso.ast.TursoExpression.InOperation;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.MatchOperation;
import sqlancer.turso.ast.TursoExpression.Subquery;
import sqlancer.turso.ast.TursoExpression.TursoAlias;
import sqlancer.turso.ast.TursoExpression.TursoBinaryOperation;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoExpression.TursoDistinct;
import sqlancer.turso.ast.TursoExpression.TursoExist;
import sqlancer.turso.ast.TursoExpression.TursoExpressionBag;
import sqlancer.turso.ast.TursoExpression.TursoOrderingTerm;
import sqlancer.turso.ast.TursoExpression.TursoPostfixText;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation;
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

public class TursoExpectedValueVisitor implements TursoVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(TursoExpression expr) {
        TursoToStringVisitor v = new TursoToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < nrTabs; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append(" explicit collate: ");
        sb.append(expr.getExplicitCollateSequence());
        sb.append(" implicit collate: ");
        sb.append(expr.getImplicitCollateSequence());
        sb.append("\n");
    }

    @Override
    public void visit(TursoExpression expr) {
        nrTabs++;
        TursoVisitor.super.visit(expr);
        nrTabs--;
    }

    @Override
    public void visit(TursoBinaryOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(BetweenOperation op) {
        print(op);
        visit(op.getTopNode());
    }

    @Override
    public void visit(TursoColumnName c) {
        print(c);
    }

    @Override
    public void visit(TursoConstant c) {
        print(c);
    }

    @Override
    public void visit(Function f) {
        print(f);
        for (TursoExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(TursoSelect s, boolean inner) {
        for (TursoExpression expr : s.getFetchColumns()) {
            if (expr instanceof TursoAggregate) {
                visit(expr);
            }
        }
        for (TursoExpression expr : s.getJoinClauses()) {
            visit(expr);
        }
        visit(s.getWhereClause());
        if (s.getHavingClause() != null) {
            visit(s.getHavingClause());
        }
    }

    @Override
    public void visit(TursoOrderingTerm term) {
        sb.append("(");
        print(term);
        visit(term.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(TursoUnaryOperation exp) {
        print(exp);
        visit(exp.getExpression());
    }

    @Override
    public void visit(TursoPostfixUnaryOperation exp) {
        print(exp);
        visit(exp.getExpression());
    }

    @Override
    public void visit(CollateOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(Cast cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(TypeLiteral literal) {
    }

    @Override
    public void visit(InOperation op) {
        print(op);
        visit(op.getLeft());
        if (op.getRightExpressionList() != null) {
            for (TursoExpression expr : op.getRightExpressionList()) {
                visit(expr);
            }
        } else {
            visit(op.getRightSelect());
        }
    }

    @Override
    public void visit(Subquery query) {
        print(query);
        if (query.getExpectedValue() != null) {
            visit(query.getExpectedValue());
        }
    }

    @Override
    public void visit(TursoExist exist) {
        print(exist);
        visit(exist.getExpression());
    }

    @Override
    public void visit(Join join) {
        print(join);
        visit(join.getOnClause());
    }

    @Override
    public void visit(BinaryComparisonOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(TursoFunction func) {
        print(func);
        for (TursoExpression expr : func.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(TursoDistinct distinct) {
        print(distinct);
        visit(distinct.getExpression());
    }

    @Override
    public void visit(TursoCaseWithoutBaseExpression caseExpr) {
        for (CasePair cExpr : caseExpr.getPairs()) {
            print(cExpr.getCond());
            visit(cExpr.getCond());
            print(cExpr.getThen());
            visit(cExpr.getThen());
        }
        if (caseExpr.getElseExpr() != null) {
            print(caseExpr.getElseExpr());
            visit(caseExpr.getElseExpr());
        }
    }

    @Override
    public void visit(TursoCaseWithBaseExpression caseExpr) {
        print(caseExpr);
        visit(caseExpr.getBaseExpr());
        for (CasePair cExpr : caseExpr.getPairs()) {
            print(cExpr.getCond());
            visit(cExpr.getCond());
            print(cExpr.getThen());
            visit(cExpr.getThen());
        }
        if (caseExpr.getElseExpr() != null) {
            print(caseExpr.getElseExpr());
            visit(caseExpr.getElseExpr());
        }
    }

    @Override
    public void visit(TursoAggregate aggr) {
        print(aggr);
        visit(aggr.getExpectedValue());
    }

    @Override
    public void visit(TursoPostfixText op) {
        print(op);
        if (op.getExpression() != null) {
            visit(op.getExpression());
        }
    }

    @Override
    public void visit(TursoWindowFunction func) {
        print(func);
        for (TursoExpression expr : func.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(MatchOperation match) {
        print(match);
        visit(match.getLeft());
        visit(match.getRight());
    }

    @Override
    public void visit(TursoRowValueExpression rw) {
        print(rw);
        for (TursoExpression expr : rw.getExpressions()) {
            visit(expr);
        }
    }

    @Override
    public void visit(TursoText func) {
        print(func);
    }

    @Override
    public void visit(TursoWindowFunctionExpression windowFunction) {

    }

    @Override
    public void visit(TursoWindowFunctionFrameSpecTerm term) {

    }

    @Override
    public void visit(TursoWindowFunctionFrameSpecBetween between) {

    }

    @Override
    public void visit(TursoTableReference tableReference) {

    }

    @Override
    public void visit(TursoSetClause set) {
        print(set);
        visit(set.getLeft());
        visit(set.getRight());
    }

    @Override
    public void visit(TursoAlias alias) {
        print(alias);
        print(alias.getOriginalExpression());
        print(alias.getAliasExpression());
    }

    @Override
    public void visit(TursoWithClause withClause) {
        print(withClause);
        print(withClause.getLeft());
        print(withClause.getRight());
    }

    @Override
    public void visit(TursoTableAndColumnRef tableAndColumnRef) {
        print(tableAndColumnRef);
    }

    @Override
    public void visit(TursoValues values) {
        print(values);
    }

    @Override
    public void visit(TursoExpressionBag expr) {
        print(expr);
        print(expr.getInnerExpr());
    }

    @Override
    public void visit(TursoTypeof expr) {
        print(expr);
        print(expr.getInnerExpr());
    }

    @Override
    public void visit(TursoResultMap tableSummary) {
    }
}
