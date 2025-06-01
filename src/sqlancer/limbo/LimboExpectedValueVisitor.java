package sqlancer.limbo;

import sqlancer.limbo.ast.LimboAggregate;
import sqlancer.limbo.ast.LimboCase.CasePair;
import sqlancer.limbo.ast.LimboCase.LimboCaseWithBaseExpression;
import sqlancer.limbo.ast.LimboCase.LimboCaseWithoutBaseExpression;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.BetweenOperation;
import sqlancer.limbo.ast.LimboExpression.BinaryComparisonOperation;
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
import sqlancer.limbo.ast.LimboExpression.LimboPostfixText;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation;
import sqlancer.limbo.ast.LimboExpression.LimboResultMap;
import sqlancer.limbo.ast.LimboExpression.LimboTableAndColumnRef;
import sqlancer.limbo.ast.LimboExpression.LimboTableReference;
import sqlancer.limbo.ast.LimboExpression.LimboText;
import sqlancer.limbo.ast.LimboExpression.LimboTypeof;
import sqlancer.limbo.ast.LimboExpression.LimboValues;
import sqlancer.limbo.ast.LimboExpression.LimboWithClause;
import sqlancer.limbo.ast.LimboExpression.LimboBinaryOperation;
import sqlancer.limbo.ast.LimboExpression.Subquery;
import sqlancer.limbo.ast.LimboExpression.TypeLiteral;
import sqlancer.limbo.ast.LimboFunction;
import sqlancer.limbo.ast.LimboRowValueExpression;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboSetClause;
import sqlancer.limbo.ast.LimboUnaryOperation;
import sqlancer.limbo.ast.LimboWindowFunction;
import sqlancer.limbo.ast.LimboWindowFunctionExpression;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecBetween;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecTerm;

public class LimboExpectedValueVisitor implements LimboVisitor {

    private final StringBuilder sb = new StringBuilder();
    private int nrTabs;

    private void print(LimboExpression expr) {
        LimboToStringVisitor v = new LimboToStringVisitor();
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
    public void visit(LimboExpression expr) {
        nrTabs++;
        LimboVisitor.super.visit(expr);
        nrTabs--;
    }

    @Override
    public void visit(LimboBinaryOperation op) {
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
    public void visit(LimboColumnName c) {
        print(c);
    }

    @Override
    public void visit(LimboConstant c) {
        print(c);
    }

    @Override
    public void visit(Function f) {
        print(f);
        for (LimboExpression expr : f.getArguments()) {
            visit(expr);
        }
    }

    @Override
    public void visit(LimboSelect s, boolean inner) {
        for (LimboExpression expr : s.getFetchColumns()) {
            if (expr instanceof LimboAggregate) {
                visit(expr);
            }
        }
        for (LimboExpression expr : s.getJoinClauses()) {
            visit(expr);
        }
        visit(s.getWhereClause());
        if (s.getHavingClause() != null) {
            visit(s.getHavingClause());
        }
    }

    @Override
    public void visit(LimboOrderingTerm term) {
        sb.append("(");
        print(term);
        visit(term.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(LimboUnaryOperation exp) {
        print(exp);
        visit(exp.getExpression());
    }

    @Override
    public void visit(LimboPostfixUnaryOperation exp) {
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
            for (LimboExpression expr : op.getRightExpressionList()) {
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
    public void visit(LimboExist exist) {
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
    public void visit(LimboFunction func) {
        print(func);
        for (LimboExpression expr : func.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(LimboDistinct distinct) {
        print(distinct);
        visit(distinct.getExpression());
    }

    @Override
    public void visit(LimboCaseWithoutBaseExpression caseExpr) {
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
    public void visit(LimboCaseWithBaseExpression caseExpr) {
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
    public void visit(LimboAggregate aggr) {
        print(aggr);
        visit(aggr.getExpectedValue());
    }

    @Override
    public void visit(LimboPostfixText op) {
        print(op);
        if (op.getExpression() != null) {
            visit(op.getExpression());
        }
    }

    @Override
    public void visit(LimboWindowFunction func) {
        print(func);
        for (LimboExpression expr : func.getArgs()) {
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
    public void visit(LimboRowValueExpression rw) {
        print(rw);
        for (LimboExpression expr : rw.getExpressions()) {
            visit(expr);
        }
    }

    @Override
    public void visit(LimboText func) {
        print(func);
    }

    @Override
    public void visit(LimboWindowFunctionExpression windowFunction) {

    }

    @Override
    public void visit(LimboWindowFunctionFrameSpecTerm term) {

    }

    @Override
    public void visit(LimboWindowFunctionFrameSpecBetween between) {

    }

    @Override
    public void visit(LimboTableReference tableReference) {

    }

    @Override
    public void visit(LimboSetClause set) {
        print(set);
        visit(set.getLeft());
        visit(set.getRight());
    }

    @Override
    public void visit(LimboAlias alias) {
        print(alias);
        print(alias.getOriginalExpression());
        print(alias.getAliasExpression());
    }

    @Override
    public void visit(LimboWithClause withClause) {
        print(withClause);
        print(withClause.getLeft());
        print(withClause.getRight());
    }

    @Override
    public void visit(LimboTableAndColumnRef tableAndColumnRef) {
        print(tableAndColumnRef);
    }

    @Override
    public void visit(LimboValues values) {
        print(values);
    }

    @Override
    public void visit(LimboExpressionBag expr) {
        print(expr);
        print(expr.getInnerExpr());
    }

    @Override
    public void visit(LimboTypeof expr) {
        print(expr);
        print(expr.getInnerExpr());
    }

    @Override
    public void visit(LimboResultMap tableSummary) {
    }
}
