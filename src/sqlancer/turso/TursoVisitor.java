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

public interface TursoVisitor {

    static byte[] hexStringToByteArray(String s) {
        byte[] b = new byte[s.length() / 2];
        for (int i = 0; i < b.length; i++) {
            int index = i * 2;
            int v = Integer.parseInt(s.substring(index, index + 2), 16);
            b[i] = (byte) v;
        }
        return b;
    }

    static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    // TODO remove these default methods

    default void visit(BinaryComparisonOperation op) {

    }

    default void visit(TursoBinaryOperation op) {

    }

    default void visit(TursoUnaryOperation exp) {

    }

    default void visit(TursoPostfixText op) {

    }

    default void visit(TursoPostfixUnaryOperation exp) {

    }

    void visit(BetweenOperation op);

    void visit(TursoColumnName c);

    void visit(TursoConstant c);

    void visit(Function f);

    void visit(TursoSelect s, boolean inner);

    void visit(TursoOrderingTerm term);

    void visit(TursoTableReference tableReference);

    void visit(TursoSetClause set);

    void visit(CollateOperation op);

    void visit(Cast cast);

    void visit(TypeLiteral literal);

    void visit(InOperation op);

    void visit(Subquery query);

    void visit(TursoExist exist);

    void visit(Join join);

    void visit(MatchOperation match);

    void visit(TursoFunction func);

    void visit(TursoText func);

    void visit(TursoDistinct distinct);

    void visit(TursoCaseWithoutBaseExpression casExpr);

    void visit(TursoCaseWithBaseExpression casExpr);

    void visit(TursoAggregate aggr);

    void visit(TursoWindowFunction func);

    void visit(TursoRowValueExpression rw);

    void visit(TursoWindowFunctionExpression windowFunction);

    void visit(TursoWindowFunctionFrameSpecTerm term);

    void visit(TursoWindowFunctionFrameSpecBetween between);

    void visit(TursoAlias alias);

    void visit(TursoWithClause withClause);

    void visit(TursoTableAndColumnRef tableAndColumnRef);

    void visit(TursoValues values);

    void visit(TursoExpressionBag expr);

    void visit(TursoTypeof expr);

    void visit(TursoResultMap tableSummary);

    default void visit(TursoExpression expr) {
        if (expr instanceof TursoBinaryOperation) {
            visit((TursoBinaryOperation) expr);
        } else if (expr instanceof TursoColumnName) {
            visit((TursoColumnName) expr);
        } else if (expr instanceof TursoConstant) {
            visit((TursoConstant) expr);
        } else if (expr instanceof TursoUnaryOperation) {
            visit((TursoUnaryOperation) expr);
        } else if (expr instanceof TursoPostfixUnaryOperation) {
            visit((TursoPostfixUnaryOperation) expr);
        } else if (expr instanceof Function) {
            visit((Function) expr);
        } else if (expr instanceof BetweenOperation) {
            visit((BetweenOperation) expr);
        } else if (expr instanceof CollateOperation) {
            visit((CollateOperation) expr);
        } else if (expr instanceof TursoOrderingTerm) {
            visit((TursoOrderingTerm) expr);
        } else if (expr instanceof TursoExpression.InOperation) {
            visit((InOperation) expr);
        } else if (expr instanceof Cast) {
            visit((Cast) expr);
        } else if (expr instanceof Subquery) {
            visit((Subquery) expr);
        } else if (expr instanceof Join) {
            visit((Join) expr);
        } else if (expr instanceof TursoSelect) {
            visit((TursoSelect) expr, true);
        } else if (expr instanceof TursoExist) {
            visit((TursoExist) expr);
        } else if (expr instanceof BinaryComparisonOperation) {
            visit((BinaryComparisonOperation) expr);
        } else if (expr instanceof TursoFunction) {
            visit((TursoFunction) expr);
        } else if (expr instanceof TursoDistinct) {
            visit((TursoDistinct) expr);
        } else if (expr instanceof TursoCaseWithoutBaseExpression) {
            visit((TursoCaseWithoutBaseExpression) expr);
        } else if (expr instanceof TursoCaseWithBaseExpression) {
            visit((TursoCaseWithBaseExpression) expr);
        } else if (expr instanceof TursoAggregate) {
            visit((TursoAggregate) expr);
        } else if (expr instanceof TursoPostfixText) {
            visit((TursoPostfixText) expr);
        } else if (expr instanceof TursoWindowFunction) {
            visit((TursoWindowFunction) expr);
        } else if (expr instanceof MatchOperation) {
            visit((MatchOperation) expr);
        } else if (expr instanceof TursoRowValueExpression) {
            visit((TursoRowValueExpression) expr);
        } else if (expr instanceof TursoText) {
            visit((TursoText) expr);
        } else if (expr instanceof TursoWindowFunctionExpression) {
            visit((TursoWindowFunctionExpression) expr);
        } else if (expr instanceof TursoWindowFunctionFrameSpecTerm) {
            visit((TursoWindowFunctionFrameSpecTerm) expr);
        } else if (expr instanceof TursoWindowFunctionFrameSpecBetween) {
            visit((TursoWindowFunctionFrameSpecBetween) expr);
        } else if (expr instanceof TursoTableReference) {
            visit((TursoTableReference) expr);
        } else if (expr instanceof TursoSetClause) {
            visit((TursoSetClause) expr);
        } else if (expr instanceof TursoAlias) {
            visit((TursoAlias) expr);
        } else if (expr instanceof TursoWithClause) {
            visit((TursoWithClause) expr);
        } else if (expr instanceof TursoTableAndColumnRef) {
            visit((TursoTableAndColumnRef) expr);
        } else if (expr instanceof TursoValues) {
            visit((TursoValues) expr);
        } else if (expr instanceof TursoExpressionBag) {
            visit((TursoExpressionBag) expr);
        } else if (expr instanceof TursoTypeof) {
            visit((TursoTypeof) expr);
        } else if (expr instanceof TursoResultMap) {
            visit((TursoResultMap) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(TursoExpression expr) {
        if (expr == null) {
            throw new AssertionError();
        }
        TursoToStringVisitor visitor = new TursoToStringVisitor();
        if (expr instanceof TursoSelect) {
            visitor.visit((TursoSelect) expr, false);
        } else {
            visitor.visit(expr);
        }
        return visitor.get();
    }

    static String asExpectedValues(TursoExpression expr) {
        TursoExpectedValueVisitor visitor = new TursoExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
