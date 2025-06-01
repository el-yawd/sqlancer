package sqlancer.limbo;

import sqlancer.limbo.ast.LimboAggregate;
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

public interface LimboVisitor {

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

    default void visit(LimboBinaryOperation op) {

    }

    default void visit(LimboUnaryOperation exp) {

    }

    default void visit(LimboPostfixText op) {

    }

    default void visit(LimboPostfixUnaryOperation exp) {

    }

    void visit(BetweenOperation op);

    void visit(LimboColumnName c);

    void visit(LimboConstant c);

    void visit(Function f);

    void visit(LimboSelect s, boolean inner);

    void visit(LimboOrderingTerm term);

    void visit(LimboTableReference tableReference);

    void visit(LimboSetClause set);

    void visit(CollateOperation op);

    void visit(Cast cast);

    void visit(TypeLiteral literal);

    void visit(InOperation op);

    void visit(Subquery query);

    void visit(LimboExist exist);

    void visit(Join join);

    void visit(MatchOperation match);

    void visit(LimboFunction func);

    void visit(LimboText func);

    void visit(LimboDistinct distinct);

    void visit(LimboCaseWithoutBaseExpression casExpr);

    void visit(LimboCaseWithBaseExpression casExpr);

    void visit(LimboAggregate aggr);

    void visit(LimboWindowFunction func);

    void visit(LimboRowValueExpression rw);

    void visit(LimboWindowFunctionExpression windowFunction);

    void visit(LimboWindowFunctionFrameSpecTerm term);

    void visit(LimboWindowFunctionFrameSpecBetween between);

    void visit(LimboAlias alias);

    void visit(LimboWithClause withClause);

    void visit(LimboTableAndColumnRef tableAndColumnRef);

    void visit(LimboValues values);

    void visit(LimboExpressionBag expr);

    void visit(LimboTypeof expr);

    void visit(LimboResultMap tableSummary);

    default void visit(LimboExpression expr) {
        if (expr instanceof LimboBinaryOperation) {
            visit((LimboBinaryOperation) expr);
        } else if (expr instanceof LimboColumnName) {
            visit((LimboColumnName) expr);
        } else if (expr instanceof LimboConstant) {
            visit((LimboConstant) expr);
        } else if (expr instanceof LimboUnaryOperation) {
            visit((LimboUnaryOperation) expr);
        } else if (expr instanceof LimboPostfixUnaryOperation) {
            visit((LimboPostfixUnaryOperation) expr);
        } else if (expr instanceof Function) {
            visit((Function) expr);
        } else if (expr instanceof BetweenOperation) {
            visit((BetweenOperation) expr);
        } else if (expr instanceof CollateOperation) {
            visit((CollateOperation) expr);
        } else if (expr instanceof LimboOrderingTerm) {
            visit((LimboOrderingTerm) expr);
        } else if (expr instanceof LimboExpression.InOperation) {
            visit((InOperation) expr);
        } else if (expr instanceof Cast) {
            visit((Cast) expr);
        } else if (expr instanceof Subquery) {
            visit((Subquery) expr);
        } else if (expr instanceof Join) {
            visit((Join) expr);
        } else if (expr instanceof LimboSelect) {
            visit((LimboSelect) expr, true);
        } else if (expr instanceof LimboExist) {
            visit((LimboExist) expr);
        } else if (expr instanceof BinaryComparisonOperation) {
            visit((BinaryComparisonOperation) expr);
        } else if (expr instanceof LimboFunction) {
            visit((LimboFunction) expr);
        } else if (expr instanceof LimboDistinct) {
            visit((LimboDistinct) expr);
        } else if (expr instanceof LimboCaseWithoutBaseExpression) {
            visit((LimboCaseWithoutBaseExpression) expr);
        } else if (expr instanceof LimboCaseWithBaseExpression) {
            visit((LimboCaseWithBaseExpression) expr);
        } else if (expr instanceof LimboAggregate) {
            visit((LimboAggregate) expr);
        } else if (expr instanceof LimboPostfixText) {
            visit((LimboPostfixText) expr);
        } else if (expr instanceof LimboWindowFunction) {
            visit((LimboWindowFunction) expr);
        } else if (expr instanceof MatchOperation) {
            visit((MatchOperation) expr);
        } else if (expr instanceof LimboRowValueExpression) {
            visit((LimboRowValueExpression) expr);
        } else if (expr instanceof LimboText) {
            visit((LimboText) expr);
        } else if (expr instanceof LimboWindowFunctionExpression) {
            visit((LimboWindowFunctionExpression) expr);
        } else if (expr instanceof LimboWindowFunctionFrameSpecTerm) {
            visit((LimboWindowFunctionFrameSpecTerm) expr);
        } else if (expr instanceof LimboWindowFunctionFrameSpecBetween) {
            visit((LimboWindowFunctionFrameSpecBetween) expr);
        } else if (expr instanceof LimboTableReference) {
            visit((LimboTableReference) expr);
        } else if (expr instanceof LimboSetClause) {
            visit((LimboSetClause) expr);
        } else if (expr instanceof LimboAlias) {
            visit((LimboAlias) expr);
        } else if (expr instanceof LimboWithClause) {
            visit((LimboWithClause) expr);
        } else if (expr instanceof LimboTableAndColumnRef) {
            visit((LimboTableAndColumnRef) expr);
        } else if (expr instanceof LimboValues) {
            visit((LimboValues) expr);
        } else if (expr instanceof LimboExpressionBag) {
            visit((LimboExpressionBag) expr);
        } else if (expr instanceof LimboTypeof) {
            visit((LimboTypeof) expr);
        } else if (expr instanceof LimboResultMap) {
            visit((LimboResultMap) expr);
        } else {
            throw new AssertionError(expr);
        }
    }

    static String asString(LimboExpression expr) {
        if (expr == null) {
            throw new AssertionError();
        }
        LimboToStringVisitor visitor = new LimboToStringVisitor();
        if (expr instanceof LimboSelect) {
            visitor.visit((LimboSelect) expr, false);
        } else {
            visitor.visit(expr);
        }
        return visitor.get();
    }

    static String asExpectedValues(LimboExpression expr) {
        LimboExpectedValueVisitor visitor = new LimboExpectedValueVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

}
