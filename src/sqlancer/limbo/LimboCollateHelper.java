package sqlancer.limbo;

import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.Cast;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboUnaryOperation;
import sqlancer.limbo.ast.LimboUnaryOperation.UnaryOperator;

public final class LimboCollateHelper {

    private LimboCollateHelper() {
    }

    public static boolean shouldGetSubexpressionAffinity(LimboExpression expression) {
        return expression instanceof LimboUnaryOperation
                && ((LimboUnaryOperation) expression).getOperation() == UnaryOperator.PLUS
                || expression instanceof Cast || expression instanceof LimboColumnName;
    }

}
