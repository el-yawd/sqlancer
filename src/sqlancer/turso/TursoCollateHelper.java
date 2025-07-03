package sqlancer.turso;

import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoExpression.Cast;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoUnaryOperation;
import sqlancer.turso.ast.TursoUnaryOperation.UnaryOperator;

public final class TursoCollateHelper {

    private TursoCollateHelper() {
    }

    public static boolean shouldGetSubexpressionAffinity(TursoExpression expression) {
        return expression instanceof TursoUnaryOperation
                && ((TursoUnaryOperation) expression).getOperation() == UnaryOperator.PLUS
                || expression instanceof Cast || expression instanceof TursoColumnName;
    }

}
