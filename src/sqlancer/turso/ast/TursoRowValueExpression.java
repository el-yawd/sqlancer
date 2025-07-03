package sqlancer.turso.ast;

import java.util.List;

import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoRowValueExpression extends TursoExpression {

    private final List<TursoExpression> expressions;

    public TursoRowValueExpression(List<TursoExpression> expressions) {
        this.expressions = expressions;
    }

    public List<TursoExpression> getExpressions() {
        return expressions;
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        for (TursoExpression expr : expressions) {
            TursoCollateSequence collate = expr.getExplicitCollateSequence();
            if (collate != null) {
                return collate;
            }
        }
        return null;
    }

}
