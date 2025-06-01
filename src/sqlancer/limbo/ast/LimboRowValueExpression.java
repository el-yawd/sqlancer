package sqlancer.limbo.ast;

import java.util.List;

import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public class LimboRowValueExpression extends LimboExpression {

    private final List<LimboExpression> expressions;

    public LimboRowValueExpression(List<LimboExpression> expressions) {
        this.expressions = expressions;
    }

    public List<LimboExpression> getExpressions() {
        return expressions;
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        for (LimboExpression expr : expressions) {
            LimboCollateSequence collate = expr.getExplicitCollateSequence();
            if (collate != null) {
                return collate;
            }
        }
        return null;
    }

}
