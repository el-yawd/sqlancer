package sqlancer.turso.ast;

import java.util.Optional;

import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public abstract class TursoCase extends TursoExpression {

    protected final CasePair[] pairs;
    protected final TursoExpression elseExpr;

    public TursoCase(CasePair[] pairs, TursoExpression elseExpr) {
        this.pairs = pairs.clone();
        this.elseExpr = elseExpr;
    }

    public static class CasePair {

        private final TursoExpression cond;
        private final TursoExpression then;

        public CasePair(TursoExpression cond, TursoExpression then) {
            this.cond = cond;
            this.then = then;
        }

        public TursoExpression getCond() {
            return cond;
        }

        public TursoExpression getThen() {
            return then;
        }
    }

    public CasePair[] getPairs() {
        return pairs.clone();
    }

    public TursoExpression getElseExpr() {
        return elseExpr;
    }

    protected TursoCollateSequence getExplicitCasePairAndElseCollate() {
        for (CasePair c : pairs) {
            if (c.getCond().getExplicitCollateSequence() != null) {
                return c.getCond().getExplicitCollateSequence();
            } else if (c.getThen().getExplicitCollateSequence() != null) {
                return c.getThen().getExplicitCollateSequence();
            }
        }
        if (elseExpr == null) {
            return null;
        } else {
            return elseExpr.getExplicitCollateSequence();
        }
    }

    public static class TursoCaseWithoutBaseExpression extends TursoCase {

        public TursoCaseWithoutBaseExpression(CasePair[] pairs, TursoExpression elseExpr) {
            super(pairs, elseExpr);
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return getExplicitCasePairAndElseCollate();
        }

        @Override
        public TursoConstant getExpectedValue() {
            for (CasePair c : pairs) {
                TursoConstant expectedValue = c.getCond().getExpectedValue();
                if (expectedValue == null) {
                    return null;
                }
                Optional<Boolean> isTrue = TursoCast.isTrue(expectedValue);
                if (isTrue.isPresent() && isTrue.get()) {
                    return c.getThen().getExpectedValue();
                }
            }
            if (elseExpr == null) {
                return TursoConstant.createNullConstant();
            } else {
                return elseExpr.getExpectedValue();
            }
        }

    }

    public static class TursoCaseWithBaseExpression extends TursoCase {

        private final TursoExpression baseExpr;

        public TursoCaseWithBaseExpression(TursoExpression baseExpr, CasePair[] pairs, TursoExpression elseExpr) {
            super(pairs, elseExpr);
            this.baseExpr = baseExpr;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            if (baseExpr.getExplicitCollateSequence() != null) {
                return baseExpr.getExplicitCollateSequence();
            } else {
                return getExplicitCasePairAndElseCollate();
            }
        }

        public TursoExpression getBaseExpr() {
            return baseExpr;
        }

        @Override
        public TursoConstant getExpectedValue() {
            TursoConstant baseExprValue = baseExpr.getExpectedValue();
            if (baseExprValue == null) {
                return null;
            }
            for (CasePair c : pairs) {
                TursoConstant whenComparisonValue = c.getCond().getExpectedValue();
                if (whenComparisonValue == null) {
                    return null;
                }
                TursoCollateSequence seq;
                if (baseExpr.getExplicitCollateSequence() != null) {
                    seq = baseExpr.getExplicitCollateSequence();
                } else if (c.getCond().getExplicitCollateSequence() != null) {
                    seq = c.getCond().getExplicitCollateSequence();
                } else if (c.getCond().getImplicitCollateSequence() != null) {
                    seq = c.getCond().getImplicitCollateSequence();
                } else if (c.getCond().getImplicitCollateSequence() != null) {
                    seq = c.getCond().getImplicitCollateSequence();
                } else {
                    seq = TursoCollateSequence.BINARY;
                }
                ConstantTuple newVals = applyAffinities(baseExpr.getAffinity(), c.getCond().getAffinity(),
                        baseExpr.getExpectedValue(), c.getCond().getExpectedValue());
                TursoConstant equals = newVals.left.applyEquals(newVals.right, seq);
                if (!equals.isNull() && equals.asInt() == 1) {
                    return c.getThen().getExpectedValue();
                }
            }
            if (elseExpr == null) {
                return TursoConstant.createNullConstant();
            } else {
                return elseExpr.getExpectedValue();
            }
        }

    }
}
