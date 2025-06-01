package sqlancer.limbo.ast;

import java.util.Optional;

import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public abstract class LimboCase extends LimboExpression {

    protected final CasePair[] pairs;
    protected final LimboExpression elseExpr;

    public LimboCase(CasePair[] pairs, LimboExpression elseExpr) {
        this.pairs = pairs.clone();
        this.elseExpr = elseExpr;
    }

    public static class CasePair {

        private final LimboExpression cond;
        private final LimboExpression then;

        public CasePair(LimboExpression cond, LimboExpression then) {
            this.cond = cond;
            this.then = then;
        }

        public LimboExpression getCond() {
            return cond;
        }

        public LimboExpression getThen() {
            return then;
        }
    }

    public CasePair[] getPairs() {
        return pairs.clone();
    }

    public LimboExpression getElseExpr() {
        return elseExpr;
    }

    protected LimboCollateSequence getExplicitCasePairAndElseCollate() {
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

    public static class LimboCaseWithoutBaseExpression extends LimboCase {

        public LimboCaseWithoutBaseExpression(CasePair[] pairs, LimboExpression elseExpr) {
            super(pairs, elseExpr);
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return getExplicitCasePairAndElseCollate();
        }

        @Override
        public LimboConstant getExpectedValue() {
            for (CasePair c : pairs) {
                LimboConstant expectedValue = c.getCond().getExpectedValue();
                if (expectedValue == null) {
                    return null;
                }
                Optional<Boolean> isTrue = LimboCast.isTrue(expectedValue);
                if (isTrue.isPresent() && isTrue.get()) {
                    return c.getThen().getExpectedValue();
                }
            }
            if (elseExpr == null) {
                return LimboConstant.createNullConstant();
            } else {
                return elseExpr.getExpectedValue();
            }
        }

    }

    public static class LimboCaseWithBaseExpression extends LimboCase {

        private final LimboExpression baseExpr;

        public LimboCaseWithBaseExpression(LimboExpression baseExpr, CasePair[] pairs, LimboExpression elseExpr) {
            super(pairs, elseExpr);
            this.baseExpr = baseExpr;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            if (baseExpr.getExplicitCollateSequence() != null) {
                return baseExpr.getExplicitCollateSequence();
            } else {
                return getExplicitCasePairAndElseCollate();
            }
        }

        public LimboExpression getBaseExpr() {
            return baseExpr;
        }

        @Override
        public LimboConstant getExpectedValue() {
            LimboConstant baseExprValue = baseExpr.getExpectedValue();
            if (baseExprValue == null) {
                return null;
            }
            for (CasePair c : pairs) {
                LimboConstant whenComparisonValue = c.getCond().getExpectedValue();
                if (whenComparisonValue == null) {
                    return null;
                }
                LimboCollateSequence seq;
                if (baseExpr.getExplicitCollateSequence() != null) {
                    seq = baseExpr.getExplicitCollateSequence();
                } else if (c.getCond().getExplicitCollateSequence() != null) {
                    seq = c.getCond().getExplicitCollateSequence();
                } else if (c.getCond().getImplicitCollateSequence() != null) {
                    seq = c.getCond().getImplicitCollateSequence();
                } else if (c.getCond().getImplicitCollateSequence() != null) {
                    seq = c.getCond().getImplicitCollateSequence();
                } else {
                    seq = LimboCollateSequence.BINARY;
                }
                ConstantTuple newVals = applyAffinities(baseExpr.getAffinity(), c.getCond().getAffinity(),
                        baseExpr.getExpectedValue(), c.getCond().getExpectedValue());
                LimboConstant equals = newVals.left.applyEquals(newVals.right, seq);
                if (!equals.isNull() && equals.asInt() == 1) {
                    return c.getThen().getExpectedValue();
                }
            }
            if (elseExpr == null) {
                return LimboConstant.createNullConstant();
            } else {
                return elseExpr.getExpectedValue();
            }
        }

    }
}
