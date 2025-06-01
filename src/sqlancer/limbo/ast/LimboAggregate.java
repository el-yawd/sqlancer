package sqlancer.limbo.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.limbo.LimboProvider;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class LimboAggregate extends LimboExpression {

    private final LimboAggregateFunction func;
    private final List<LimboExpression> expr;

    public enum LimboAggregateFunction {
        AVG() {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                return LimboCast.castToReal(exprVal);
            }

        },
        COUNT() {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                int count;
                if (exprVal.isNull()) {
                    count = 0;
                } else {
                    count = 1;
                }
                return LimboConstant.createIntConstant(count);
            }
        },
        COUNT_ALL() {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                return LimboConstant.createIntConstant(1);
            }
        },
        GROUP_CONCAT() {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                LimboConstant castToText = LimboCast.castToText(exprVal);
                if (castToText == null && LimboProvider.mustKnowResult) {
                    throw new IgnoreMeException();
                }
                return castToText;
            }
        },
        MAX {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                return exprVal;
            }
        },
        MIN {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                return exprVal;
            }
        },
        SUM() {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                return LimboCast.castToReal(exprVal);
            }

        },
        TOTAL() {
            @Override
            public LimboConstant apply(LimboConstant exprVal) {
                if (exprVal.isNull()) {
                    return LimboConstant.createRealConstant(0);
                } else {
                    return LimboCast.castToReal(exprVal);
                }
            }

        };

        public abstract LimboConstant apply(LimboConstant exprVal);

        public static LimboAggregateFunction getRandom() {
            List<LimboAggregateFunction> functions = new ArrayList<>(Arrays.asList(values()));
            if (LimboProvider.mustKnowResult) {
                functions.remove(LimboAggregateFunction.SUM);
                functions.remove(LimboAggregateFunction.TOTAL);
                functions.remove(LimboAggregateFunction.GROUP_CONCAT);
            }
            return Randomly.fromOptions(values());
        }

        public static LimboAggregateFunction getRandom(LimboDataType type) {
            return Randomly.fromOptions(values());
        }

    }

    public LimboAggregate(List<LimboExpression> expr, LimboAggregateFunction func) {
        this.expr = expr;
        this.func = func;
    }

    public LimboAggregateFunction getFunc() {
        return func;
    }

    public List<LimboExpression> getExpr() {
        return expr;
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        return null;
        // return expr.getExplicitCollateSequence();
    }

    @Override
    public LimboConstant getExpectedValue() {
        assert !LimboProvider.mustKnowResult;
        return null;
        // return func.apply(expr.getExpectedValue());
    }

}
