package sqlancer.turso.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.turso.TursoProvider;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class TursoAggregate extends TursoExpression {

    private final TursoAggregateFunction func;
    private final List<TursoExpression> expr;

    public enum TursoAggregateFunction {
        AVG() {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                return TursoCast.castToReal(exprVal);
            }

        },
        COUNT() {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                int count;
                if (exprVal.isNull()) {
                    count = 0;
                } else {
                    count = 1;
                }
                return TursoConstant.createIntConstant(count);
            }
        },
        COUNT_ALL() {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                return TursoConstant.createIntConstant(1);
            }
        },
        GROUP_CONCAT() {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                TursoConstant castToText = TursoCast.castToText(exprVal);
                if (castToText == null && TursoProvider.mustKnowResult) {
                    throw new IgnoreMeException();
                }
                return castToText;
            }
        },
        MAX {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                return exprVal;
            }
        },
        MIN {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                return exprVal;
            }
        },
        SUM() {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                return TursoCast.castToReal(exprVal);
            }

        },
        TOTAL() {
            @Override
            public TursoConstant apply(TursoConstant exprVal) {
                if (exprVal.isNull()) {
                    return TursoConstant.createRealConstant(0);
                } else {
                    return TursoCast.castToReal(exprVal);
                }
            }

        };

        public abstract TursoConstant apply(TursoConstant exprVal);

        public static TursoAggregateFunction getRandom() {
            List<TursoAggregateFunction> functions = new ArrayList<>(Arrays.asList(values()));
            if (TursoProvider.mustKnowResult) {
                functions.remove(TursoAggregateFunction.SUM);
                functions.remove(TursoAggregateFunction.TOTAL);
                functions.remove(TursoAggregateFunction.GROUP_CONCAT);
            }
            return Randomly.fromOptions(values());
        }

        public static TursoAggregateFunction getRandom(TursoDataType type) {
            return Randomly.fromOptions(values());
        }

    }

    public TursoAggregate(List<TursoExpression> expr, TursoAggregateFunction func) {
        this.expr = expr;
        this.func = func;
    }

    public TursoAggregateFunction getFunc() {
        return func;
    }

    public List<TursoExpression> getExpr() {
        return expr;
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        return null;
        // return expr.getExplicitCollateSequence();
    }

    @Override
    public TursoConstant getExpectedValue() {
        assert !TursoProvider.mustKnowResult;
        return null;
        // return func.apply(expr.getExpectedValue());
    }

}
