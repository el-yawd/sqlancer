package sqlancer.turso.ast;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoProvider;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoWindowFunction extends TursoExpression {

    private WindowFunction func;
    private TursoExpression[] args;

    public static TursoWindowFunction getRandom(List<TursoColumn> columns, TursoGlobalState globalState) {
        WindowFunction func = Randomly.fromOptions(WindowFunction.values());
        TursoExpression[] args = new TursoExpression[func.nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = new TursoExpressionGenerator(globalState).setColumns(columns).generateExpression();
        }
        return new TursoWindowFunction(func, args);
    }

    public enum WindowFunction {

        ROW_NUMBER {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return TursoConstant.createIntConstant(1);
            }

        },
        RANK {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return TursoConstant.createIntConstant(1);
            }
        },
        DENSE_RANK {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return TursoConstant.createIntConstant(1);
            }
        },
        PERCENT_RANK {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return TursoConstant.createRealConstant(0.0);
            }
        },
        CUME_DIST {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return TursoConstant.createRealConstant(1.0);
            }
        },
        NTILE(1), //
        LAG(3), //
        LEAD(3), //
        FIRST_VALUE(1) {

            @Override
            public TursoConstant apply(TursoConstant... args) {
                return args[0];
            }
        },
        LAST_VALUE(1) {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return args[0];
            }
        },
        NTH_VALUE(2) {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                TursoConstant n = TursoCast.castToInt(args[1]);
                if (!n.isNull() && n.asInt() == 1) {
                    return args[0];
                } else {
                    return TursoConstant.createNullConstant();
                }
            }
        };

        int nrArgs;

        WindowFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        };

        WindowFunction() {
            this(0);
        }

        public TursoConstant apply(TursoConstant... args) {
            if (TursoProvider.mustKnowResult) {
                throw new AssertionError();
            }
            return null;
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    public TursoWindowFunction(WindowFunction func, TursoExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public WindowFunction getFunc() {
        return func;
    }

    public TursoExpression[] getArgs() {
        return args.clone();
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public TursoConstant getExpectedValue() {
        if (!TursoProvider.mustKnowResult) {
            return null;
        }
        TursoConstant[] evaluatedConst = new TursoConstant[args.length];
        for (int i = 0; i < evaluatedConst.length; i++) {
            evaluatedConst[i] = args[i].getExpectedValue();
            if (evaluatedConst[i] == null) {
                throw new IgnoreMeException();
            }
        }
        return func.apply(evaluatedConst);
    }

}
