package sqlancer.limbo.ast;

import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboProvider;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public class LimboWindowFunction extends LimboExpression {

    private WindowFunction func;
    private LimboExpression[] args;

    public static LimboWindowFunction getRandom(List<LimboColumn> columns, LimboGlobalState globalState) {
        WindowFunction func = Randomly.fromOptions(WindowFunction.values());
        LimboExpression[] args = new LimboExpression[func.nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = new LimboExpressionGenerator(globalState).setColumns(columns).generateExpression();
        }
        return new LimboWindowFunction(func, args);
    }

    public enum WindowFunction {

        ROW_NUMBER {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return LimboConstant.createIntConstant(1);
            }

        },
        RANK {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return LimboConstant.createIntConstant(1);
            }
        },
        DENSE_RANK {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return LimboConstant.createIntConstant(1);
            }
        },
        PERCENT_RANK {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return LimboConstant.createRealConstant(0.0);
            }
        },
        CUME_DIST {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return LimboConstant.createRealConstant(1.0);
            }
        },
        NTILE(1), //
        LAG(3), //
        LEAD(3), //
        FIRST_VALUE(1) {

            @Override
            public LimboConstant apply(LimboConstant... args) {
                return args[0];
            }
        },
        LAST_VALUE(1) {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return args[0];
            }
        },
        NTH_VALUE(2) {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                LimboConstant n = LimboCast.castToInt(args[1]);
                if (!n.isNull() && n.asInt() == 1) {
                    return args[0];
                } else {
                    return LimboConstant.createNullConstant();
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

        public LimboConstant apply(LimboConstant... args) {
            if (LimboProvider.mustKnowResult) {
                throw new AssertionError();
            }
            return null;
        }

        public int getNrArgs() {
            return nrArgs;
        }
    }

    public LimboWindowFunction(WindowFunction func, LimboExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public WindowFunction getFunc() {
        return func;
    }

    public LimboExpression[] getArgs() {
        return args.clone();
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public LimboConstant getExpectedValue() {
        if (!LimboProvider.mustKnowResult) {
            return null;
        }
        LimboConstant[] evaluatedConst = new LimboConstant[args.length];
        for (int i = 0; i < evaluatedConst.length; i++) {
            evaluatedConst[i] = args[i].getExpectedValue();
            if (evaluatedConst[i] == null) {
                throw new IgnoreMeException();
            }
        }
        return func.apply(evaluatedConst);
    }

}
