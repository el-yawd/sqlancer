package sqlancer.turso.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.turso.ast.TursoConstant.TursoTextConstant;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoFunction extends TursoExpression {

    private final ComputableFunction func;
    private final TursoExpression[] args;

    public TursoFunction(ComputableFunction func, TursoExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public enum ComputableFunction {

        ABS(1, "ABS") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                TursoConstant castValue;
                if (args[0].getDataType() == TursoDataType.BINARY) {
                    throw new IgnoreMeException(); // TODO
                                                   // implement
                }
                if (args[0].getDataType() == TursoDataType.INT) {
                    castValue = TursoCast.castToInt(args[0]);
                } else {
                    castValue = TursoCast.castToNumericNoNumAsRealZero(args[0]);
                }
                if (castValue.isNull()) {
                    return castValue;
                } else if (castValue.getDataType() == TursoDataType.INT) {
                    long absVal = Math.abs(castValue.asInt());
                    return TursoConstant.createIntConstant(absVal);
                } else {
                    assert castValue.getDataType() == TursoDataType.REAL;
                    double absVal = Math.abs(castValue.asDouble());
                    return TursoConstant.createRealConstant(absVal);
                }
            }
        },

        COALESCE(2, "COALESCE") {

            @Override
            public TursoConstant apply(TursoConstant... args) {
                for (TursoConstant arg : args) {
                    if (!arg.isNull()) {
                        return arg;
                    }
                }
                return TursoConstant.createNullConstant();
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },

        HEX(1, "HEX") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return null;
                // TursoConstant binaryValue = TursoCast.castToBlob(args[0]);
                // return
                // TursoConstant.createTextConstant(binaryValue.getStringRepresentation());
            }
        },

        LOWER(1, "LOWER") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                args[0] = TursoCast.castToText(args[0]);
                if (args[0] == null) {
                    return null;
                }
                if (args[0].getDataType() == TursoDataType.TEXT) {
                    StringBuilder text = new StringBuilder(args[0].asString());
                    for (int i = 0; i < text.length(); i++) {
                        char c = text.charAt(i);
                        if (c >= 'A' && c <= 'Z') {
                            text.setCharAt(i, Character.toLowerCase(c));
                        }
                    }
                    return TursoConstant.createTextConstant(text.toString());
                } else {
                    return TursoConstant.createNullConstant();
                }
            }
        },
        LIKELY(1, "LIKELY") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return args[0];
            }

        },
        LIKELIHOOD(2, "LIKELIHOOD") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return args[0];
            }

        },
        IFNULL(2, "IFNULL") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                for (TursoExpression arg : args) {
                    if (!arg.getExpectedValue().isNull()) {
                        return arg.getExpectedValue();
                    }
                }
                return TursoConstant.createNullConstant();
            }
        },

        UPPER(1, "UPPER") {

            @Override
            public TursoConstant apply(TursoConstant... args) {
                args[0] = TursoCast.castToText(args[0]);
                if (args[0] == null) {
                    return null;
                }
                if (args[0].getDataType() == TursoDataType.TEXT) {
                    String string = TursoTextConstant.toUpper(args[0].asString());
                    return TursoConstant.createTextConstant(string);
                } else {
                    return TursoConstant.createNullConstant();
                }
            }

        },
        NULLIF(2, "NULLIF") {
            @Override
            public TursoConstant apply(TursoConstant[] args, TursoCollateSequence collateSequence) {
                TursoConstant equals = args[0].applyEquals(args[1],
                        collateSequence == null ? TursoCollateSequence.BINARY : collateSequence);
                if (TursoCast.isTrue(equals).isPresent() && TursoCast.isTrue(equals).get()) {
                    return TursoConstant.createNullConstant();
                } else {
                    return args[0];
                }
            }

            @Override
            public TursoConstant apply(TursoConstant... args) {
                TursoCollateSequence collateSequence = null;
                for (TursoConstant con : args) {
                    if (con.getExplicitCollateSequence() != null) {
                        collateSequence = con.getExplicitCollateSequence();
                        break;
                    }
                }
                if (collateSequence == null) {
                    for (TursoConstant con : args) {
                        if (con.getImplicitCollateSequence() != null) {
                            collateSequence = con.getImplicitCollateSequence();
                            break;
                        }
                    }
                }
                return apply(args, collateSequence);
            }
        },
        TRIM(1, "TRIM") {

            @Override
            public TursoConstant apply(TursoConstant... args) {
                TursoConstant str = TursoCast.castToText(args[0]);
                if (args[0].getDataType() == TursoDataType.TEXT) {
                    String text = str.asString();
                    return TursoConstant.createTextConstant(TursoTextConstant.trim(text));
                } else {
                    return str;
                }
            }

        },
        TRIM_TWO_ARGS(2, "TRIM") {

            @Override
            public TursoConstant apply(TursoConstant... args) {
                if (args[0].isNull() || args[1].isNull()) {
                    return TursoConstant.createNullConstant();
                }
                TursoConstant str = TursoCast.castToText(args[0]);
                TursoConstant castToText = TursoCast.castToText(args[1]);
                if (str == null || castToText == null) {
                    return null;
                }
                String remove = castToText.asString();
                StringBuilder text = new StringBuilder(str.asString());
                int i = 0;
                while (i < text.length()) {
                    boolean shouldRemoveChar = false;
                    char c = text.charAt(i);
                    for (char charToRemove : remove.toCharArray()) {
                        if (charToRemove == c) {
                            shouldRemoveChar = true;
                            break;
                        }
                    }
                    if (shouldRemoveChar) {
                        text.deleteCharAt(i);
                    } else {
                        break;
                    }
                }
                i = text.length() - 1;
                while (i >= 0) {
                    boolean shouldRemoveChar = false;
                    char c = text.charAt(i);
                    for (char charToRemove : remove.toCharArray()) {
                        if (charToRemove == c) {
                            shouldRemoveChar = true;
                            break;
                        }
                    }
                    if (shouldRemoveChar) {
                        text.deleteCharAt(i);
                        i--;
                    } else {
                        break;
                    }
                }
                String string = text.toString();
                assert string != null;
                return TursoConstant.createTextConstant(string);
            }
        },
        TYPEOF(1, "TYPEOF") {

            @Override
            public TursoConstant apply(TursoConstant... args) {
                switch (args[0].getDataType()) {
                case BINARY:
                    return TursoConstant.createTextConstant("blob");
                case INT:
                    return TursoConstant.createTextConstant("integer");
                case NULL:
                    return TursoConstant.createTextConstant("null");
                case REAL:
                    return TursoConstant.createTextConstant("real");
                case TEXT:
                    return TursoConstant.createTextConstant("text");
                default:
                    throw new AssertionError(args[0]);
                }
            }

        },
        UNLIKELY(1, "UNLIKELY") {
            @Override
            public TursoConstant apply(TursoConstant... args) {
                return args[0];
            }

        };

        private String functionName;
        final int nrArgs;

        ComputableFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract TursoConstant apply(TursoConstant... args);

        public TursoConstant apply(TursoConstant[] evaluatedArgs, TursoCollateSequence collate) {
            return apply(evaluatedArgs);
        }

        public static ComputableFunction getRandomFunction() {
            return Randomly.fromOptions(ComputableFunction.values());
        }

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return false;
        }

        public TypeAffinity getAffinity(TursoExpression... args) {
            return TypeAffinity.NONE;
        }

    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        for (TursoExpression expr : args) {
            if (expr.getExplicitCollateSequence() != null) {
                return expr.getExplicitCollateSequence();
            }
        }
        return null;
    }

    public TursoExpression[] getArgs() {
        return args.clone();
    }

    public ComputableFunction getFunc() {
        return func;
    }

    @Override
    public TursoConstant getExpectedValue() {
        TursoConstant[] constants = new TursoConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        TursoCollateSequence collate = getExplicitCollateSequence();
        if (collate == null) {
            for (TursoExpression arg : args) {
                if (arg.getImplicitCollateSequence() != null) {
                    collate = arg.getImplicitCollateSequence();
                    break;
                }
            }
        }
        return func.apply(constants, collate);

    };

    @Override
    public TypeAffinity getAffinity() {
        return func.getAffinity(args);
    }

}
