package sqlancer.limbo.ast;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.limbo.ast.LimboConstant.LimboTextConstant;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public class LimboFunction extends LimboExpression {

    private final ComputableFunction func;
    private final LimboExpression[] args;

    public LimboFunction(ComputableFunction func, LimboExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public enum ComputableFunction {

        ABS(1, "ABS") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                LimboConstant castValue;
                if (args[0].getDataType() == LimboDataType.BINARY) {
                    throw new IgnoreMeException(); // TODO
                                                   // implement
                }
                if (args[0].getDataType() == LimboDataType.INT) {
                    castValue = LimboCast.castToInt(args[0]);
                } else {
                    castValue = LimboCast.castToNumericNoNumAsRealZero(args[0]);
                }
                if (castValue.isNull()) {
                    return castValue;
                } else if (castValue.getDataType() == LimboDataType.INT) {
                    long absVal = Math.abs(castValue.asInt());
                    return LimboConstant.createIntConstant(absVal);
                } else {
                    assert castValue.getDataType() == LimboDataType.REAL;
                    double absVal = Math.abs(castValue.asDouble());
                    return LimboConstant.createRealConstant(absVal);
                }
            }
        },

        COALESCE(2, "COALESCE") {

            @Override
            public LimboConstant apply(LimboConstant... args) {
                for (LimboConstant arg : args) {
                    if (!arg.isNull()) {
                        return arg;
                    }
                }
                return LimboConstant.createNullConstant();
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },

        HEX(1, "HEX") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return null;
                // LimboConstant binaryValue = LimboCast.castToBlob(args[0]);
                // return
                // LimboConstant.createTextConstant(binaryValue.getStringRepresentation());
            }
        },

        LOWER(1, "LOWER") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                args[0] = LimboCast.castToText(args[0]);
                if (args[0] == null) {
                    return null;
                }
                if (args[0].getDataType() == LimboDataType.TEXT) {
                    StringBuilder text = new StringBuilder(args[0].asString());
                    for (int i = 0; i < text.length(); i++) {
                        char c = text.charAt(i);
                        if (c >= 'A' && c <= 'Z') {
                            text.setCharAt(i, Character.toLowerCase(c));
                        }
                    }
                    return LimboConstant.createTextConstant(text.toString());
                } else {
                    return LimboConstant.createNullConstant();
                }
            }
        },
        LIKELY(1, "LIKELY") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return args[0];
            }

        },
        LIKELIHOOD(2, "LIKELIHOOD") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                return args[0];
            }

        },
        IFNULL(2, "IFNULL") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
                for (LimboExpression arg : args) {
                    if (!arg.getExpectedValue().isNull()) {
                        return arg.getExpectedValue();
                    }
                }
                return LimboConstant.createNullConstant();
            }
        },

        UPPER(1, "UPPER") {

            @Override
            public LimboConstant apply(LimboConstant... args) {
                args[0] = LimboCast.castToText(args[0]);
                if (args[0] == null) {
                    return null;
                }
                if (args[0].getDataType() == LimboDataType.TEXT) {
                    String string = LimboTextConstant.toUpper(args[0].asString());
                    return LimboConstant.createTextConstant(string);
                } else {
                    return LimboConstant.createNullConstant();
                }
            }

        },
        NULLIF(2, "NULLIF") {
            @Override
            public LimboConstant apply(LimboConstant[] args, LimboCollateSequence collateSequence) {
                LimboConstant equals = args[0].applyEquals(args[1],
                        collateSequence == null ? LimboCollateSequence.BINARY : collateSequence);
                if (LimboCast.isTrue(equals).isPresent() && LimboCast.isTrue(equals).get()) {
                    return LimboConstant.createNullConstant();
                } else {
                    return args[0];
                }
            }

            @Override
            public LimboConstant apply(LimboConstant... args) {
                LimboCollateSequence collateSequence = null;
                for (LimboConstant con : args) {
                    if (con.getExplicitCollateSequence() != null) {
                        collateSequence = con.getExplicitCollateSequence();
                        break;
                    }
                }
                if (collateSequence == null) {
                    for (LimboConstant con : args) {
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
            public LimboConstant apply(LimboConstant... args) {
                LimboConstant str = LimboCast.castToText(args[0]);
                if (args[0].getDataType() == LimboDataType.TEXT) {
                    String text = str.asString();
                    return LimboConstant.createTextConstant(LimboTextConstant.trim(text));
                } else {
                    return str;
                }
            }

        },
        TRIM_TWO_ARGS(2, "TRIM") {

            @Override
            public LimboConstant apply(LimboConstant... args) {
                if (args[0].isNull() || args[1].isNull()) {
                    return LimboConstant.createNullConstant();
                }
                LimboConstant str = LimboCast.castToText(args[0]);
                LimboConstant castToText = LimboCast.castToText(args[1]);
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
                return LimboConstant.createTextConstant(string);
            }
        },
        TYPEOF(1, "TYPEOF") {

            @Override
            public LimboConstant apply(LimboConstant... args) {
                switch (args[0].getDataType()) {
                case BINARY:
                    return LimboConstant.createTextConstant("blob");
                case INT:
                    return LimboConstant.createTextConstant("integer");
                case NULL:
                    return LimboConstant.createTextConstant("null");
                case REAL:
                    return LimboConstant.createTextConstant("real");
                case TEXT:
                    return LimboConstant.createTextConstant("text");
                default:
                    throw new AssertionError(args[0]);
                }
            }

        },
        UNLIKELY(1, "UNLIKELY") {
            @Override
            public LimboConstant apply(LimboConstant... args) {
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

        public abstract LimboConstant apply(LimboConstant... args);

        public LimboConstant apply(LimboConstant[] evaluatedArgs, LimboCollateSequence collate) {
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

        public TypeAffinity getAffinity(LimboExpression... args) {
            return TypeAffinity.NONE;
        }

    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        for (LimboExpression expr : args) {
            if (expr.getExplicitCollateSequence() != null) {
                return expr.getExplicitCollateSequence();
            }
        }
        return null;
    }

    public LimboExpression[] getArgs() {
        return args.clone();
    }

    public ComputableFunction getFunc() {
        return func;
    }

    @Override
    public LimboConstant getExpectedValue() {
        LimboConstant[] constants = new LimboConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        LimboCollateSequence collate = getExplicitCollateSequence();
        if (collate == null) {
            for (LimboExpression arg : args) {
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
