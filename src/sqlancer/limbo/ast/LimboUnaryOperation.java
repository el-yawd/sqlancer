package sqlancer.limbo.ast;

import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.limbo.LimboCollateHelper;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public class LimboUnaryOperation extends LimboExpression implements UnaryOperation<LimboExpression> {

    private final LimboUnaryOperation.UnaryOperator operation;
    private final LimboExpression expression;

    public LimboUnaryOperation(LimboUnaryOperation.UnaryOperator operation, LimboExpression expression) {
        this.operation = operation;
        this.expression = expression;
    }

    // For the purposes of the previous sentence, a column name preceded by one or
    // more unary "+" operators is still considered a column name.
    @Override
    public LimboCollateSequence getImplicitCollateSequence() {
        if (operation == UnaryOperator.PLUS) {
            if (LimboCollateHelper.shouldGetSubexpressionAffinity(expression)) {
                return expression.getImplicitCollateSequence();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Supported unary prefix operators are these: -, +, ~, and NOT.
     *
     * @see <a href="https://www.sqlite.org/lang_expr.html">SQL Language Expressions</a>
     *
     */
    public enum UnaryOperator {
        MINUS("-") {
            @Override
            public LimboConstant apply(LimboConstant constant) {
                if (constant.isNull()) {
                    return LimboConstant.createNullConstant();
                }
                LimboConstant intConstant;
                if (constant.getDataType() == LimboDataType.TEXT
                        || constant.getDataType() == LimboDataType.BINARY) {
                    intConstant = LimboCast.castToNumericFromNumOperand(constant);
                } else {
                    intConstant = constant;
                }
                if (intConstant.getDataType() == LimboDataType.INT) {
                    if (intConstant.asInt() == Long.MIN_VALUE) {
                        // SELECT - -9223372036854775808; -- 9.22337203685478e+18
                        return LimboConstant.createRealConstant(-(double) Long.MIN_VALUE);
                    } else {
                        return LimboConstant.createIntConstant(-intConstant.asInt());
                    }
                }
                if (intConstant.getDataType() == LimboDataType.REAL) {
                    return LimboConstant.createRealConstant(-intConstant.asDouble());
                }
                throw new AssertionError(intConstant);
            }
        },
        PLUS("+") {
            @Override
            public LimboConstant apply(LimboConstant constant) {
                return constant;
            }

        },
        NEGATE("~") {
            @Override
            public LimboConstant apply(LimboConstant constant) {
                LimboConstant intValue = LimboCast.castToInt(constant);
                if (intValue.isNull()) {
                    return intValue;
                }
                return LimboConstant.createIntConstant(~intValue.asInt());
            }
        },
        NOT("NOT") {
            @Override
            public LimboConstant apply(LimboConstant constant) {
                Optional<Boolean> boolVal = LimboCast.isTrue(constant);
                if (boolVal.isPresent()) {
                    Boolean negated = !boolVal.get();
                    return LimboConstant.createBoolean(negated);
                } else {
                    return LimboConstant.createNullConstant();
                }
            }
        };

        private String textRepresentation;

        UnaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String toString() {
            return getTextRepresentation();
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public LimboUnaryOperation.UnaryOperator getRandomOperator() {
            return Randomly.fromOptions(values());
        }

        public abstract LimboConstant apply(LimboConstant constant);

    }

    public LimboUnaryOperation.UnaryOperator getOperation() {
        return operation;
    }

    @Override
    public LimboExpression getExpression() {
        return expression;
    }

    @Override
    public LimboConstant getExpectedValue() {
        if (expression.getExpectedValue() == null) {
            return null;
        } else {
            return operation.apply(expression.getExpectedValue());
        }
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        return expression.getExplicitCollateSequence();
    }

    @Override
    public String getOperatorRepresentation() {
        return operation.getTextRepresentation();
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.PREFIX;
    }

}
