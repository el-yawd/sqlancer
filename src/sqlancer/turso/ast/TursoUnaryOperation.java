package sqlancer.turso.ast;

import java.util.Optional;

import sqlancer.Randomly;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.turso.TursoCollateHelper;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoUnaryOperation extends TursoExpression implements UnaryOperation<TursoExpression> {

    private final TursoUnaryOperation.UnaryOperator operation;
    private final TursoExpression expression;

    public TursoUnaryOperation(TursoUnaryOperation.UnaryOperator operation, TursoExpression expression) {
        this.operation = operation;
        this.expression = expression;
    }

    // For the purposes of the previous sentence, a column name preceded by one or
    // more unary "+" operators is still considered a column name.
    @Override
    public TursoCollateSequence getImplicitCollateSequence() {
        if (operation == UnaryOperator.PLUS) {
            if (TursoCollateHelper.shouldGetSubexpressionAffinity(expression)) {
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
            public TursoConstant apply(TursoConstant constant) {
                if (constant.isNull()) {
                    return TursoConstant.createNullConstant();
                }
                TursoConstant intConstant;
                if (constant.getDataType() == TursoDataType.TEXT
                        || constant.getDataType() == TursoDataType.BINARY) {
                    intConstant = TursoCast.castToNumericFromNumOperand(constant);
                } else {
                    intConstant = constant;
                }
                if (intConstant.getDataType() == TursoDataType.INT) {
                    if (intConstant.asInt() == Long.MIN_VALUE) {
                        // SELECT - -9223372036854775808; -- 9.22337203685478e+18
                        return TursoConstant.createRealConstant(-(double) Long.MIN_VALUE);
                    } else {
                        return TursoConstant.createIntConstant(-intConstant.asInt());
                    }
                }
                if (intConstant.getDataType() == TursoDataType.REAL) {
                    return TursoConstant.createRealConstant(-intConstant.asDouble());
                }
                throw new AssertionError(intConstant);
            }
        },
        PLUS("+") {
            @Override
            public TursoConstant apply(TursoConstant constant) {
                return constant;
            }

        },
        NEGATE("~") {
            @Override
            public TursoConstant apply(TursoConstant constant) {
                TursoConstant intValue = TursoCast.castToInt(constant);
                if (intValue.isNull()) {
                    return intValue;
                }
                return TursoConstant.createIntConstant(~intValue.asInt());
            }
        },
        NOT("NOT") {
            @Override
            public TursoConstant apply(TursoConstant constant) {
                Optional<Boolean> boolVal = TursoCast.isTrue(constant);
                if (boolVal.isPresent()) {
                    Boolean negated = !boolVal.get();
                    return TursoConstant.createBoolean(negated);
                } else {
                    return TursoConstant.createNullConstant();
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

        public TursoUnaryOperation.UnaryOperator getRandomOperator() {
            return Randomly.fromOptions(values());
        }

        public abstract TursoConstant apply(TursoConstant constant);

    }

    public TursoUnaryOperation.UnaryOperator getOperation() {
        return operation;
    }

    @Override
    public TursoExpression getExpression() {
        return expression;
    }

    @Override
    public TursoConstant getExpectedValue() {
        if (expression.getExpectedValue() == null) {
            return null;
        } else {
            return operation.apply(expression.getExpectedValue());
        }
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
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
