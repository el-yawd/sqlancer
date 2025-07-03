package sqlancer.turso.ast;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import sqlancer.IgnoreMeException;
import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.turso.TursoCollateHelper;
import sqlancer.turso.TursoProvider;
import sqlancer.turso.ast.TursoExpression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.turso.ast.TursoExpression.TursoBinaryOperation.BinaryOperator;
import sqlancer.turso.ast.TursoUnaryOperation.UnaryOperator;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public abstract class TursoExpression implements Expression<TursoColumn> {

    public static class TursoTableReference extends TursoExpression {

        private final String indexedBy;
        private final TursoTable table;

        public TursoTableReference(String indexedBy, TursoTable table) {
            this.indexedBy = indexedBy;
            this.table = table;
        }

        public TursoTableReference(TursoTable table) {
            this.indexedBy = null;
            this.table = table;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public TursoTable getTable() {
            return table;
        }

        public String getIndexedBy() {
            return indexedBy;
        }

    }

    public static class TursoDistinct extends TursoExpression {

        private final TursoExpression expr;

        public TursoDistinct(TursoExpression expr) {
            this.expr = expr;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return expr.getExplicitCollateSequence();
        }

        @Override
        public TursoConstant getExpectedValue() {
            return expr.getExpectedValue();
        }

        public TursoExpression getExpression() {
            return expr;
        }

        @Override
        public TursoCollateSequence getImplicitCollateSequence() {
            // https://www.sqlite.org/src/tktview/18ab5da2c05ad57d7f9d79c41d3138b141378543
            return expr.getImplicitCollateSequence();
        }

    }

    public TursoConstant getExpectedValue() {
        return null;
    }

    public enum TypeAffinity {
        INTEGER, TEXT, BLOB, REAL, NUMERIC, NONE;

        public boolean isNumeric() {
            return this == INTEGER || this == REAL || this == NUMERIC;
        }
    }

    /*
     * See https://www.sqlite.org/datatype3.html 3.2
     */
    public TypeAffinity getAffinity() {
        return TypeAffinity.NONE;
    }

    /*
     * See https://www.sqlite.org/datatype3.html#assigning_collating_sequences_from_sql 7.1
     *
     */
    public abstract TursoCollateSequence getExplicitCollateSequence();

    public TursoCollateSequence getImplicitCollateSequence() {
        return null;
    }

    public static class TursoExist extends TursoExpression {

        private final TursoExpression select;
        private boolean negated;

        public TursoExist(TursoExpression select, boolean negated) {
            this.select = select;
            this.negated = negated;
        }

        public void setNegated(boolean negated) {
            this.negated = negated;
        }

        public boolean getNegated() {
            return this.negated;
        }

        public TursoExpression getExpression() {
            return select;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class Join extends TursoExpression
            implements sqlancer.common.ast.newast.Join<TursoExpression, TursoTable, TursoColumn> {

        public enum JoinType {
            INNER, CROSS, OUTER, NATURAL, RIGHT, FULL;
        }

        private final TursoTable table;
        private TursoExpression onClause;
        private JoinType type;

        public Join(Join other) {
            this.table = other.table;
            this.onClause = other.onClause;
            this.type = other.type;
        }

        public Join(TursoTable table, TursoExpression onClause, JoinType type) {
            this.table = table;
            this.onClause = onClause;
            this.type = type;
        }

        public Join(TursoTable table, JoinType type) {
            this.table = table;
            if (type != JoinType.NATURAL) {
                throw new AssertionError();
            }
            this.onClause = null;
            this.type = type;
        }

        public TursoTable getTable() {
            return table;
        }

        public TursoExpression getOnClause() {
            return onClause;
        }

        public JoinType getType() {
            return type;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public void setOnClause(TursoExpression onClause) {
            this.onClause = onClause;
        }

        public void setType(JoinType type) {
            this.type = type;
        }
    }

    public static class Subquery extends TursoExpression {

        private final String query;

        public Subquery(String query) {
            this.query = query;
        }

        public static TursoExpression create(String query) {
            return new Subquery(query);
        }

        public String getQuery() {
            return query;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class TypeLiteral {

        private final Type type;

        public enum Type {
            TEXT {
                @Override
                public TursoConstant apply(TursoConstant cons) {
                    return TursoCast.castToText(cons);
                }
            },
            REAL {
                @Override
                public TursoConstant apply(TursoConstant cons) {
                    return TursoCast.castToReal(cons);
                }
            },
            INTEGER {
                @Override
                public TursoConstant apply(TursoConstant cons) {
                    return TursoCast.castToInt(cons);
                }
            },
            NUMERIC {
                @Override
                public TursoConstant apply(TursoConstant cons) {
                    return TursoCast.castToNumeric(cons);
                }
            },
            BLOB {
                @Override
                public TursoConstant apply(TursoConstant cons) {
                    return TursoCast.castToBlob(cons);
                }
            };

            public abstract TursoConstant apply(TursoConstant cons);
        }

        public TypeLiteral(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }

    }

    public static class Cast extends TursoExpression {

        private final TypeLiteral type;
        private final TursoExpression expression;

        public Cast(TypeLiteral typeofExpr, TursoExpression expression) {
            this.type = typeofExpr;
            this.expression = expression;
        }

        public TursoExpression getExpression() {
            return expression;
        }

        public TypeLiteral getType() {
            return type;
        }

        @Override
        public TursoConstant getExpectedValue() {
            if (expression.getExpectedValue() == null) {
                return null;
            } else {
                return type.type.apply(expression.getExpectedValue());
            }
        }

        /**
         * An expression of the form "CAST(expr AS type)" has an affinity that is the same as a column with a declared
         * type of "type".
         */
        @Override
        public TypeAffinity getAffinity() {
            switch (type.type) {
            case BLOB:
                return TypeAffinity.BLOB;
            case INTEGER:
                return TypeAffinity.INTEGER;
            case NUMERIC:
                return TypeAffinity.NUMERIC;
            case REAL:
                return TypeAffinity.REAL;
            case TEXT:
                return TypeAffinity.TEXT;
            default:
                throw new AssertionError();
            }
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

        @Override
        public TursoCollateSequence getImplicitCollateSequence() {
            if (TursoCollateHelper.shouldGetSubexpressionAffinity(expression)) {
                return expression.getImplicitCollateSequence();
            } else {
                return null;
            }
        }

    }

    public static class BetweenOperation extends TursoExpression {

        private final TursoExpression expr;
        private final boolean negated;
        private final TursoExpression left;
        private final TursoExpression right;

        public BetweenOperation(TursoExpression expr, boolean negated, TursoExpression left,
                TursoExpression right) {
            this.expr = expr;
            this.negated = negated;
            this.left = left;
            this.right = right;
        }

        public TursoExpression getExpression() {
            return expr;
        }

        public boolean isNegated() {
            return negated;
        }

        public TursoExpression getLeft() {
            return left;
        }

        public TursoExpression getRight() {
            return right;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            if (expr.getExplicitCollateSequence() != null) {
                return expr.getExplicitCollateSequence();
            } else if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        @Override
        public TursoConstant getExpectedValue() {
            return getTopNode().getExpectedValue();
        }

        public TursoExpression getTopNode() {
            BinaryComparisonOperation leftOp = new BinaryComparisonOperation(expr, left,
                    BinaryComparisonOperator.GREATER_EQUALS);
            BinaryComparisonOperation rightOp = new BinaryComparisonOperation(expr, right,
                    BinaryComparisonOperator.SMALLER_EQUALS);
            TursoBinaryOperation and = new TursoBinaryOperation(leftOp, rightOp, BinaryOperator.AND);
            if (negated) {
                return new TursoUnaryOperation(UnaryOperator.NOT, and);
            } else {
                return and;
            }
        }

    }

    public static class Function extends TursoExpression {

        private final TursoExpression[] arguments;
        private final String name;

        public Function(String name, TursoExpression... arguments) {
            this.name = name;
            this.arguments = arguments.clone();
        }

        public TursoExpression[] getArguments() {
            return arguments.clone();
        }

        public String getName() {
            return name;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            for (TursoExpression arg : arguments) {
                if (arg.getExplicitCollateSequence() != null) {
                    return arg.getExplicitCollateSequence();
                }
            }
            return null;
        }

    }

    public static class TursoOrderingTerm extends TursoExpression {

        private final TursoExpression expression;
        private final Ordering ordering;

        public enum Ordering {
            ASC, DESC;

            public static Ordering getRandomValue() {
                return Randomly.fromOptions(Ordering.values());
            }
        }

        public TursoOrderingTerm(TursoExpression expression, Ordering ordering) {
            this.expression = expression;
            this.ordering = ordering;
        }

        public TursoExpression getExpression() {
            return expression;
        }

        public Ordering getOrdering() {
            return ordering;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

    }

    public static class CollateOperation extends TursoExpression {

        private final TursoExpression expression;
        private final TursoCollateSequence collate;

        public CollateOperation(TursoExpression expression, TursoCollateSequence collate) {
            this.expression = expression;
            this.collate = collate;
        }

        public TursoCollateSequence getCollate() {
            return collate;
        }

        public TursoExpression getExpression() {
            return expression;
        }

        // If either operand has an explicit collating function assignment using the
        // postfix COLLATE operator, then the explicit collating function is used for
        // comparison, with precedence to the collating function of the left operand.
        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return collate;
        }

        @Override
        public TursoConstant getExpectedValue() {
            return expression.getExpectedValue();
        }

        @Override
        public TypeAffinity getAffinity() {
            return expression.getAffinity();
        }

    }

    public static class TursoPostfixUnaryOperation extends TursoExpression
            implements UnaryOperation<TursoExpression> {

        public enum PostfixUnaryOperator {
            ISNULL("ISNULL") {
                @Override
                public TursoConstant apply(TursoConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return TursoConstant.createTrue();
                    } else {
                        return TursoConstant.createFalse();
                    }
                }
            },
            NOT_NULL("NOT NULL") {
                @Override
                public TursoConstant apply(TursoConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return TursoConstant.createFalse();
                    } else {
                        return TursoConstant.createTrue();
                    }
                }

            },

            NOTNULL("NOTNULL") {

                @Override
                public TursoConstant apply(TursoConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return TursoConstant.createFalse();
                    } else {
                        return TursoConstant.createTrue();
                    }
                }

            },
            IS_TRUE("IS TRUE") {

                @Override
                public TursoConstant apply(TursoConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return TursoConstant.createIntConstant(0);
                    }
                    return TursoCast.asBoolean(expectedValue);
                }
            },
            IS_FALSE("IS FALSE") {

                @Override
                public TursoConstant apply(TursoConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return TursoConstant.createIntConstant(0);
                    }
                    return TursoUnaryOperation.UnaryOperator.NOT.apply(TursoCast.asBoolean(expectedValue));
                }

            };

            private final String textRepresentation;

            PostfixUnaryOperator(String textRepresentation) {
                this.textRepresentation = textRepresentation;
            }

            @Override
            public String toString() {
                return getTextRepresentation();
            }

            public String getTextRepresentation() {
                return textRepresentation;
            }

            public static PostfixUnaryOperator getRandomOperator() {
                return Randomly.fromOptions(values());
            }

            public abstract TursoConstant apply(TursoConstant expectedValue);

        }

        private final PostfixUnaryOperator operation;
        private final TursoExpression expression;

        public TursoPostfixUnaryOperation(PostfixUnaryOperator operation, TursoExpression expression) {
            this.operation = operation;
            this.expression = expression;
        }

        public PostfixUnaryOperator getOperation() {
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
            }
            return operation.apply(expression.getExpectedValue());
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
            return OperatorKind.POSTFIX;
        }

    }

    public static class InOperation extends TursoExpression {

        private final TursoExpression left;
        private List<TursoExpression> rightExpressionList;
        private TursoExpression rightSelect;

        public InOperation(TursoExpression left, List<TursoExpression> right) {
            this.left = left;
            this.rightExpressionList = right;
        }

        public InOperation(TursoExpression left, TursoExpression select) {
            this.left = left;
            this.rightSelect = select;
        }

        public TursoExpression getLeft() {
            return left;
        }

        public List<TursoExpression> getRightExpressionList() {
            return rightExpressionList;
        }

        public TursoExpression getRightSelect() {
            return rightSelect;
        }

        @Override
        // The collating sequence used for expressions of the form "x IN (y, z, ...)" is
        // the collating sequence of x.
        public TursoCollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return null;
            }
        }

        @Override
        public TursoConstant getExpectedValue() {
            // TODO query as right hand side is not implemented
            if (left.getExpectedValue() == null) {
                return null;
            }
            if (rightExpressionList.isEmpty()) {
                return TursoConstant.createFalse();
            } else if (left.getExpectedValue().isNull()) {
                return TursoConstant.createNullConstant();
            } else {
                boolean containsNull = false;
                for (TursoExpression expr : getRightExpressionList()) {
                    if (expr.getExpectedValue() == null) {
                        return null; // TODO: we can still compute something if the value is already contained
                    }
                    TursoCollateSequence collate = getExplicitCollateSequence();
                    if (collate == null) {
                        collate = left.getImplicitCollateSequence();
                    }
                    if (collate == null) {
                        collate = TursoCollateSequence.BINARY;
                    }
                    ConstantTuple convertedConstants = applyAffinities(left.getAffinity(), TypeAffinity.NONE,
                            left.getExpectedValue(), expr.getExpectedValue());
                    TursoConstant equals = left.getExpectedValue().applyEquals(convertedConstants.right, collate);
                    Optional<Boolean> isEquals = TursoCast.isTrue(equals);
                    if (isEquals.isPresent() && isEquals.get()) {
                        return TursoConstant.createTrue();
                    } else if (!isEquals.isPresent()) {
                        containsNull = true;
                    }
                }
                if (containsNull) {
                    return TursoConstant.createNullConstant();
                } else {
                    return TursoConstant.createFalse();
                }
            }
        }
    }

    public static class MatchOperation extends TursoExpression {

        private final TursoExpression left;
        private final TursoExpression right;

        public MatchOperation(TursoExpression left, TursoExpression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public TursoExpression getLeft() {
            return left;
        }

        public TursoExpression getRight() {
            return right;
        }

    }

    public static class BinaryComparisonOperation extends TursoExpression
            implements BinaryOperation<TursoExpression> {

        private final BinaryComparisonOperator operation;
        private final TursoExpression left;
        private final TursoExpression right;

        public BinaryComparisonOperation(TursoExpression left, TursoExpression right,
                BinaryComparisonOperator operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public BinaryComparisonOperator getOperator() {
            return operation;
        }

        @Override
        public TursoExpression getLeft() {
            return left;
        }

        @Override
        public TursoExpression getRight() {
            return right;
        }

        @Override
        public TursoConstant getExpectedValue() {
            TursoConstant leftExpected = left.getExpectedValue();
            TursoConstant rightExpected = right.getExpectedValue();
            if (leftExpected == null || rightExpected == null) {
                return null;
            }
            TypeAffinity leftAffinity = left.getAffinity();
            TypeAffinity rightAffinity = right.getAffinity();
            return operation.applyOperand(leftExpected, leftAffinity, rightExpected, rightAffinity, left, right,
                    operation.shouldApplyAffinity());
        }

        public static BinaryComparisonOperation create(TursoExpression leftVal, TursoExpression rightVal,
                BinaryComparisonOperator op) {
            return new BinaryComparisonOperation(leftVal, rightVal, op);
        }

        public enum BinaryComparisonOperator {
            SMALLER("<") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    return left.applyLess(right, collate);
                }

            },
            SMALLER_EQUALS("<=") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    TursoConstant lessThan = left.applyLess(right, collate);
                    if (lessThan == null) {
                        return null;
                    }
                    if (lessThan.getDataType() == TursoDataType.INT && lessThan.asInt() == 0) {
                        return left.applyEquals(right, collate);
                    } else {
                        return lessThan;
                    }
                }

            },
            GREATER(">") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    TursoConstant equals = left.applyEquals(right, collate);
                    if (equals == null) {
                        return null;
                    }
                    if (equals.getDataType() == TursoDataType.INT && equals.asInt() == 1) {
                        return TursoConstant.createFalse();
                    } else {
                        TursoConstant applyLess = left.applyLess(right, collate);
                        if (applyLess == null) {
                            return null;
                        }
                        return UnaryOperator.NOT.apply(applyLess);
                    }
                }

            },
            GREATER_EQUALS(">=") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    TursoConstant equals = left.applyEquals(right, collate);
                    if (equals == null) {
                        return null;
                    }
                    if (equals.getDataType() == TursoDataType.INT && equals.asInt() == 1) {
                        return TursoConstant.createTrue();
                    } else {
                        TursoConstant applyLess = left.applyLess(right, collate);
                        if (applyLess == null) {
                            return null;
                        }
                        return UnaryOperator.NOT.apply(applyLess);
                    }
                }

            },
            EQUALS("=", "==") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    return left.applyEquals(right, collate);
                }

            },
            NOT_EQUALS("!=", "<>") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return TursoConstant.createNullConstant();
                    } else {
                        TursoConstant applyEquals = left.applyEquals(right, collate);
                        if (applyEquals == null) {
                            return null;
                        }
                        boolean equals = applyEquals.asInt() == 1;
                        return TursoConstant.createBoolean(!equals);
                    }
                }

            },
            IS("IS") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    } else if (left.isNull()) {
                        return TursoConstant.createBoolean(right.isNull());
                    } else if (right.isNull()) {
                        return TursoConstant.createFalse();
                    } else {
                        return left.applyEquals(right, collate);
                    }
                }

            },
            IS_NOT("IS NOT") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    } else if (left.isNull()) {
                        return TursoConstant.createBoolean(!right.isNull());
                    } else if (right.isNull()) {
                        return TursoConstant.createTrue();
                    } else {
                        TursoConstant applyEquals = left.applyEquals(right, collate);
                        if (applyEquals == null) {
                            return null;
                        }
                        boolean equals = applyEquals.asInt() == 1;
                        return TursoConstant.createBoolean(!equals);
                    }
                }

            },
            LIKE("LIKE") {
                @Override
                public boolean shouldApplyAffinity() {
                    return false;
                }

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return TursoConstant.createNullConstant();
                    }
                    TursoConstant leftStr = TursoCast.castToText(left);
                    TursoConstant rightStr = TursoCast.castToText(right);
                    if (leftStr == null || rightStr == null) {
                        return null;
                    }
                    boolean val = LikeImplementationHelper.match(leftStr.asString(), rightStr.asString(), 0, 0, false);
                    return TursoConstant.createBoolean(val);
                }

            },
            GLOB("GLOB") {

                @Override
                public boolean shouldApplyAffinity() {
                    return false;
                }

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return TursoConstant.createNullConstant();
                    }
                    TursoConstant leftStr = TursoCast.castToText(left);
                    TursoConstant rightStr = TursoCast.castToText(right);
                    if (leftStr == null || rightStr == null) {
                        return null;
                    }
                    boolean val = match(leftStr.asString(), rightStr.asString(), 0, 0);
                    return TursoConstant.createBoolean(val);
                }

                private boolean match(String str, String regex, int regexPosition, int strPosition) {
                    if (strPosition == str.length() && regexPosition == regex.length()) {
                        return true;
                    }
                    if (regexPosition >= regex.length()) {
                        return false;
                    }
                    char cur = regex.charAt(regexPosition);
                    if (strPosition >= str.length()) {
                        if (cur == '*') {
                            return match(str, regex, regexPosition + 1, strPosition);
                        } else {
                            return false;
                        }
                    }
                    switch (cur) {
                    case '[':
                        int endingBrackets = regexPosition;
                        do {
                            endingBrackets++;
                            if (endingBrackets >= regex.length()) {
                                return false;
                            }
                        } while (regex.charAt(endingBrackets) != ']');
                        StringBuilder patternInBrackets = new StringBuilder(
                                regex.substring(regexPosition + 1, endingBrackets));
                        boolean inverted;
                        if (patternInBrackets.toString().startsWith("^")) {
                            if (patternInBrackets.length() > 1) {
                                inverted = true;
                                patternInBrackets = new StringBuilder(patternInBrackets.substring(1));
                            } else {
                                return false;
                            }
                        } else {
                            inverted = false;
                        }
                        int currentSearchIndex = 0;
                        boolean found = false;
                        do {
                            int minusPosition = patternInBrackets.toString().indexOf('-', currentSearchIndex);
                            boolean minusAtBoundaries = minusPosition == 0
                                    || minusPosition == patternInBrackets.length() - 1;
                            if (minusPosition == -1 || minusAtBoundaries) {
                                break;
                            }
                            found = true;
                            StringBuilder expandedPattern = new StringBuilder();
                            for (char start = patternInBrackets.charAt(minusPosition - 1); start < patternInBrackets
                                    .charAt(minusPosition + 1); start += 1) {
                                expandedPattern.append(start);
                            }
                            patternInBrackets.replace(minusPosition, minusPosition + 1, expandedPattern.toString());
                            currentSearchIndex = minusPosition + expandedPattern.length();
                        } while (found);

                        if (patternInBrackets.length() > 0) {
                            char textChar = str.charAt(strPosition);
                            boolean contains = patternInBrackets.toString().contains(Character.toString(textChar));
                            if (contains && !inverted || !contains && inverted) {
                                return match(str, regex, endingBrackets + 1, strPosition + 1);
                            } else {
                                return false;
                            }
                        } else {
                            return false;
                        }

                    case '*':
                        // match
                        boolean foundMatch = match(str, regex, regexPosition, strPosition + 1);
                        if (!foundMatch) {
                            return match(str, regex, regexPosition + 1, strPosition);
                        } else {
                            return true;
                        }
                    case '?':
                        return match(str, regex, regexPosition + 1, strPosition + 1);
                    default:
                        if (cur == str.charAt(strPosition)) {
                            return match(str, regex, regexPosition + 1, strPosition + 1);
                        } else {
                            return false;
                        }
                    }
                }

            };

            private final String[] textRepresentation;

            TursoConstant apply(TursoConstant left, TursoConstant right, TursoCollateSequence collate) {
                return null;
            }

            public boolean shouldApplyAffinity() {
                return true;
            }

            BinaryComparisonOperator(String... textRepresentation) {
                this.textRepresentation = textRepresentation.clone();
            }

            public static BinaryComparisonOperator getRandomOperator() {
                return Randomly.fromOptions(values());
            }

            public static BinaryComparisonOperator getRandomRowValueOperator() {
                return Randomly.fromOptions(SMALLER, SMALLER_EQUALS, GREATER, GREATER_EQUALS, EQUALS, NOT_EQUALS);
            }

            public String getTextRepresentation() {
                return Randomly.fromOptions(textRepresentation);
            }

            public TursoConstant applyOperand(TursoConstant leftBeforeAffinity, TypeAffinity leftAffinity,
                    TursoConstant rightBeforeAffinity, TypeAffinity rightAffinity, TursoExpression origLeft,
                    TursoExpression origRight, boolean applyAffinity) {

                TursoConstant left;
                TursoConstant right;
                if (applyAffinity) {
                    ConstantTuple vals = applyAffinities(leftAffinity, rightAffinity, leftBeforeAffinity,
                            rightBeforeAffinity);
                    left = vals.left;
                    right = vals.right;
                } else {
                    left = leftBeforeAffinity;
                    right = rightBeforeAffinity;
                }

                // If either operand has an explicit collating function assignment using the
                // postfix COLLATE operator, then the explicit collating function is used for
                // comparison, with precedence to the collating function of the left operand.
                TursoCollateSequence seq = origLeft.getExplicitCollateSequence();
                if (seq == null) {
                    seq = origRight.getExplicitCollateSequence();
                }
                // If either operand is a column, then the collating function of that column is
                // used with precedence to the left operand. For the purposes of the previous
                // sentence, a column name preceded by one or more unary "+" operators is still
                // considered a column name.
                if (seq == null) {
                    seq = origLeft.getImplicitCollateSequence();
                }
                if (seq == null) {
                    seq = origRight.getImplicitCollateSequence();
                }
                // Otherwise, the BINARY collating function is used for comparison.
                if (seq == null) {
                    seq = TursoCollateSequence.BINARY;
                }
                return apply(left, right, seq);
            }

        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        @Override
        public String getOperatorRepresentation() {
            return operation.getTextRepresentation();
        }

    }

    public static class TursoBinaryOperation extends TursoExpression implements BinaryOperation<TursoExpression> {

        public enum BinaryOperator {
            CONCATENATE("||") {
                @Override
                public TursoConstant apply(TursoConstant left, TursoConstant right) {
                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    }
                    if (!TursoProvider.allowFloatingPointFp && (left.getDataType() == TursoDataType.REAL
                            || right.getDataType() == TursoDataType.REAL)) {
                        throw new IgnoreMeException();
                    }
                    if (left.getExpectedValue().isNull() || right.getExpectedValue().isNull()) {
                        return TursoConstant.createNullConstant();
                    }
                    TursoConstant leftText = TursoCast.castToText(left);
                    TursoConstant rightText = TursoCast.castToText(right);
                    if (leftText == null || rightText == null) {
                        return null;
                    }
                    return TursoConstant.createTextConstant(leftText.asString() + rightText.asString());
                }
            },
            MULTIPLY("*") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return null;
                }

            },
            DIVIDE("/") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return null;
                }

            }, // division by zero results in zero
            REMAINDER("%") {
                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return null;
                }

            },

            PLUS("+") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return null;
                }
            },

            MINUS("-") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return null;
                }

            },
            SHIFT_LEFT("<<") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return applyIntOperation(left, right, (leftResult, rightResult) -> {
                        if (rightResult >= 0) {
                            if (rightResult >= Long.SIZE) {
                                return 0L;
                            }
                            return leftResult << rightResult;
                        } else {
                            if (rightResult == Long.MIN_VALUE) {
                                return leftResult >= 0 ? 0L : -1L;
                            }
                            return SHIFT_RIGHT.apply(left, TursoConstant.createIntConstant(-rightResult)).asInt();
                        }

                    });
                }

            },
            SHIFT_RIGHT(">>") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return applyIntOperation(left, right, (leftResult, rightResult) -> {
                        if (rightResult >= 0) {
                            if (rightResult >= Long.SIZE) {
                                return leftResult >= 0 ? 0L : -1L;
                            }
                            return leftResult >> rightResult;
                        } else {
                            if (rightResult == Long.MIN_VALUE) {
                                return 0L;
                            }
                            return SHIFT_LEFT.apply(left, TursoConstant.createIntConstant(-rightResult)).asInt();
                        }

                    });
                }

            },
            ARITHMETIC_AND("&") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return applyIntOperation(left, right, (a, b) -> a & b);
                }

            },
            ARITHMETIC_OR("|") {

                @Override
                TursoConstant apply(TursoConstant left, TursoConstant right) {
                    return applyIntOperation(left, right, (a, b) -> a | b);
                }

            },
            AND("AND") {

                @Override
                public TursoConstant apply(TursoConstant left, TursoConstant right) {

                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    } else {
                        Optional<Boolean> leftBoolVal = TursoCast.isTrue(left.getExpectedValue());
                        Optional<Boolean> rightBoolVal = TursoCast.isTrue(right.getExpectedValue());
                        if (leftBoolVal.isPresent() && !leftBoolVal.get()) {
                            return TursoConstant.createFalse();
                        } else if (rightBoolVal.isPresent() && !rightBoolVal.get()) {
                            return TursoConstant.createFalse();
                        } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                            return TursoConstant.createNullConstant();
                        } else {
                            return TursoConstant.createTrue();
                        }
                    }
                }

            },
            OR("OR") {

                @Override
                public TursoConstant apply(TursoConstant left, TursoConstant right) {
                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    } else {
                        Optional<Boolean> leftBoolVal = TursoCast.isTrue(left.getExpectedValue());
                        Optional<Boolean> rightBoolVal = TursoCast.isTrue(right.getExpectedValue());
                        if (leftBoolVal.isPresent() && leftBoolVal.get()) {
                            return TursoConstant.createTrue();
                        } else if (rightBoolVal.isPresent() && rightBoolVal.get()) {
                            return TursoConstant.createTrue();
                        } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                            return TursoConstant.createNullConstant();
                        } else {
                            return TursoConstant.createFalse();
                        }
                    }
                }
            };

            private final String[] textRepresentation;

            BinaryOperator(String... textRepresentation) {
                this.textRepresentation = textRepresentation.clone();
            }

            public static BinaryOperator getRandomOperator() {
                return Randomly.fromOptions(values());
            }

            public String getTextRepresentation() {
                return Randomly.fromOptions(textRepresentation);
            }

            public TursoConstant applyOperand(TursoConstant left, TypeAffinity leftAffinity, TursoConstant right,
                    TypeAffinity rightAffinity) {
                return apply(left, right);
            }

            public TursoConstant applyIntOperation(TursoConstant left, TursoConstant right,
                    java.util.function.BinaryOperator<Long> func) {
                if (left.isNull() || right.isNull()) {
                    return TursoConstant.createNullConstant();
                }
                TursoConstant leftInt = TursoCast.castToInt(left);
                TursoConstant rightInt = TursoCast.castToInt(right);
                long result = func.apply(leftInt.asInt(), rightInt.asInt());
                return TursoConstant.createIntConstant(result);
            }

            TursoConstant apply(TursoConstant left, TursoConstant right) {
                return null;
            }

        }

        private final BinaryOperator operation;
        private final TursoExpression left;
        private final TursoExpression right;

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        public TursoBinaryOperation(TursoExpression left, TursoExpression right, BinaryOperator operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public BinaryOperator getOperator() {
            return operation;
        }

        @Override
        public TursoExpression getLeft() {
            return left;
        }

        @Override
        public TursoExpression getRight() {
            return right;
        }

        @Override
        public TursoConstant getExpectedValue() {
            if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                return null;
            }
            TursoConstant result = operation.applyOperand(left.getExpectedValue(), left.getAffinity(),
                    right.getExpectedValue(), right.getAffinity());
            if (result != null && result.isReal()) {
                TursoCast.checkDoubleIsInsideDangerousRange(result.asDouble());
            }
            return result;
        }

        public static TursoBinaryOperation create(TursoExpression leftVal, TursoExpression rightVal,
                BinaryOperator op) {
            return new TursoBinaryOperation(leftVal, rightVal, op);
        }

        @Override
        public String getOperatorRepresentation() {
            return Randomly.fromOptions(operation.textRepresentation);
        }

    }

    public static class TursoColumnName extends TursoExpression {

        private final TursoColumn column;
        private final TursoConstant value;

        public TursoColumnName(TursoColumn name, TursoConstant value) {
            this.column = name;
            this.value = value;
        }

        public TursoColumn getColumn() {
            return column;
        }

        @Override
        public TursoConstant getExpectedValue() {
            return value;
        }

        /*
         * When an expression is a simple reference to a column of a real table (not a VIEW or subquery) then the
         * expression has the same affinity as the table column.
         */
        @Override
        public TypeAffinity getAffinity() {
            switch (column.getType()) {
            case BINARY:
                return TypeAffinity.BLOB;
            case INT:
                return TypeAffinity.INTEGER;
            case NONE:
                return TypeAffinity.NONE;
            case REAL:
                return TypeAffinity.REAL;
            case TEXT:
                return TypeAffinity.TEXT;
            default:
                throw new AssertionError(column);
            }
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public TursoCollateSequence getImplicitCollateSequence() {
            return column.getCollateSequence();
        }

        public static TursoColumnName createDummy(String string) {
            return new TursoColumnName(TursoColumn.createDummy(string), null);
        }

    }

    static class ConstantTuple {
        TursoConstant left;
        TursoConstant right;

        ConstantTuple(TursoConstant left, TursoConstant right) {
            this.left = left;
            this.right = right;
        }

    }

    public static ConstantTuple applyAffinities(TypeAffinity leftAffinity, TypeAffinity rightAffinity,
            TursoConstant leftBeforeAffinity, TursoConstant rightBeforeAffinity) {
        // If one operand has INTEGER, REAL or NUMERIC affinity and the other operand
        // has TEXT or BLOB or no affinity then NUMERIC affinity is applied to other
        // operand.
        TursoConstant left = leftBeforeAffinity;
        TursoConstant right = rightBeforeAffinity;
        if (leftAffinity.isNumeric() && (rightAffinity == TypeAffinity.TEXT || rightAffinity == TypeAffinity.BLOB
                || rightAffinity == TypeAffinity.NONE)) {
            right = right.applyNumericAffinity();
            assert right != null;
        } else if (rightAffinity.isNumeric() && (leftAffinity == TypeAffinity.TEXT || leftAffinity == TypeAffinity.BLOB
                || leftAffinity == TypeAffinity.NONE)) {
            left = left.applyNumericAffinity();
            assert left != null;
        }

        // If one operand has TEXT affinity and the other has no affinity, then TEXT
        // affinity is applied to the other operand.
        if (leftAffinity == TypeAffinity.TEXT && rightAffinity == TypeAffinity.NONE) {
            right = right.applyTextAffinity();
            if (right == null) {
                throw new IgnoreMeException();
            }
        } else if (rightAffinity == TypeAffinity.TEXT && leftAffinity == TypeAffinity.NONE) {
            left = left.applyTextAffinity();
            if (left == null) {
                throw new IgnoreMeException();
            }
        }
        return new ConstantTuple(left, right);
    }

    public static class TursoText extends TursoExpression {

        private final String text;
        private final TursoConstant expectedValue;

        public TursoText(String text, TursoConstant expectedValue) {
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public String getText() {
            return text;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public TursoConstant getExpectedValue() {
            return expectedValue;
        }

    }

    public static class TursoPostfixText extends TursoExpression implements UnaryOperation<TursoExpression> {

        private final TursoExpression expr;
        private final String text;
        private TursoConstant expectedValue;

        public TursoPostfixText(TursoExpression expr, String text, TursoConstant expectedValue) {
            this.expr = expr;
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public TursoPostfixText(String text, TursoConstant expectedValue) {
            this(null, text, expectedValue);
        }

        public String getText() {
            return text;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            if (expr == null) {
                return null;
            } else {
                return expr.getExplicitCollateSequence();
            }
        }

        @Override
        public TursoConstant getExpectedValue() {
            return expectedValue;
        }

        @Override
        public TursoExpression getExpression() {
            return expr;
        }

        @Override
        public String getOperatorRepresentation() {
            return getText();
        }

        @Override
        public OperatorKind getOperatorKind() {
            return OperatorKind.POSTFIX;
        }

        @Override
        public boolean omitBracketsWhenPrinting() {
            return true;
        }
    }

    public static class TursoWithClause extends TursoExpression {

        private final TursoExpression left;
        private TursoExpression right;

        public TursoWithClause(TursoExpression left, TursoExpression right) {
            this.left = left;
            this.right = right;
        }

        public TursoExpression getLeft() {
            return this.left;
        }

        public TursoExpression getRight() {
            return this.right;
        }

        public void updateRight(TursoExpression right) {
            this.right = right;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }
    }

    public static class TursoAlias extends TursoExpression {

        private final TursoExpression originalExpression;
        private final TursoExpression aliasExpression;

        public TursoAlias(TursoExpression originalExpression, TursoExpression aliasExpression) {
            this.originalExpression = originalExpression;
            this.aliasExpression = aliasExpression;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public TursoExpression getOriginalExpression() {
            return originalExpression;
        }

        public TursoExpression getAliasExpression() {
            return aliasExpression;
        }
    }

    public static class TursoTableAndColumnRef extends TursoExpression {

        private final TursoTable table;

        public TursoTableAndColumnRef(TursoTable table) {
            this.table = table;
        }

        public TursoTable getTable() {
            return this.table;
        }

        public String getString() {
            StringBuilder sb = new StringBuilder();
            sb.append(table.getName());
            sb.append("(");
            Boolean isFirstColumn = true;
            for (TursoColumn c : this.table.getColumns()) {
                if (!isFirstColumn) {
                    sb.append(", ");
                }
                sb.append(c.getName());
                isFirstColumn = false;
            }
            sb.append(")");
            return sb.toString();
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }
    }

    public static class TursoValues extends TursoExpression {

        private final Map<String, List<TursoConstant>> values;
        private final List<TursoColumn> columns;

        public TursoValues(Map<String, List<TursoConstant>> values, List<TursoColumn> columns) {
            this.values = values;
            this.columns = columns;
        }

        public Map<String, List<TursoConstant>> getValues() {
            return this.values;
        }

        public List<TursoColumn> getColumns() {
            return this.columns;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }
    }

    // The ExpressionBag is not a built-in SQL feature,
    // but rather a utility class used in CODDTest's oracle construction
    // to substitute expressions with their corresponding constant values.
    public static class TursoExpressionBag extends TursoExpression {
        private TursoExpression innerExpr;

        public TursoExpressionBag(TursoExpression innerExpr) {
            this.innerExpr = innerExpr;
        }

        public void updateInnerExpr(TursoExpression innerExpr) {
            this.innerExpr = innerExpr;
        }

        public TursoExpression getInnerExpr() {
            return innerExpr;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class TursoTypeof extends TursoExpression {
        private final TursoExpression innerExpr;

        public TursoTypeof(TursoExpression innerExpr) {
            this.innerExpr = innerExpr;
        }

        public TursoExpression getInnerExpr() {
            return innerExpr;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class TursoResultMap extends TursoExpression {
        private final TursoValues values;
        private final List<TursoColumnName> columns;
        private final List<TursoConstant> summary;
        private final TursoDataType summaryDataType;

        public TursoResultMap(TursoValues values, List<TursoColumnName> columns, List<TursoConstant> summary,
                TursoDataType summaryDataType) {
            this.values = values;
            this.columns = columns;
            this.summary = summary;
            this.summaryDataType = summaryDataType;

            Map<String, List<TursoConstant>> vs = values.getValues();
            if (vs.get(vs.keySet().iterator().next()).size() != summary.size()) {
                throw new AssertionError();
            }
        }

        public TursoValues getValues() {
            return this.values;
        }

        public List<TursoColumnName> getColumns() {
            return this.columns;
        }

        public List<TursoConstant> getSummary() {
            return this.summary;
        }

        public TursoDataType getSummaryDataType() {
            return this.summaryDataType;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }
}
