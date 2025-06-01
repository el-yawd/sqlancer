package sqlancer.limbo.ast;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import sqlancer.IgnoreMeException;
import sqlancer.LikeImplementationHelper;
import sqlancer.Randomly;
import sqlancer.common.ast.newast.Expression;
import sqlancer.common.visitor.BinaryOperation;
import sqlancer.common.visitor.UnaryOperation;
import sqlancer.limbo.LimboCollateHelper;
import sqlancer.limbo.LimboProvider;
import sqlancer.limbo.ast.LimboExpression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.limbo.ast.LimboExpression.LimboBinaryOperation.BinaryOperator;
import sqlancer.limbo.ast.LimboUnaryOperation.UnaryOperator;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public abstract class LimboExpression implements Expression<LimboColumn> {

    public static class LimboTableReference extends LimboExpression {

        private final String indexedBy;
        private final LimboTable table;

        public LimboTableReference(String indexedBy, LimboTable table) {
            this.indexedBy = indexedBy;
            this.table = table;
        }

        public LimboTableReference(LimboTable table) {
            this.indexedBy = null;
            this.table = table;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public LimboTable getTable() {
            return table;
        }

        public String getIndexedBy() {
            return indexedBy;
        }

    }

    public static class LimboDistinct extends LimboExpression {

        private final LimboExpression expr;

        public LimboDistinct(LimboExpression expr) {
            this.expr = expr;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return expr.getExplicitCollateSequence();
        }

        @Override
        public LimboConstant getExpectedValue() {
            return expr.getExpectedValue();
        }

        public LimboExpression getExpression() {
            return expr;
        }

        @Override
        public LimboCollateSequence getImplicitCollateSequence() {
            // https://www.sqlite.org/src/tktview/18ab5da2c05ad57d7f9d79c41d3138b141378543
            return expr.getImplicitCollateSequence();
        }

    }

    public LimboConstant getExpectedValue() {
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
    public abstract LimboCollateSequence getExplicitCollateSequence();

    public LimboCollateSequence getImplicitCollateSequence() {
        return null;
    }

    public static class LimboExist extends LimboExpression {

        private final LimboExpression select;
        private boolean negated;

        public LimboExist(LimboExpression select, boolean negated) {
            this.select = select;
            this.negated = negated;
        }

        public void setNegated(boolean negated) {
            this.negated = negated;
        }

        public boolean getNegated() {
            return this.negated;
        }

        public LimboExpression getExpression() {
            return select;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class Join extends LimboExpression
            implements sqlancer.common.ast.newast.Join<LimboExpression, LimboTable, LimboColumn> {

        public enum JoinType {
            INNER, CROSS, OUTER, NATURAL, RIGHT, FULL;
        }

        private final LimboTable table;
        private LimboExpression onClause;
        private JoinType type;

        public Join(Join other) {
            this.table = other.table;
            this.onClause = other.onClause;
            this.type = other.type;
        }

        public Join(LimboTable table, LimboExpression onClause, JoinType type) {
            this.table = table;
            this.onClause = onClause;
            this.type = type;
        }

        public Join(LimboTable table, JoinType type) {
            this.table = table;
            if (type != JoinType.NATURAL) {
                throw new AssertionError();
            }
            this.onClause = null;
            this.type = type;
        }

        public LimboTable getTable() {
            return table;
        }

        public LimboExpression getOnClause() {
            return onClause;
        }

        public JoinType getType() {
            return type;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public void setOnClause(LimboExpression onClause) {
            this.onClause = onClause;
        }

        public void setType(JoinType type) {
            this.type = type;
        }
    }

    public static class Subquery extends LimboExpression {

        private final String query;

        public Subquery(String query) {
            this.query = query;
        }

        public static LimboExpression create(String query) {
            return new Subquery(query);
        }

        public String getQuery() {
            return query;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class TypeLiteral {

        private final Type type;

        public enum Type {
            TEXT {
                @Override
                public LimboConstant apply(LimboConstant cons) {
                    return LimboCast.castToText(cons);
                }
            },
            REAL {
                @Override
                public LimboConstant apply(LimboConstant cons) {
                    return LimboCast.castToReal(cons);
                }
            },
            INTEGER {
                @Override
                public LimboConstant apply(LimboConstant cons) {
                    return LimboCast.castToInt(cons);
                }
            },
            NUMERIC {
                @Override
                public LimboConstant apply(LimboConstant cons) {
                    return LimboCast.castToNumeric(cons);
                }
            },
            BLOB {
                @Override
                public LimboConstant apply(LimboConstant cons) {
                    return LimboCast.castToBlob(cons);
                }
            };

            public abstract LimboConstant apply(LimboConstant cons);
        }

        public TypeLiteral(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }

    }

    public static class Cast extends LimboExpression {

        private final TypeLiteral type;
        private final LimboExpression expression;

        public Cast(TypeLiteral typeofExpr, LimboExpression expression) {
            this.type = typeofExpr;
            this.expression = expression;
        }

        public LimboExpression getExpression() {
            return expression;
        }

        public TypeLiteral getType() {
            return type;
        }

        @Override
        public LimboConstant getExpectedValue() {
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
        public LimboCollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

        @Override
        public LimboCollateSequence getImplicitCollateSequence() {
            if (LimboCollateHelper.shouldGetSubexpressionAffinity(expression)) {
                return expression.getImplicitCollateSequence();
            } else {
                return null;
            }
        }

    }

    public static class BetweenOperation extends LimboExpression {

        private final LimboExpression expr;
        private final boolean negated;
        private final LimboExpression left;
        private final LimboExpression right;

        public BetweenOperation(LimboExpression expr, boolean negated, LimboExpression left,
                LimboExpression right) {
            this.expr = expr;
            this.negated = negated;
            this.left = left;
            this.right = right;
        }

        public LimboExpression getExpression() {
            return expr;
        }

        public boolean isNegated() {
            return negated;
        }

        public LimboExpression getLeft() {
            return left;
        }

        public LimboExpression getRight() {
            return right;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            if (expr.getExplicitCollateSequence() != null) {
                return expr.getExplicitCollateSequence();
            } else if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        @Override
        public LimboConstant getExpectedValue() {
            return getTopNode().getExpectedValue();
        }

        public LimboExpression getTopNode() {
            BinaryComparisonOperation leftOp = new BinaryComparisonOperation(expr, left,
                    BinaryComparisonOperator.GREATER_EQUALS);
            BinaryComparisonOperation rightOp = new BinaryComparisonOperation(expr, right,
                    BinaryComparisonOperator.SMALLER_EQUALS);
            LimboBinaryOperation and = new LimboBinaryOperation(leftOp, rightOp, BinaryOperator.AND);
            if (negated) {
                return new LimboUnaryOperation(UnaryOperator.NOT, and);
            } else {
                return and;
            }
        }

    }

    public static class Function extends LimboExpression {

        private final LimboExpression[] arguments;
        private final String name;

        public Function(String name, LimboExpression... arguments) {
            this.name = name;
            this.arguments = arguments.clone();
        }

        public LimboExpression[] getArguments() {
            return arguments.clone();
        }

        public String getName() {
            return name;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            for (LimboExpression arg : arguments) {
                if (arg.getExplicitCollateSequence() != null) {
                    return arg.getExplicitCollateSequence();
                }
            }
            return null;
        }

    }

    public static class LimboOrderingTerm extends LimboExpression {

        private final LimboExpression expression;
        private final Ordering ordering;

        public enum Ordering {
            ASC, DESC;

            public static Ordering getRandomValue() {
                return Randomly.fromOptions(Ordering.values());
            }
        }

        public LimboOrderingTerm(LimboExpression expression, Ordering ordering) {
            this.expression = expression;
            this.ordering = ordering;
        }

        public LimboExpression getExpression() {
            return expression;
        }

        public Ordering getOrdering() {
            return ordering;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return expression.getExplicitCollateSequence();
        }

    }

    public static class CollateOperation extends LimboExpression {

        private final LimboExpression expression;
        private final LimboCollateSequence collate;

        public CollateOperation(LimboExpression expression, LimboCollateSequence collate) {
            this.expression = expression;
            this.collate = collate;
        }

        public LimboCollateSequence getCollate() {
            return collate;
        }

        public LimboExpression getExpression() {
            return expression;
        }

        // If either operand has an explicit collating function assignment using the
        // postfix COLLATE operator, then the explicit collating function is used for
        // comparison, with precedence to the collating function of the left operand.
        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return collate;
        }

        @Override
        public LimboConstant getExpectedValue() {
            return expression.getExpectedValue();
        }

        @Override
        public TypeAffinity getAffinity() {
            return expression.getAffinity();
        }

    }

    public static class LimboPostfixUnaryOperation extends LimboExpression
            implements UnaryOperation<LimboExpression> {

        public enum PostfixUnaryOperator {
            ISNULL("ISNULL") {
                @Override
                public LimboConstant apply(LimboConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return LimboConstant.createTrue();
                    } else {
                        return LimboConstant.createFalse();
                    }
                }
            },
            NOT_NULL("NOT NULL") {
                @Override
                public LimboConstant apply(LimboConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return LimboConstant.createFalse();
                    } else {
                        return LimboConstant.createTrue();
                    }
                }

            },

            NOTNULL("NOTNULL") {

                @Override
                public LimboConstant apply(LimboConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return LimboConstant.createFalse();
                    } else {
                        return LimboConstant.createTrue();
                    }
                }

            },
            IS_TRUE("IS TRUE") {

                @Override
                public LimboConstant apply(LimboConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return LimboConstant.createIntConstant(0);
                    }
                    return LimboCast.asBoolean(expectedValue);
                }
            },
            IS_FALSE("IS FALSE") {

                @Override
                public LimboConstant apply(LimboConstant expectedValue) {
                    if (expectedValue.isNull()) {
                        return LimboConstant.createIntConstant(0);
                    }
                    return LimboUnaryOperation.UnaryOperator.NOT.apply(LimboCast.asBoolean(expectedValue));
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

            public abstract LimboConstant apply(LimboConstant expectedValue);

        }

        private final PostfixUnaryOperator operation;
        private final LimboExpression expression;

        public LimboPostfixUnaryOperation(PostfixUnaryOperator operation, LimboExpression expression) {
            this.operation = operation;
            this.expression = expression;
        }

        public PostfixUnaryOperator getOperation() {
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
            }
            return operation.apply(expression.getExpectedValue());
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
            return OperatorKind.POSTFIX;
        }

    }

    public static class InOperation extends LimboExpression {

        private final LimboExpression left;
        private List<LimboExpression> rightExpressionList;
        private LimboExpression rightSelect;

        public InOperation(LimboExpression left, List<LimboExpression> right) {
            this.left = left;
            this.rightExpressionList = right;
        }

        public InOperation(LimboExpression left, LimboExpression select) {
            this.left = left;
            this.rightSelect = select;
        }

        public LimboExpression getLeft() {
            return left;
        }

        public List<LimboExpression> getRightExpressionList() {
            return rightExpressionList;
        }

        public LimboExpression getRightSelect() {
            return rightSelect;
        }

        @Override
        // The collating sequence used for expressions of the form "x IN (y, z, ...)" is
        // the collating sequence of x.
        public LimboCollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return null;
            }
        }

        @Override
        public LimboConstant getExpectedValue() {
            // TODO query as right hand side is not implemented
            if (left.getExpectedValue() == null) {
                return null;
            }
            if (rightExpressionList.isEmpty()) {
                return LimboConstant.createFalse();
            } else if (left.getExpectedValue().isNull()) {
                return LimboConstant.createNullConstant();
            } else {
                boolean containsNull = false;
                for (LimboExpression expr : getRightExpressionList()) {
                    if (expr.getExpectedValue() == null) {
                        return null; // TODO: we can still compute something if the value is already contained
                    }
                    LimboCollateSequence collate = getExplicitCollateSequence();
                    if (collate == null) {
                        collate = left.getImplicitCollateSequence();
                    }
                    if (collate == null) {
                        collate = LimboCollateSequence.BINARY;
                    }
                    ConstantTuple convertedConstants = applyAffinities(left.getAffinity(), TypeAffinity.NONE,
                            left.getExpectedValue(), expr.getExpectedValue());
                    LimboConstant equals = left.getExpectedValue().applyEquals(convertedConstants.right, collate);
                    Optional<Boolean> isEquals = LimboCast.isTrue(equals);
                    if (isEquals.isPresent() && isEquals.get()) {
                        return LimboConstant.createTrue();
                    } else if (!isEquals.isPresent()) {
                        containsNull = true;
                    }
                }
                if (containsNull) {
                    return LimboConstant.createNullConstant();
                } else {
                    return LimboConstant.createFalse();
                }
            }
        }
    }

    public static class MatchOperation extends LimboExpression {

        private final LimboExpression left;
        private final LimboExpression right;

        public MatchOperation(LimboExpression left, LimboExpression right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public LimboExpression getLeft() {
            return left;
        }

        public LimboExpression getRight() {
            return right;
        }

    }

    public static class BinaryComparisonOperation extends LimboExpression
            implements BinaryOperation<LimboExpression> {

        private final BinaryComparisonOperator operation;
        private final LimboExpression left;
        private final LimboExpression right;

        public BinaryComparisonOperation(LimboExpression left, LimboExpression right,
                BinaryComparisonOperator operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public BinaryComparisonOperator getOperator() {
            return operation;
        }

        @Override
        public LimboExpression getLeft() {
            return left;
        }

        @Override
        public LimboExpression getRight() {
            return right;
        }

        @Override
        public LimboConstant getExpectedValue() {
            LimboConstant leftExpected = left.getExpectedValue();
            LimboConstant rightExpected = right.getExpectedValue();
            if (leftExpected == null || rightExpected == null) {
                return null;
            }
            TypeAffinity leftAffinity = left.getAffinity();
            TypeAffinity rightAffinity = right.getAffinity();
            return operation.applyOperand(leftExpected, leftAffinity, rightExpected, rightAffinity, left, right,
                    operation.shouldApplyAffinity());
        }

        public static BinaryComparisonOperation create(LimboExpression leftVal, LimboExpression rightVal,
                BinaryComparisonOperator op) {
            return new BinaryComparisonOperation(leftVal, rightVal, op);
        }

        public enum BinaryComparisonOperator {
            SMALLER("<") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    return left.applyLess(right, collate);
                }

            },
            SMALLER_EQUALS("<=") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    LimboConstant lessThan = left.applyLess(right, collate);
                    if (lessThan == null) {
                        return null;
                    }
                    if (lessThan.getDataType() == LimboDataType.INT && lessThan.asInt() == 0) {
                        return left.applyEquals(right, collate);
                    } else {
                        return lessThan;
                    }
                }

            },
            GREATER(">") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    LimboConstant equals = left.applyEquals(right, collate);
                    if (equals == null) {
                        return null;
                    }
                    if (equals.getDataType() == LimboDataType.INT && equals.asInt() == 1) {
                        return LimboConstant.createFalse();
                    } else {
                        LimboConstant applyLess = left.applyLess(right, collate);
                        if (applyLess == null) {
                            return null;
                        }
                        return UnaryOperator.NOT.apply(applyLess);
                    }
                }

            },
            GREATER_EQUALS(">=") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    LimboConstant equals = left.applyEquals(right, collate);
                    if (equals == null) {
                        return null;
                    }
                    if (equals.getDataType() == LimboDataType.INT && equals.asInt() == 1) {
                        return LimboConstant.createTrue();
                    } else {
                        LimboConstant applyLess = left.applyLess(right, collate);
                        if (applyLess == null) {
                            return null;
                        }
                        return UnaryOperator.NOT.apply(applyLess);
                    }
                }

            },
            EQUALS("=", "==") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    return left.applyEquals(right, collate);
                }

            },
            NOT_EQUALS("!=", "<>") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return LimboConstant.createNullConstant();
                    } else {
                        LimboConstant applyEquals = left.applyEquals(right, collate);
                        if (applyEquals == null) {
                            return null;
                        }
                        boolean equals = applyEquals.asInt() == 1;
                        return LimboConstant.createBoolean(!equals);
                    }
                }

            },
            IS("IS") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    } else if (left.isNull()) {
                        return LimboConstant.createBoolean(right.isNull());
                    } else if (right.isNull()) {
                        return LimboConstant.createFalse();
                    } else {
                        return left.applyEquals(right, collate);
                    }
                }

            },
            IS_NOT("IS NOT") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    } else if (left.isNull()) {
                        return LimboConstant.createBoolean(!right.isNull());
                    } else if (right.isNull()) {
                        return LimboConstant.createTrue();
                    } else {
                        LimboConstant applyEquals = left.applyEquals(right, collate);
                        if (applyEquals == null) {
                            return null;
                        }
                        boolean equals = applyEquals.asInt() == 1;
                        return LimboConstant.createBoolean(!equals);
                    }
                }

            },
            LIKE("LIKE") {
                @Override
                public boolean shouldApplyAffinity() {
                    return false;
                }

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return LimboConstant.createNullConstant();
                    }
                    LimboConstant leftStr = LimboCast.castToText(left);
                    LimboConstant rightStr = LimboCast.castToText(right);
                    if (leftStr == null || rightStr == null) {
                        return null;
                    }
                    boolean val = LikeImplementationHelper.match(leftStr.asString(), rightStr.asString(), 0, 0, false);
                    return LimboConstant.createBoolean(val);
                }

            },
            GLOB("GLOB") {

                @Override
                public boolean shouldApplyAffinity() {
                    return false;
                }

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
                    if (left == null || right == null) {
                        return null;
                    }
                    if (left.isNull() || right.isNull()) {
                        return LimboConstant.createNullConstant();
                    }
                    LimboConstant leftStr = LimboCast.castToText(left);
                    LimboConstant rightStr = LimboCast.castToText(right);
                    if (leftStr == null || rightStr == null) {
                        return null;
                    }
                    boolean val = match(leftStr.asString(), rightStr.asString(), 0, 0);
                    return LimboConstant.createBoolean(val);
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

            LimboConstant apply(LimboConstant left, LimboConstant right, LimboCollateSequence collate) {
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

            public LimboConstant applyOperand(LimboConstant leftBeforeAffinity, TypeAffinity leftAffinity,
                    LimboConstant rightBeforeAffinity, TypeAffinity rightAffinity, LimboExpression origLeft,
                    LimboExpression origRight, boolean applyAffinity) {

                LimboConstant left;
                LimboConstant right;
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
                LimboCollateSequence seq = origLeft.getExplicitCollateSequence();
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
                    seq = LimboCollateSequence.BINARY;
                }
                return apply(left, right, seq);
            }

        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
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

    public static class LimboBinaryOperation extends LimboExpression implements BinaryOperation<LimboExpression> {

        public enum BinaryOperator {
            CONCATENATE("||") {
                @Override
                public LimboConstant apply(LimboConstant left, LimboConstant right) {
                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    }
                    if (!LimboProvider.allowFloatingPointFp && (left.getDataType() == LimboDataType.REAL
                            || right.getDataType() == LimboDataType.REAL)) {
                        throw new IgnoreMeException();
                    }
                    if (left.getExpectedValue().isNull() || right.getExpectedValue().isNull()) {
                        return LimboConstant.createNullConstant();
                    }
                    LimboConstant leftText = LimboCast.castToText(left);
                    LimboConstant rightText = LimboCast.castToText(right);
                    if (leftText == null || rightText == null) {
                        return null;
                    }
                    return LimboConstant.createTextConstant(leftText.asString() + rightText.asString());
                }
            },
            MULTIPLY("*") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return null;
                }

            },
            DIVIDE("/") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return null;
                }

            }, // division by zero results in zero
            REMAINDER("%") {
                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return null;
                }

            },

            PLUS("+") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return null;
                }
            },

            MINUS("-") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return null;
                }

            },
            SHIFT_LEFT("<<") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
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
                            return SHIFT_RIGHT.apply(left, LimboConstant.createIntConstant(-rightResult)).asInt();
                        }

                    });
                }

            },
            SHIFT_RIGHT(">>") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
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
                            return SHIFT_LEFT.apply(left, LimboConstant.createIntConstant(-rightResult)).asInt();
                        }

                    });
                }

            },
            ARITHMETIC_AND("&") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return applyIntOperation(left, right, (a, b) -> a & b);
                }

            },
            ARITHMETIC_OR("|") {

                @Override
                LimboConstant apply(LimboConstant left, LimboConstant right) {
                    return applyIntOperation(left, right, (a, b) -> a | b);
                }

            },
            AND("AND") {

                @Override
                public LimboConstant apply(LimboConstant left, LimboConstant right) {

                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    } else {
                        Optional<Boolean> leftBoolVal = LimboCast.isTrue(left.getExpectedValue());
                        Optional<Boolean> rightBoolVal = LimboCast.isTrue(right.getExpectedValue());
                        if (leftBoolVal.isPresent() && !leftBoolVal.get()) {
                            return LimboConstant.createFalse();
                        } else if (rightBoolVal.isPresent() && !rightBoolVal.get()) {
                            return LimboConstant.createFalse();
                        } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                            return LimboConstant.createNullConstant();
                        } else {
                            return LimboConstant.createTrue();
                        }
                    }
                }

            },
            OR("OR") {

                @Override
                public LimboConstant apply(LimboConstant left, LimboConstant right) {
                    if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                        return null;
                    } else {
                        Optional<Boolean> leftBoolVal = LimboCast.isTrue(left.getExpectedValue());
                        Optional<Boolean> rightBoolVal = LimboCast.isTrue(right.getExpectedValue());
                        if (leftBoolVal.isPresent() && leftBoolVal.get()) {
                            return LimboConstant.createTrue();
                        } else if (rightBoolVal.isPresent() && rightBoolVal.get()) {
                            return LimboConstant.createTrue();
                        } else if (!rightBoolVal.isPresent() || !leftBoolVal.isPresent()) {
                            return LimboConstant.createNullConstant();
                        } else {
                            return LimboConstant.createFalse();
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

            public LimboConstant applyOperand(LimboConstant left, TypeAffinity leftAffinity, LimboConstant right,
                    TypeAffinity rightAffinity) {
                return apply(left, right);
            }

            public LimboConstant applyIntOperation(LimboConstant left, LimboConstant right,
                    java.util.function.BinaryOperator<Long> func) {
                if (left.isNull() || right.isNull()) {
                    return LimboConstant.createNullConstant();
                }
                LimboConstant leftInt = LimboCast.castToInt(left);
                LimboConstant rightInt = LimboCast.castToInt(right);
                long result = func.apply(leftInt.asInt(), rightInt.asInt());
                return LimboConstant.createIntConstant(result);
            }

            LimboConstant apply(LimboConstant left, LimboConstant right) {
                return null;
            }

        }

        private final BinaryOperator operation;
        private final LimboExpression left;
        private final LimboExpression right;

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            if (left.getExplicitCollateSequence() != null) {
                return left.getExplicitCollateSequence();
            } else {
                return right.getExplicitCollateSequence();
            }
        }

        public LimboBinaryOperation(LimboExpression left, LimboExpression right, BinaryOperator operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public BinaryOperator getOperator() {
            return operation;
        }

        @Override
        public LimboExpression getLeft() {
            return left;
        }

        @Override
        public LimboExpression getRight() {
            return right;
        }

        @Override
        public LimboConstant getExpectedValue() {
            if (left.getExpectedValue() == null || right.getExpectedValue() == null) {
                return null;
            }
            LimboConstant result = operation.applyOperand(left.getExpectedValue(), left.getAffinity(),
                    right.getExpectedValue(), right.getAffinity());
            if (result != null && result.isReal()) {
                LimboCast.checkDoubleIsInsideDangerousRange(result.asDouble());
            }
            return result;
        }

        public static LimboBinaryOperation create(LimboExpression leftVal, LimboExpression rightVal,
                BinaryOperator op) {
            return new LimboBinaryOperation(leftVal, rightVal, op);
        }

        @Override
        public String getOperatorRepresentation() {
            return Randomly.fromOptions(operation.textRepresentation);
        }

    }

    public static class LimboColumnName extends LimboExpression {

        private final LimboColumn column;
        private final LimboConstant value;

        public LimboColumnName(LimboColumn name, LimboConstant value) {
            this.column = name;
            this.value = value;
        }

        public LimboColumn getColumn() {
            return column;
        }

        @Override
        public LimboConstant getExpectedValue() {
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
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public LimboCollateSequence getImplicitCollateSequence() {
            return column.getCollateSequence();
        }

        public static LimboColumnName createDummy(String string) {
            return new LimboColumnName(LimboColumn.createDummy(string), null);
        }

    }

    static class ConstantTuple {
        LimboConstant left;
        LimboConstant right;

        ConstantTuple(LimboConstant left, LimboConstant right) {
            this.left = left;
            this.right = right;
        }

    }

    public static ConstantTuple applyAffinities(TypeAffinity leftAffinity, TypeAffinity rightAffinity,
            LimboConstant leftBeforeAffinity, LimboConstant rightBeforeAffinity) {
        // If one operand has INTEGER, REAL or NUMERIC affinity and the other operand
        // has TEXT or BLOB or no affinity then NUMERIC affinity is applied to other
        // operand.
        LimboConstant left = leftBeforeAffinity;
        LimboConstant right = rightBeforeAffinity;
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

    public static class LimboText extends LimboExpression {

        private final String text;
        private final LimboConstant expectedValue;

        public LimboText(String text, LimboConstant expectedValue) {
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public String getText() {
            return text;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        @Override
        public LimboConstant getExpectedValue() {
            return expectedValue;
        }

    }

    public static class LimboPostfixText extends LimboExpression implements UnaryOperation<LimboExpression> {

        private final LimboExpression expr;
        private final String text;
        private LimboConstant expectedValue;

        public LimboPostfixText(LimboExpression expr, String text, LimboConstant expectedValue) {
            this.expr = expr;
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public LimboPostfixText(String text, LimboConstant expectedValue) {
            this(null, text, expectedValue);
        }

        public String getText() {
            return text;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            if (expr == null) {
                return null;
            } else {
                return expr.getExplicitCollateSequence();
            }
        }

        @Override
        public LimboConstant getExpectedValue() {
            return expectedValue;
        }

        @Override
        public LimboExpression getExpression() {
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

    public static class LimboWithClause extends LimboExpression {

        private final LimboExpression left;
        private LimboExpression right;

        public LimboWithClause(LimboExpression left, LimboExpression right) {
            this.left = left;
            this.right = right;
        }

        public LimboExpression getLeft() {
            return this.left;
        }

        public LimboExpression getRight() {
            return this.right;
        }

        public void updateRight(LimboExpression right) {
            this.right = right;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }
    }

    public static class LimboAlias extends LimboExpression {

        private final LimboExpression originalExpression;
        private final LimboExpression aliasExpression;

        public LimboAlias(LimboExpression originalExpression, LimboExpression aliasExpression) {
            this.originalExpression = originalExpression;
            this.aliasExpression = aliasExpression;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public LimboExpression getOriginalExpression() {
            return originalExpression;
        }

        public LimboExpression getAliasExpression() {
            return aliasExpression;
        }
    }

    public static class LimboTableAndColumnRef extends LimboExpression {

        private final LimboTable table;

        public LimboTableAndColumnRef(LimboTable table) {
            this.table = table;
        }

        public LimboTable getTable() {
            return this.table;
        }

        public String getString() {
            StringBuilder sb = new StringBuilder();
            sb.append(table.getName());
            sb.append("(");
            Boolean isFirstColumn = true;
            for (LimboColumn c : this.table.getColumns()) {
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
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }
    }

    public static class LimboValues extends LimboExpression {

        private final Map<String, List<LimboConstant>> values;
        private final List<LimboColumn> columns;

        public LimboValues(Map<String, List<LimboConstant>> values, List<LimboColumn> columns) {
            this.values = values;
            this.columns = columns;
        }

        public Map<String, List<LimboConstant>> getValues() {
            return this.values;
        }

        public List<LimboColumn> getColumns() {
            return this.columns;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }
    }

    // The ExpressionBag is not a built-in SQL feature,
    // but rather a utility class used in CODDTest's oracle construction
    // to substitute expressions with their corresponding constant values.
    public static class LimboExpressionBag extends LimboExpression {
        private LimboExpression innerExpr;

        public LimboExpressionBag(LimboExpression innerExpr) {
            this.innerExpr = innerExpr;
        }

        public void updateInnerExpr(LimboExpression innerExpr) {
            this.innerExpr = innerExpr;
        }

        public LimboExpression getInnerExpr() {
            return innerExpr;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class LimboTypeof extends LimboExpression {
        private final LimboExpression innerExpr;

        public LimboTypeof(LimboExpression innerExpr) {
            this.innerExpr = innerExpr;
        }

        public LimboExpression getInnerExpr() {
            return innerExpr;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class LimboResultMap extends LimboExpression {
        private final LimboValues values;
        private final List<LimboColumnName> columns;
        private final List<LimboConstant> summary;
        private final LimboDataType summaryDataType;

        public LimboResultMap(LimboValues values, List<LimboColumnName> columns, List<LimboConstant> summary,
                LimboDataType summaryDataType) {
            this.values = values;
            this.columns = columns;
            this.summary = summary;
            this.summaryDataType = summaryDataType;

            Map<String, List<LimboConstant>> vs = values.getValues();
            if (vs.get(vs.keySet().iterator().next()).size() != summary.size()) {
                throw new AssertionError();
            }
        }

        public LimboValues getValues() {
            return this.values;
        }

        public List<LimboColumnName> getColumns() {
            return this.columns;
        }

        public List<LimboConstant> getSummary() {
            return this.summary;
        }

        public LimboDataType getSummaryDataType() {
            return this.summaryDataType;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }
}
