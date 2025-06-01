package sqlancer.limbo.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public class LimboWindowFunctionExpression extends LimboExpression {

    private final LimboExpression baseWindowFunction; // also contains the arguments to the window function
    private List<LimboExpression> partitionBy = new ArrayList<>();
    private List<LimboExpression> orderBy = new ArrayList<>();
    private LimboExpression filterClause;
    private LimboExpression frameSpec;
    private LimboFrameSpecExclude exclude;
    private LimboFrameSpecKind frameSpecKind;

    public static class LimboWindowFunctionFrameSpecTerm extends LimboExpression {

        public enum LimboWindowFunctionFrameSpecTermKind {
            UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"), EXPR_PRECEDING("PRECEDING"), CURRENT_ROW("CURRENT ROW"),
            EXPR_FOLLOWING("FOLLOWING"), UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");

            String s;

            LimboWindowFunctionFrameSpecTermKind(String s) {
                this.s = s;
            }

            public String getString() {
                return s;
            }

        }

        private final LimboExpression expression;
        private final LimboWindowFunctionFrameSpecTermKind kind;

        public LimboWindowFunctionFrameSpecTerm(LimboExpression expression,
                LimboWindowFunctionFrameSpecTermKind kind) {
            this.expression = expression;
            this.kind = kind;
        }

        public LimboWindowFunctionFrameSpecTerm(LimboWindowFunctionFrameSpecTermKind kind) {
            this.kind = kind;
            this.expression = null;
        }

        public LimboExpression getExpression() {
            return expression;
        }

        public LimboWindowFunctionFrameSpecTermKind getKind() {
            return kind;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class LimboWindowFunctionFrameSpecBetween extends LimboExpression {

        private final LimboWindowFunctionFrameSpecTerm left;
        private final LimboWindowFunctionFrameSpecTerm right;

        public LimboWindowFunctionFrameSpecBetween(LimboWindowFunctionFrameSpecTerm left,
                LimboWindowFunctionFrameSpecTerm right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public LimboCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public LimboWindowFunctionFrameSpecTerm getLeft() {
            return left;
        }

        public LimboWindowFunctionFrameSpecTerm getRight() {
            return right;
        }

    }

    public enum LimboFrameSpecExclude {
        EXCLUDE_NO_OTHERS("EXCLUDE NO OTHERS"), EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),
        EXCLUDE_GROUP("EXCLUDE GROUP"), EXCLUDE_TIES("EXCLUDE TIES");

        private final String s;

        LimboFrameSpecExclude(String s) {
            this.s = s;
        }

        public static LimboFrameSpecExclude getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getString() {
            return s;
        }
    }

    public enum LimboFrameSpecKind {
        RANGE, ROWS, GROUPS;

        public static LimboFrameSpecKind getRandom() {
            return Randomly.fromOptions(LimboFrameSpecKind.values());
        }
    }

    public LimboWindowFunctionExpression(LimboExpression baseWindowFunction) {
        this.baseWindowFunction = baseWindowFunction;
    }

    public LimboExpression getBaseWindowFunction() {
        return baseWindowFunction;
    }

    public List<LimboExpression> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<LimboExpression> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public List<LimboExpression> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<LimboExpression> orderBy) {
        this.orderBy = orderBy;
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        return null;
    }

    public LimboExpression getFilterClause() {
        return filterClause;
    }

    public void setFilterClause(LimboExpression filterClause) {
        this.filterClause = filterClause;
    }

    public LimboExpression getFrameSpec() {
        return frameSpec;
    }

    public void setFrameSpec(LimboExpression frameSpec) {
        this.frameSpec = frameSpec;
    }

    public LimboFrameSpecExclude getExclude() {
        return exclude;
    }

    public void setExclude(LimboFrameSpecExclude exclude) {
        this.exclude = exclude;
    }

    public LimboFrameSpecKind getFrameSpecKind() {
        return frameSpecKind;
    }

    public void setFrameSpecKind(LimboFrameSpecKind frameSpecKind) {
        this.frameSpecKind = frameSpecKind;
    }

}
