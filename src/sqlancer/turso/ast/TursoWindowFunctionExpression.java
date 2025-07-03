package sqlancer.turso.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoWindowFunctionExpression extends TursoExpression {

    private final TursoExpression baseWindowFunction; // also contains the arguments to the window function
    private List<TursoExpression> partitionBy = new ArrayList<>();
    private List<TursoExpression> orderBy = new ArrayList<>();
    private TursoExpression filterClause;
    private TursoExpression frameSpec;
    private TursoFrameSpecExclude exclude;
    private TursoFrameSpecKind frameSpecKind;

    public static class TursoWindowFunctionFrameSpecTerm extends TursoExpression {

        public enum TursoWindowFunctionFrameSpecTermKind {
            UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"), EXPR_PRECEDING("PRECEDING"), CURRENT_ROW("CURRENT ROW"),
            EXPR_FOLLOWING("FOLLOWING"), UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");

            String s;

            TursoWindowFunctionFrameSpecTermKind(String s) {
                this.s = s;
            }

            public String getString() {
                return s;
            }

        }

        private final TursoExpression expression;
        private final TursoWindowFunctionFrameSpecTermKind kind;

        public TursoWindowFunctionFrameSpecTerm(TursoExpression expression,
                TursoWindowFunctionFrameSpecTermKind kind) {
            this.expression = expression;
            this.kind = kind;
        }

        public TursoWindowFunctionFrameSpecTerm(TursoWindowFunctionFrameSpecTermKind kind) {
            this.kind = kind;
            this.expression = null;
        }

        public TursoExpression getExpression() {
            return expression;
        }

        public TursoWindowFunctionFrameSpecTermKind getKind() {
            return kind;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

    }

    public static class TursoWindowFunctionFrameSpecBetween extends TursoExpression {

        private final TursoWindowFunctionFrameSpecTerm left;
        private final TursoWindowFunctionFrameSpecTerm right;

        public TursoWindowFunctionFrameSpecBetween(TursoWindowFunctionFrameSpecTerm left,
                TursoWindowFunctionFrameSpecTerm right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public TursoCollateSequence getExplicitCollateSequence() {
            return null;
        }

        public TursoWindowFunctionFrameSpecTerm getLeft() {
            return left;
        }

        public TursoWindowFunctionFrameSpecTerm getRight() {
            return right;
        }

    }

    public enum TursoFrameSpecExclude {
        EXCLUDE_NO_OTHERS("EXCLUDE NO OTHERS"), EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),
        EXCLUDE_GROUP("EXCLUDE GROUP"), EXCLUDE_TIES("EXCLUDE TIES");

        private final String s;

        TursoFrameSpecExclude(String s) {
            this.s = s;
        }

        public static TursoFrameSpecExclude getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getString() {
            return s;
        }
    }

    public enum TursoFrameSpecKind {
        RANGE, ROWS, GROUPS;

        public static TursoFrameSpecKind getRandom() {
            return Randomly.fromOptions(TursoFrameSpecKind.values());
        }
    }

    public TursoWindowFunctionExpression(TursoExpression baseWindowFunction) {
        this.baseWindowFunction = baseWindowFunction;
    }

    public TursoExpression getBaseWindowFunction() {
        return baseWindowFunction;
    }

    public List<TursoExpression> getPartitionBy() {
        return partitionBy;
    }

    public void setPartitionBy(List<TursoExpression> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public List<TursoExpression> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<TursoExpression> orderBy) {
        this.orderBy = orderBy;
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        return null;
    }

    public TursoExpression getFilterClause() {
        return filterClause;
    }

    public void setFilterClause(TursoExpression filterClause) {
        this.filterClause = filterClause;
    }

    public TursoExpression getFrameSpec() {
        return frameSpec;
    }

    public void setFrameSpec(TursoExpression frameSpec) {
        this.frameSpec = frameSpec;
    }

    public TursoFrameSpecExclude getExclude() {
        return exclude;
    }

    public void setExclude(TursoFrameSpecExclude exclude) {
        this.exclude = exclude;
    }

    public TursoFrameSpecKind getFrameSpecKind() {
        return frameSpecKind;
    }

    public void setFrameSpecKind(TursoFrameSpecKind frameSpecKind) {
        this.frameSpecKind = frameSpecKind;
    }

}
