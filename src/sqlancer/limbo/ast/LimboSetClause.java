package sqlancer.limbo.ast;

import sqlancer.Randomly;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public class LimboSetClause extends LimboExpression {

    private final LimboExpression left;
    private final LimboExpression right;
    private final LimboClauseType type;

    public enum LimboClauseType {
        UNION("UNION"), UNION_ALL("UNION ALL"), INTERSECT("INTERSECT"), EXCEPT("EXCEPT");

        private final String textRepresentation;

        LimboClauseType(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static LimboClauseType getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public LimboSetClause(LimboExpression left, LimboExpression right, LimboClauseType type) {
        this.left = left;
        this.right = right;
        this.type = type;
    }

    public LimboExpression getLeft() {
        return left;
    }

    public LimboExpression getRight() {
        return right;
    }

    public LimboClauseType getType() {
        return type;
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        // TODO Auto-generated method stub
        return null;
    }

}
