package sqlancer.turso.ast;

import sqlancer.Randomly;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoSetClause extends TursoExpression {

    private final TursoExpression left;
    private final TursoExpression right;
    private final TursoClauseType type;

    public enum TursoClauseType {
        UNION("UNION"), UNION_ALL("UNION ALL"), INTERSECT("INTERSECT"), EXCEPT("EXCEPT");

        private final String textRepresentation;

        TursoClauseType(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static TursoClauseType getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public TursoSetClause(TursoExpression left, TursoExpression right, TursoClauseType type) {
        this.left = left;
        this.right = right;
        this.type = type;
    }

    public TursoExpression getLeft() {
        return left;
    }

    public TursoExpression getRight() {
        return right;
    }

    public TursoClauseType getType() {
        return type;
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        // TODO Auto-generated method stub
        return null;
    }

}
