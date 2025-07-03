package sqlancer.turso.gen;

import java.util.List;
import sqlancer.Randomly;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.schema.TursoSchema.TursoColumn;

public class TursoColumnBuilder {

    private boolean containsPrimaryKey;
    private boolean containsAutoIncrement;
    private final StringBuilder sb = new StringBuilder();
    private boolean conflictClauseInserted;

    private boolean allowPrimaryKey = true;
    private boolean allowUnique = false;
    private boolean allowDefaultValue = true;
    private boolean allowNotNull = true;

    private enum Constraints {
        NOT_NULL,
    }

    public boolean isContainsAutoIncrement() {
        return containsAutoIncrement;
    }

    public boolean isConflictClauseInserted() {
        return conflictClauseInserted;
    }

    public boolean isContainsPrimaryKey() {
        return containsPrimaryKey;
    }

    public String createColumn(
        String columnName,
        TursoGlobalState globalState,
        List<TursoColumn> columns
    ) {
        sb.append(columnName);
        sb.append(" ");
        String dataType = Randomly.fromOptions(
            "INT",
            "TEXT",
            "BLOB",
            "REAL",
            "INTEGER"
        );
        sb.append(dataType);

        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<Constraints> constraints = Randomly.subset(
                Constraints.values()
            );

            for (Constraints c : constraints) {
                switch (c) {
                    case NOT_NULL:
                        if (allowNotNull) {
                            sb.append(" NOT NULL");
                        }
                        break;
                    default:
                        throw new AssertionError();
                }
            }
        }
        if (allowDefaultValue && Randomly.getBooleanWithSmallProbability()) {
            sb.append(" DEFAULT ");
            sb.append(
                TursoVisitor.asString(
                    TursoExpressionGenerator.getRandomLiteralValue(globalState)
                )
            );
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            String randomCollate = TursoCommon.getRandomCollate();
            sb.append(randomCollate);
        }
        return sb.toString();
    }

    public TursoColumnBuilder allowPrimaryKey(boolean allowPrimaryKey) {
        this.allowPrimaryKey = allowPrimaryKey;
        return this;
    }

    public TursoColumnBuilder allowUnique(boolean allowUnique) {
        this.allowUnique = allowUnique;
        return this;
    }

    public TursoColumnBuilder allowDefaultValue(boolean allowDefaultValue) {
        this.allowDefaultValue = allowDefaultValue;
        return this;
    }

    public TursoColumnBuilder allowNotNull(boolean allowNotNull) {
        this.allowNotNull = allowNotNull;
        return this;
    }
}
