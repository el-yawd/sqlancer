package sqlancer.limbo.gen;

import java.util.List;
import sqlancer.Randomly;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;

public class LimboColumnBuilder {

    private boolean containsPrimaryKey;
    private boolean containsAutoIncrement;
    private final StringBuilder sb = new StringBuilder();
    private boolean conflictClauseInserted;

    private boolean allowPrimaryKey = true;
    private boolean allowUnique = true;
    private boolean allowDefaultValue = true;
    private boolean allowNotNull = true;

    private enum Constraints {
        NOT_NULL,
        PRIMARY_KEY,
        UNIQUE,
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
        LimboGlobalState globalState,
        List<LimboColumn> columns
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
                    case PRIMARY_KEY:
                        // only one primary key is allow if not specified as table constraint
                        if (allowPrimaryKey) {
                            sb.append(" PRIMARY KEY");
                            containsPrimaryKey = true;
                            boolean hasOrdering = Randomly.getBoolean();
                            if (hasOrdering) {
                                if (Randomly.getBoolean()) {
                                    sb.append(" ASC");
                                } else {
                                    sb.append(" DESC");
                                }
                            }
                            if (
                                !hasOrdering &&
                                dataType.equals("INTEGER") &&
                                Randomly.getBoolean()
                            ) {
                                containsAutoIncrement = true;
                                sb.append(" AUTOINCREMENT");
                            }
                        }
                        break;
                    case UNIQUE:
                        if (allowUnique) {
                            sb.append(" UNIQUE");
                        }
                        break;
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
                LimboVisitor.asString(
                    LimboExpressionGenerator.getRandomLiteralValue(globalState)
                )
            );
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            String randomCollate = LimboCommon.getRandomCollate();
            sb.append(randomCollate);
        }
        return sb.toString();
    }

    public LimboColumnBuilder allowPrimaryKey(boolean allowPrimaryKey) {
        this.allowPrimaryKey = allowPrimaryKey;
        return this;
    }

    public LimboColumnBuilder allowUnique(boolean allowUnique) {
        this.allowUnique = allowUnique;
        return this;
    }

    public LimboColumnBuilder allowDefaultValue(boolean allowDefaultValue) {
        this.allowDefaultValue = allowDefaultValue;
        return this;
    }

    public LimboColumnBuilder allowNotNull(boolean allowNotNull) {
        this.allowNotNull = allowNotNull;
        return this;
    }
}
