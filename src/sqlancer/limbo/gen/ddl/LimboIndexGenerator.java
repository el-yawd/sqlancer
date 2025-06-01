package sqlancer.limbo.gen.ddl;

import java.sql.SQLException;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboProvider;
import sqlancer.limbo.LimboToStringVisitor;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

// see https://www.sqlite.org/lang_createindex.html
public class LimboIndexGenerator {

    private final ExpectedErrors errors = new ExpectedErrors();
    private final LimboGlobalState globalState;

    public static SQLQueryAdapter insertIndex(LimboGlobalState globalState) throws SQLException {
        if (globalState.getSchema().getIndexNames().size() >= globalState.getDbmsSpecificOptions().maxNumIndexes) {
            throw new IgnoreMeException();
        }
        return new LimboIndexGenerator(globalState).create();
    }

    public LimboIndexGenerator(LimboGlobalState globalState) throws SQLException {
        this.globalState = globalState;
    }

    private SQLQueryAdapter create() throws SQLException {
        LimboTable t = globalState.getSchema()
                .getRandomTableOrBailout(tab -> !tab.isView() && !tab.isVirtual() && !tab.isReadOnly());
        String q = createIndex(t, t.getColumns());
        errors.add("no such collation sequence: UINT");
        errors.add("[SQLITE_ERROR] SQL error or missing database (parser stack overflow)");
        errors.add("subqueries prohibited in index expressions");
        errors.add("subqueries prohibited in partial index WHERE clauses");
        errors.add("non-deterministic use of time() in an index");
        errors.add("non-deterministic use of strftime() in an index");
        errors.add("non-deterministic use of julianday() in an index");
        errors.add("non-deterministic use of date() in an index");
        errors.add("non-deterministic use of datetime() in an index");
        errors.add("The database file is locked");
        LimboErrors.addExpectedExpressionErrors(errors);
        if (!LimboProvider.mustKnowResult) {
            // can only happen when PRAGMA case_sensitive_like=ON;
            errors.add("non-deterministic functions prohibited");
        }

        /*
         * Strings in single quotes are sometimes interpreted as column names. Since we found an issue with double
         * quotes, they can no longer be used (see https://sqlite.org/src/info/9b78184b). Single quotes are interpreted
         * as column names in certain contexts (see
         * https://www.mail-archive.com/sqlite-users@mailinglists.sqlite.org/msg115014.html).
         */
        errors.add("[SQLITE_ERROR] SQL error or missing database (no such column:");
        return new SQLQueryAdapter(q, errors, true);
    }

    private String createIndex(LimboTable t, List<LimboColumn> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (Randomly.getBoolean()) {
            errors.add("UNIQUE constraint failed ");
            sb.append(" UNIQUE");
        }
        sb.append(" INDEX");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        } else {
            errors.add("already exists");
        }
        sb.append(" ");
        sb.append(LimboCommon.getFreeIndexName(globalState.getSchema()));
        sb.append(" ON ");
        sb.append(t.getName());
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            LimboExpression expr = new LimboExpressionGenerator(globalState).setColumns(columns).deterministicOnly()
                    .generateExpression();
            LimboToStringVisitor visitor = new LimboToStringVisitor();
            visitor.fullyQualifiedNames = false;
            visitor.visit(expr);
            sb.append(visitor.get());
            if (Randomly.getBoolean()) {
                sb.append(LimboCommon.getRandomCollate());
            }
            appendPotentialOrdering(sb);
        }
        sb.append(")");
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            LimboExpression expr = new LimboExpressionGenerator(globalState).setColumns(columns).deterministicOnly()
                    .generateExpression();
            LimboToStringVisitor visitor = new LimboToStringVisitor();
            visitor.fullyQualifiedNames = false;
            visitor.visit(expr);
            sb.append(visitor.get());
        }
        return sb.toString();
    }

    /*
     * Appends ASC, DESC, or nothing.
     */
    private void appendPotentialOrdering(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            if (Randomly.getBoolean()) {
                sb.append(" ASC");
            } else {
                sb.append(" DESC");
            }
        }
    }

}
