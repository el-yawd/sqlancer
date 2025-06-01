package sqlancer.limbo.gen.ddl;

import java.sql.SQLException;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboOracleFactory;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.oracle.LimboRandomQuerySynthesizer;
import sqlancer.limbo.schema.LimboSchema;

public final class LimboViewGenerator {

    private LimboViewGenerator() {
    }

    public static SQLQueryAdapter dropView(LimboGlobalState globalState) {
        LimboSchema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder("DROP VIEW ");
        sb.append(s.getRandomViewOrBailout().getName());
        return new SQLQueryAdapter(sb.toString(), true);
    }

    public static SQLQueryAdapter generate(LimboGlobalState globalState) throws SQLException {
        if (globalState.getSchema().getTables().getTables()
                .size() >= globalState.getDbmsSpecificOptions().maxNumTables) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE");
        if (globalState.getDbmsSpecificOptions().testTempTables && Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("TEMP", "TEMPORARY"));
        }
        sb.append(" VIEW ");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS ");
        }
        sb.append(LimboCommon.getFreeViewName(globalState.getSchema()));
        ExpectedErrors errors = new ExpectedErrors();
        LimboErrors.addExpectedExpressionErrors(errors);
        errors.add("is circularly defined");
        errors.add("unsupported frame specification");
        errors.add("The database file is locked");
        int size = 1 + Randomly.smallNumber();
        columnNamesAs(sb, size);
        LimboExpression randomQuery;
        do {
            randomQuery = LimboRandomQuerySynthesizer.generate(globalState, size);
        } while (globalState.getDbmsSpecificOptions().oracles == LimboOracleFactory.PQS
                && !checkAffinity(randomQuery));
        sb.append(LimboVisitor.asString(randomQuery));
        return new SQLQueryAdapter(sb.toString(), errors, true);

    }

    /**
     * The affinity of columns in a view cannot be determined using features of the DBMS - this would need to be parsed
     * from the CREATE TABLE and CREATE VIEW statements. This is non-trivial, and currently not implemented. Rather, we
     * avoid generating expressions with an affinity or view.
     *
     * @see http://sqlite.1065341.n5.nabble.com/Determining-column-collating-functions-td108157.html#a108159
     *
     * @param randomQuery
     *
     * @return true if the query can be used for PQS
     */
    private static boolean checkAffinity(LimboExpression randomQuery) {
        if (randomQuery instanceof LimboSelect) {
            for (LimboExpression expr : ((LimboSelect) randomQuery).getFetchColumns()) {
                if (expr.getExpectedValue() == null || expr.getAffinity() != null
                        || expr.getImplicitCollateSequence() != null || expr.getExplicitCollateSequence() != null) {
                    return false;
                }
            }
            return true;
        } else {
            return false; // the columns in UNION clauses can also have affinities
        }
    }

    private static void columnNamesAs(StringBuilder sb, int size) {
        sb.append("(");
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(DBMSCommon.createColumnName(i));
        }
        sb.append(")");
        sb.append(" AS ");
    }

}
