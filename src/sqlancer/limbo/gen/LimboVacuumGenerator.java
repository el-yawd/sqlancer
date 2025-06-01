package sqlancer.limbo.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;

/**
 * @see <a href="https://www.sqlite.org/lang_vacuum.html">VACUUM</a>
 */
public final class LimboVacuumGenerator {

    private LimboVacuumGenerator() {
    }

    public static SQLQueryAdapter executeVacuum(LimboGlobalState globalState) {
        StringBuilder sb = new StringBuilder("VACUUM");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("temp", "main"));
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot VACUUM from within a transaction",
                "cannot VACUUM - SQL statements in progress", "The database file is locked"));
    }

}
