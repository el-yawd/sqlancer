package sqlancer.turso.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.TursoVisitor;

// tries to trigger a crash
public class TursoFuzzer implements TestOracle<TursoGlobalState> {

    private final TursoGlobalState globalState;

    public TursoFuzzer(TursoGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        System.out.println("Checking Turso Fuzzer");
        String s =
            TursoVisitor.asString(
                TursoRandomQuerySynthesizer.generate(
                    globalState,
                    Randomly.smallNumber() + 1
                )
            ) +
            ";";
        try {
            globalState.executeStatement(new SQLQueryAdapter(s));
            globalState.getManager().incrementSelectQueryCount();
        } catch (Error e) {}
    }
}
