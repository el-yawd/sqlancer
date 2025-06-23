package sqlancer.limbo.oracle;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.LimboVisitor;

// tries to trigger a crash
public class LimboFuzzer implements TestOracle<LimboGlobalState> {

    private final LimboGlobalState globalState;

    public LimboFuzzer(LimboGlobalState globalState) {
        this.globalState = globalState;
    }

    @Override
    public void check() throws Exception {
        System.out.println("Checking Limbo Fuzzer");
        String s =
            LimboVisitor.asString(
                LimboRandomQuerySynthesizer.generate(
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
