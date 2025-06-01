package sqlancer.limbo;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.oracle.LimboCODDTestOracle;
import sqlancer.limbo.oracle.LimboFuzzer;
import sqlancer.limbo.oracle.LimboPivotedQuerySynthesisOracle;
import sqlancer.limbo.oracle.tlp.LimboTLPAggregateOracle;
import sqlancer.limbo.oracle.tlp.LimboTLPDistinctOracle;
import sqlancer.limbo.oracle.tlp.LimboTLPGroupByOracle;
import sqlancer.limbo.oracle.tlp.LimboTLPHavingOracle;

public enum LimboOracleFactory implements OracleFactory<LimboGlobalState> {
    PQS {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }

    },
    NoREC {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            LimboExpressionGenerator gen = new LimboExpressionGenerator(globalState);
            ExpectedErrors errors = ExpectedErrors.newErrors().with(LimboErrors.getExpectedExpressionErrors())
                    .with(LimboErrors.getMatchQueryErrors()).with(LimboErrors.getQueryErrors())
                    .with("misuse of aggregate", "misuse of window function",
                            "second argument to nth_value must be a positive integer", "no such table",
                            "no query solution", "unable to use function MATCH in the requested context")
                    .build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboTLPAggregateOracle(globalState);
        }

    },
    WHERE {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            LimboExpressionGenerator gen = new LimboExpressionGenerator(globalState);
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors().with(LimboErrors.getExpectedExpressionErrors())
                    .build();
            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }

    },
    DISTINCT {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboTLPDistinctOracle(globalState);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboTLPGroupByOracle(globalState);
        }
    },
    HAVING {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboTLPHavingOracle(globalState);
        }
    },
    FUZZER {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboFuzzer(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws Exception {
            List<TestOracle<LimboGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            return new CompositeTestOracle<LimboGlobalState>(oracles, globalState);
        }
    },
    CODDTest {
        @Override
        public TestOracle<LimboGlobalState> create(LimboGlobalState globalState) throws SQLException {
            return new LimboCODDTestOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    };

}
