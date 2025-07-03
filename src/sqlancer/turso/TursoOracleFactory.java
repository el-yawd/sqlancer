package sqlancer.turso;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.NoRECOracle;
import sqlancer.common.oracle.TLPWhereOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.oracle.TursoCODDTestOracle;
import sqlancer.turso.oracle.TursoFuzzer;
import sqlancer.turso.oracle.TursoPivotedQuerySynthesisOracle;
import sqlancer.turso.oracle.tlp.TursoTLPAggregateOracle;
import sqlancer.turso.oracle.tlp.TursoTLPDistinctOracle;
import sqlancer.turso.oracle.tlp.TursoTLPGroupByOracle;
import sqlancer.turso.oracle.tlp.TursoTLPHavingOracle;

public enum TursoOracleFactory implements OracleFactory<TursoGlobalState> {
    PQS {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoPivotedQuerySynthesisOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
    NoREC {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            TursoExpressionGenerator gen = new TursoExpressionGenerator(
                globalState
            );
            ExpectedErrors errors = ExpectedErrors.newErrors()
                .with(TursoErrors.getExpectedExpressionErrors())
                .with(TursoErrors.getMatchQueryErrors())
                .with(TursoErrors.getQueryErrors())
                .with(
                    "misuse of aggregate",
                    "misuse of window function",
                    "second argument to nth_value must be a positive integer",
                    "no such table",
                    "no query solution",
                    "unable to use function MATCH in the requested context"
                )
                .build();
            return new NoRECOracle<>(globalState, gen, errors);
        }
    },
    AGGREGATE {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoTLPAggregateOracle(globalState);
        }
    },
    WHERE {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            TursoExpressionGenerator gen = new TursoExpressionGenerator(
                globalState
            );
            ExpectedErrors expectedErrors = ExpectedErrors.newErrors()
                .with(TursoErrors.getExpectedExpressionErrors())
                .build();
            return new TLPWhereOracle<>(globalState, gen, expectedErrors);
        }
    },
    DISTINCT {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoTLPDistinctOracle(globalState);
        }
    },
    GROUP_BY {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoTLPGroupByOracle(globalState);
        }
    },
    HAVING {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoTLPHavingOracle(globalState);
        }
    },
    FUZZER {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoFuzzer(globalState);
        }
    },
    QUERY_PARTITIONING {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws Exception {
            List<TestOracle<TursoGlobalState>> oracles = new ArrayList<>();
            oracles.add(WHERE.create(globalState));
            oracles.add(DISTINCT.create(globalState));
            oracles.add(GROUP_BY.create(globalState));
            oracles.add(HAVING.create(globalState));
            oracles.add(AGGREGATE.create(globalState));
            return new CompositeTestOracle<TursoGlobalState>(
                oracles,
                globalState
            );
        }
    },
    CODDTest {
        @Override
        public TestOracle<TursoGlobalState> create(TursoGlobalState globalState)
            throws SQLException {
            return new TursoCODDTestOracle(globalState);
        }

        @Override
        public boolean requiresAllTablesToContainRows() {
            return true;
        }
    },
}
