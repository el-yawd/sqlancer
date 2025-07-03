package sqlancer.turso.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.turso.TursoErrors;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoTables;

public class TursoTLPBase extends TernaryLogicPartitioningOracleBase<TursoExpression, TursoGlobalState>
        implements TestOracle<TursoGlobalState> {

    TursoSchema s;
    TursoTables targetTables;
    TursoExpressionGenerator gen;
    TursoSelect select;

    public TursoTLPBase(TursoGlobalState state) {
        super(state);
        TursoErrors.addExpectedExpressionErrors(errors);
        TursoErrors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new TursoExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new TursoSelect();
        select.setFetchColumns(generateFetchColumns());
        List<TursoTable> tables = targetTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<TursoExpression> tableRefs = TursoCommon.getTableRefs(tables, s);
        select.setJoinClauses(joinStatements.stream().collect(Collectors.toList()));
        select.setFromList(tableRefs);
        select.setWhereClause(null);
    }

    List<TursoExpression> generateFetchColumns() {
        List<TursoExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new TursoColumnName(TursoColumn.createDummy("*"), null));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new TursoColumnName(c, null)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<TursoExpression> getGen() {
        return gen;
    }

}
