package sqlancer.limbo.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.limbo.LimboErrors;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboTable;
import sqlancer.limbo.schema.LimboSchema.LimboTables;

public class LimboTLPBase extends TernaryLogicPartitioningOracleBase<LimboExpression, LimboGlobalState>
        implements TestOracle<LimboGlobalState> {

    LimboSchema s;
    LimboTables targetTables;
    LimboExpressionGenerator gen;
    LimboSelect select;

    public LimboTLPBase(LimboGlobalState state) {
        super(state);
        LimboErrors.addExpectedExpressionErrors(errors);
        LimboErrors.addQueryErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        gen = new LimboExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new LimboSelect();
        select.setFetchColumns(generateFetchColumns());
        List<LimboTable> tables = targetTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<LimboExpression> tableRefs = LimboCommon.getTableRefs(tables, s);
        select.setJoinClauses(joinStatements.stream().collect(Collectors.toList()));
        select.setFromList(tableRefs);
        select.setWhereClause(null);
    }

    List<LimboExpression> generateFetchColumns() {
        List<LimboExpression> columns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            columns.add(new LimboColumnName(LimboColumn.createDummy("*"), null));
        } else {
            columns = Randomly.nonEmptySubset(targetTables.getColumns()).stream()
                    .map(c -> new LimboColumnName(c, null)).collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    protected ExpressionGenerator<LimboExpression> getGen() {
        return gen;
    }

}
