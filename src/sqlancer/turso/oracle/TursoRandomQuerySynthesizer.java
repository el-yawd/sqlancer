package sqlancer.turso.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoSetClause;
import sqlancer.turso.ast.TursoWindowFunction;
import sqlancer.turso.ast.TursoWindowFunctionExpression;
import sqlancer.turso.ast.TursoSelect.SelectType;
import sqlancer.turso.ast.TursoSetClause.TursoClauseType;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoFrameSpecExclude;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoFrameSpecKind;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoWindowFunctionFrameSpecBetween;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoWindowFunctionFrameSpecTerm;
import sqlancer.turso.ast.TursoWindowFunctionExpression.TursoWindowFunctionFrameSpecTerm.TursoWindowFunctionFrameSpecTermKind;
import sqlancer.turso.gen.TursoCommon;
import sqlancer.turso.gen.TursoExpressionGenerator;
import sqlancer.turso.schema.TursoSchema;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoTables;

public final class TursoRandomQuerySynthesizer {

    private TursoRandomQuerySynthesizer() {
    }

    // TODO join clauses
    // TODO union, intersect
    public static TursoExpression generate(TursoGlobalState globalState, int size) {
        Randomly r = globalState.getRandomly();
        TursoSchema s = globalState.getSchema();
        TursoTables targetTables = s.getRandomTableNonEmptyTables();
        List<TursoExpression> expressions = new ArrayList<>();
        TursoExpressionGenerator gen = new TursoExpressionGenerator(globalState)
                .setColumns(s.getTables().getColumns());
        TursoExpressionGenerator whereClauseGen = new TursoExpressionGenerator(globalState);
        TursoExpressionGenerator aggregateGen = new TursoExpressionGenerator(globalState)
                .setColumns(s.getTables().getColumns()).allowAggregateFunctions();

        // SELECT
        TursoSelect select = new TursoSelect();
        // DISTINCT or ALL
        select.setSelectType(Randomly.fromOptions(SelectType.values()));
        for (int i = 0; i < size; i++) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                TursoExpression baseWindowFunction;
                boolean normalAggregateFunction = Randomly.getBoolean();
                if (!normalAggregateFunction) {
                    baseWindowFunction = TursoWindowFunction.getRandom(targetTables.getColumns(), globalState);
                } else {
                    baseWindowFunction = gen.getAggregateFunction(true);
                    assert baseWindowFunction != null;
                }
                TursoWindowFunctionExpression windowFunction = new TursoWindowFunctionExpression(
                        baseWindowFunction);
                if (Randomly.getBooleanWithRatherLowProbability() && normalAggregateFunction) {
                    windowFunction.setFilterClause(gen.generateExpression());
                }
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    windowFunction.setOrderBy(gen.generateOrderBys());
                }
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    windowFunction.setPartitionBy(gen.getRandomExpressions(Randomly.smallNumber()));
                }
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    windowFunction.setFrameSpecKind(TursoFrameSpecKind.getRandom());
                    TursoExpression windowFunctionTerm;
                    if (Randomly.getBoolean()) {
                        windowFunctionTerm = new TursoWindowFunctionFrameSpecTerm(
                                Randomly.fromOptions(TursoWindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING,
                                        TursoWindowFunctionFrameSpecTermKind.CURRENT_ROW));
                    } else if (Randomly.getBoolean()) {
                        windowFunctionTerm = new TursoWindowFunctionFrameSpecTerm(gen.generateExpression(),
                                TursoWindowFunctionFrameSpecTermKind.EXPR_PRECEDING);
                    } else {
                        TursoWindowFunctionFrameSpecTerm left = getTerm(true, gen);
                        TursoWindowFunctionFrameSpecTerm right = getTerm(false, gen);
                        windowFunctionTerm = new TursoWindowFunctionFrameSpecBetween(left, right);
                    }
                    windowFunction.setFrameSpec(windowFunctionTerm);
                    if (Randomly.getBoolean()) {
                        windowFunction.setExclude(TursoFrameSpecExclude.getRandom());
                    }
                }
                expressions.add(windowFunction);
            } else {
                expressions.add(aggregateGen.generateExpression());
            }
        }
        select.setFetchColumns(expressions);
        List<TursoTable> tables = targetTables.getTables();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // JOIN ... (might remove tables)
            select.setJoinClauses(gen.getRandomJoinClauses(tables));
        }
        // FROM ...
        select.setFromList(TursoCommon.getTableRefs(tables, s));
        // TODO: no values are referenced from this sub query yet
        // if (Randomly.getBooleanWithSmallProbability()) {
        // select.getFromList().add(TursoRandomQuerySynthesizer.generate(globalState,
        // Randomly.smallNumber() + 1));
        // }

        // WHERE
        if (Randomly.getBoolean()) {
            select.setWhereClause(whereClauseGen.generateExpression());
        }
        boolean groupBy = Randomly.getBooleanWithRatherLowProbability();
        if (groupBy) {
            // GROUP BY
            select.setGroupByClause(gen.getRandomExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                // HAVING
                select.setHavingClause(aggregateGen.generateExpression());
            }
        }
        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            // ORDER BY
            select.setOrderByClauses(gen.generateOrderBys());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // LIMIT
            select.setLimitClause(TursoConstant.createIntConstant(r.getInteger()));
            if (Randomly.getBoolean()) {
                // OFFSET
                select.setOffsetClause(TursoConstant.createIntConstant(r.getInteger()));
            }
        }
        if (!orderBy && !groupBy && Randomly.getBooleanWithSmallProbability()) {
            return new TursoSetClause(select, generate(globalState, size), TursoClauseType.getRandom());
        }
        return select;
    }

    private static TursoWindowFunctionFrameSpecTerm getTerm(boolean isLeftTerm, TursoExpressionGenerator gen) {
        if (Randomly.getBoolean()) {
            TursoExpression expr = gen.generateExpression();
            TursoWindowFunctionFrameSpecTermKind kind = Randomly.fromOptions(
                    TursoWindowFunctionFrameSpecTermKind.EXPR_FOLLOWING,
                    TursoWindowFunctionFrameSpecTermKind.EXPR_PRECEDING);
            return new TursoWindowFunctionFrameSpecTerm(expr, kind);
        } else if (Randomly.getBoolean()) {
            return new TursoWindowFunctionFrameSpecTerm(TursoWindowFunctionFrameSpecTermKind.CURRENT_ROW);
        } else {
            if (isLeftTerm) {
                return new TursoWindowFunctionFrameSpecTerm(
                        TursoWindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING);
            } else {
                return new TursoWindowFunctionFrameSpecTerm(
                        TursoWindowFunctionFrameSpecTermKind.UNBOUNDED_FOLLOWING);
            }
        }
    }

}
