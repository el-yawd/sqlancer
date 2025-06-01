package sqlancer.limbo.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboSelect.SelectType;
import sqlancer.limbo.ast.LimboSetClause;
import sqlancer.limbo.ast.LimboSetClause.LimboClauseType;
import sqlancer.limbo.ast.LimboWindowFunction;
import sqlancer.limbo.ast.LimboWindowFunctionExpression;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboFrameSpecExclude;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboFrameSpecKind;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecBetween;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecTerm;
import sqlancer.limbo.ast.LimboWindowFunctionExpression.LimboWindowFunctionFrameSpecTerm.LimboWindowFunctionFrameSpecTermKind;
import sqlancer.limbo.gen.LimboCommon;
import sqlancer.limbo.gen.LimboExpressionGenerator;
import sqlancer.limbo.schema.LimboSchema;
import sqlancer.limbo.schema.LimboSchema.LimboTable;
import sqlancer.limbo.schema.LimboSchema.LimboTables;

public final class LimboRandomQuerySynthesizer {

    private LimboRandomQuerySynthesizer() {
    }

    // TODO join clauses
    // TODO union, intersect
    public static LimboExpression generate(LimboGlobalState globalState, int size) {
        Randomly r = globalState.getRandomly();
        LimboSchema s = globalState.getSchema();
        LimboTables targetTables = s.getRandomTableNonEmptyTables();
        List<LimboExpression> expressions = new ArrayList<>();
        LimboExpressionGenerator gen = new LimboExpressionGenerator(globalState)
                .setColumns(s.getTables().getColumns());
        LimboExpressionGenerator whereClauseGen = new LimboExpressionGenerator(globalState);
        LimboExpressionGenerator aggregateGen = new LimboExpressionGenerator(globalState)
                .setColumns(s.getTables().getColumns()).allowAggregateFunctions();

        // SELECT
        LimboSelect select = new LimboSelect();
        // DISTINCT or ALL
        select.setSelectType(Randomly.fromOptions(SelectType.values()));
        for (int i = 0; i < size; i++) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                LimboExpression baseWindowFunction;
                boolean normalAggregateFunction = Randomly.getBoolean();
                if (!normalAggregateFunction) {
                    baseWindowFunction = LimboWindowFunction.getRandom(targetTables.getColumns(), globalState);
                } else {
                    baseWindowFunction = gen.getAggregateFunction(true);
                    assert baseWindowFunction != null;
                }
                LimboWindowFunctionExpression windowFunction = new LimboWindowFunctionExpression(
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
                    windowFunction.setFrameSpecKind(LimboFrameSpecKind.getRandom());
                    LimboExpression windowFunctionTerm;
                    if (Randomly.getBoolean()) {
                        windowFunctionTerm = new LimboWindowFunctionFrameSpecTerm(
                                Randomly.fromOptions(LimboWindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING,
                                        LimboWindowFunctionFrameSpecTermKind.CURRENT_ROW));
                    } else if (Randomly.getBoolean()) {
                        windowFunctionTerm = new LimboWindowFunctionFrameSpecTerm(gen.generateExpression(),
                                LimboWindowFunctionFrameSpecTermKind.EXPR_PRECEDING);
                    } else {
                        LimboWindowFunctionFrameSpecTerm left = getTerm(true, gen);
                        LimboWindowFunctionFrameSpecTerm right = getTerm(false, gen);
                        windowFunctionTerm = new LimboWindowFunctionFrameSpecBetween(left, right);
                    }
                    windowFunction.setFrameSpec(windowFunctionTerm);
                    if (Randomly.getBoolean()) {
                        windowFunction.setExclude(LimboFrameSpecExclude.getRandom());
                    }
                }
                expressions.add(windowFunction);
            } else {
                expressions.add(aggregateGen.generateExpression());
            }
        }
        select.setFetchColumns(expressions);
        List<LimboTable> tables = targetTables.getTables();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            // JOIN ... (might remove tables)
            select.setJoinClauses(gen.getRandomJoinClauses(tables));
        }
        // FROM ...
        select.setFromList(LimboCommon.getTableRefs(tables, s));
        // TODO: no values are referenced from this sub query yet
        // if (Randomly.getBooleanWithSmallProbability()) {
        // select.getFromList().add(LimboRandomQuerySynthesizer.generate(globalState,
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
            select.setLimitClause(LimboConstant.createIntConstant(r.getInteger()));
            if (Randomly.getBoolean()) {
                // OFFSET
                select.setOffsetClause(LimboConstant.createIntConstant(r.getInteger()));
            }
        }
        if (!orderBy && !groupBy && Randomly.getBooleanWithSmallProbability()) {
            return new LimboSetClause(select, generate(globalState, size), LimboClauseType.getRandom());
        }
        return select;
    }

    private static LimboWindowFunctionFrameSpecTerm getTerm(boolean isLeftTerm, LimboExpressionGenerator gen) {
        if (Randomly.getBoolean()) {
            LimboExpression expr = gen.generateExpression();
            LimboWindowFunctionFrameSpecTermKind kind = Randomly.fromOptions(
                    LimboWindowFunctionFrameSpecTermKind.EXPR_FOLLOWING,
                    LimboWindowFunctionFrameSpecTermKind.EXPR_PRECEDING);
            return new LimboWindowFunctionFrameSpecTerm(expr, kind);
        } else if (Randomly.getBoolean()) {
            return new LimboWindowFunctionFrameSpecTerm(LimboWindowFunctionFrameSpecTermKind.CURRENT_ROW);
        } else {
            if (isLeftTerm) {
                return new LimboWindowFunctionFrameSpecTerm(
                        LimboWindowFunctionFrameSpecTermKind.UNBOUNDED_PRECEDING);
            } else {
                return new LimboWindowFunctionFrameSpecTerm(
                        LimboWindowFunctionFrameSpecTermKind.UNBOUNDED_FOLLOWING);
            }
        }
    }

}
