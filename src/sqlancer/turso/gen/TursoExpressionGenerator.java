package sqlancer.turso.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.gen.NoRECGenerator;
import sqlancer.common.gen.TLPWhereGenerator;
import sqlancer.common.schema.AbstractTables;
import sqlancer.turso.TursoGlobalState;
import sqlancer.turso.ast.TursoAggregate;
import sqlancer.turso.ast.TursoConstant;
import sqlancer.turso.ast.TursoExpression;
import sqlancer.turso.ast.TursoFunction;
import sqlancer.turso.ast.TursoRowValueExpression;
import sqlancer.turso.ast.TursoSelect;
import sqlancer.turso.ast.TursoUnaryOperation;
import sqlancer.turso.ast.TursoAggregate.TursoAggregateFunction;
import sqlancer.turso.ast.TursoCase.CasePair;
import sqlancer.turso.ast.TursoCase.TursoCaseWithBaseExpression;
import sqlancer.turso.ast.TursoCase.TursoCaseWithoutBaseExpression;
import sqlancer.turso.ast.TursoConstant.TursoTextConstant;
import sqlancer.turso.ast.TursoExpression.BetweenOperation;
import sqlancer.turso.ast.TursoExpression.BinaryComparisonOperation;
import sqlancer.turso.ast.TursoExpression.CollateOperation;
import sqlancer.turso.ast.TursoExpression.Join;
import sqlancer.turso.ast.TursoExpression.MatchOperation;
import sqlancer.turso.ast.TursoExpression.TursoBinaryOperation;
import sqlancer.turso.ast.TursoExpression.TursoColumnName;
import sqlancer.turso.ast.TursoExpression.TursoDistinct;
import sqlancer.turso.ast.TursoExpression.TursoOrderingTerm;
import sqlancer.turso.ast.TursoExpression.TursoPostfixText;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation;
import sqlancer.turso.ast.TursoExpression.TursoTableReference;
import sqlancer.turso.ast.TursoExpression.TypeLiteral;
import sqlancer.turso.ast.TursoExpression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.turso.ast.TursoExpression.Join.JoinType;
import sqlancer.turso.ast.TursoExpression.TursoBinaryOperation.BinaryOperator;
import sqlancer.turso.ast.TursoExpression.TursoOrderingTerm.Ordering;
import sqlancer.turso.ast.TursoExpression.TursoPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.turso.ast.TursoFunction.ComputableFunction;
import sqlancer.turso.ast.TursoUnaryOperation.UnaryOperator;
import sqlancer.turso.oracle.TursoRandomQuerySynthesizer;
import sqlancer.turso.schema.TursoSchema.TursoColumn;
import sqlancer.turso.schema.TursoSchema.TursoRowValue;
import sqlancer.turso.schema.TursoSchema.TursoTable;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public class TursoExpressionGenerator
    implements
        ExpressionGenerator<TursoExpression>,
        NoRECGenerator<
            TursoSelect,
            Join,
            TursoExpression,
            TursoTable,
            TursoColumn
        >,
        TLPWhereGenerator<
            TursoSelect,
            Join,
            TursoExpression,
            TursoTable,
            TursoColumn
        > {

    private TursoRowValue rw;
    private final TursoGlobalState globalState;
    private boolean tryToGenerateKnownResult;
    private List<TursoColumn> columns = Collections.emptyList();
    private List<TursoTable> targetTables;
    private final Randomly r;
    private boolean deterministicOnly;
    private boolean allowMatchClause;
    private boolean allowAggregateFunctions;
    private boolean allowSubqueries;
    private boolean allowAggreates;

    public TursoExpressionGenerator(TursoExpressionGenerator other) {
        this.rw = other.rw;
        this.globalState = other.globalState;
        this.tryToGenerateKnownResult = other.tryToGenerateKnownResult;
        this.columns = new ArrayList<>(other.columns);
        this.targetTables = other.targetTables;
        this.r = other.r;
        this.deterministicOnly = other.deterministicOnly;
        this.allowMatchClause = other.allowMatchClause;
        this.allowAggregateFunctions = other.allowAggregateFunctions;
        this.allowSubqueries = other.allowSubqueries;
        this.allowAggreates = other.allowAggreates;
    }

    private enum LiteralValueType {
        INTEGER,
        NUMERIC,
        STRING,
        BLOB_LITERAL,
        NULL,
    }

    public TursoExpressionGenerator(TursoGlobalState globalState) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public TursoExpressionGenerator deterministicOnly() {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.deterministicOnly = true;
        return gen;
    }

    public TursoExpressionGenerator allowAggregateFunctions() {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.allowAggregateFunctions = true;
        return gen;
    }

    public TursoExpressionGenerator setColumns(List<TursoColumn> columns) {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.columns = new ArrayList<>(columns);
        return gen;
    }

    public TursoExpressionGenerator setRowValue(TursoRowValue rw) {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.rw = rw;
        return gen;
    }

    public TursoExpressionGenerator allowMatchClause() {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.allowMatchClause = true;
        return gen;
    }

    public TursoExpressionGenerator allowSubqueries() {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.allowSubqueries = true;
        return gen;
    }

    public TursoExpressionGenerator tryToGenerateKnownResult() {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.tryToGenerateKnownResult = true;
        return gen;
    }

    public static TursoExpression getRandomLiteralValue(
        TursoGlobalState globalState
    ) {
        return new TursoExpressionGenerator(
            globalState
        ).getRandomLiteralValueInternal(globalState.getRandomly());
    }

    @Override
    public List<TursoExpression> generateOrderBys() {
        List<TursoExpression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(generateOrderingTerm());
        }
        return expressions;
    }

    public List<Join> getRandomJoinClauses(List<TursoTable> tables) {
        List<Join> joinStatements = new ArrayList<>();
        if (!globalState.getDbmsSpecificOptions().testJoins) {
            return joinStatements;
        }
        List<JoinType> options = new ArrayList<>(
            Arrays.asList(JoinType.values())
        );
        if (Randomly.getBoolean() && tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(
                0,
                tables.size()
            );
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) {
                TursoExpression joinClause = generateExpression();
                TursoTable table = Randomly.fromList(tables);
                tables.remove(table);
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    joinClause = null;
                }
                Join j = new TursoExpression.Join(
                    table,
                    joinClause,
                    selectedOption
                );
                joinStatements.add(j);
            }
        }
        return joinStatements;
    }

    public TursoExpression generateOrderingTerm() {
        TursoExpression expr = generateExpression();
        // COLLATE is potentially already generated
        if (Randomly.getBoolean()) {
            expr = new TursoOrderingTerm(expr, Ordering.getRandomValue());
        }
        if (
            globalState.getDbmsSpecificOptions().testNullsFirstLast &&
            Randomly.getBoolean()
        ) {
            expr = new TursoPostfixText(
                expr,
                Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
                null/* expr.getExpectedValue() */
            ) {
                @Override
                public boolean omitBracketsWhenPrinting() {
                    return true;
                }
            };
        }
        return expr;
    }

    /*
     * https://www.sqlite.org/syntax/literal-value.html
     */
    private TursoExpression getRandomLiteralValueInternal(Randomly r) {
        LiteralValueType randomLiteral = Randomly.fromOptions(
            LiteralValueType.values()
        );
        switch (randomLiteral) {
            case INTEGER:
                if (Randomly.getBoolean()) {
                    return TursoConstant.createIntConstant(
                        r.getInteger(),
                        Randomly.getBoolean()
                    );
                } else {
                    return TursoConstant.createTextConstant(
                        String.valueOf(r.getInteger())
                    );
                }
            case NUMERIC:
                return TursoConstant.createRealConstant(r.getDouble());
            case STRING:
                return TursoConstant.createTextConstant(r.getString());
            case BLOB_LITERAL:
                return TursoConstant.getRandomBinaryConstant(r);
            case NULL:
                return TursoConstant.createNullConstant();
            default:
                throw new AssertionError(randomLiteral);
        }
    }

    enum ExpressionType {
        RANDOM_QUERY,
        COLUMN_NAME,
        LITERAL_VALUE,
        UNARY_OPERATOR,
        POSTFIX_UNARY_OPERATOR,
        BINARY_OPERATOR,
        BETWEEN_OPERATOR,
        CAST_EXPRESSION,
        BINARY_COMPARISON_OPERATOR,
        FUNCTION,
        IN_OPERATOR,
        COLLATE,
        CASE_OPERATOR,
        MATCH,
        AGGREGATE_FUNCTION,
        ROW_VALUE_COMPARISON,
        AND_OR_CHAIN,
    }

    public TursoExpression generateExpression() {
        return getRandomExpression(0);
    }

    public List<TursoExpression> getRandomExpressions(int size) {
        List<TursoExpression> expressions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expressions.add(generateExpression());
        }
        return expressions;
    }

    public List<TursoExpression> getRandomExpressions(int size, int depth) {
        List<TursoExpression> expressions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expressions.add(getRandomExpression(depth));
        }
        return expressions;
    }

    public TursoExpression getRandomExpression(int depth) {
        if (allowAggreates && Randomly.getBoolean()) {
            return getAggregateFunction(depth + 1);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth()) {
            if (
                Randomly.getBooleanWithRatherLowProbability() ||
                columns.isEmpty()
            ) {
                return getRandomLiteralValue(globalState);
            } else {
                return getRandomColumn();
            }
        }

        List<ExpressionType> list = new ArrayList<>(
            Arrays.asList(ExpressionType.values())
        );
        if (columns.isEmpty()) {
            list.remove(ExpressionType.COLUMN_NAME);
        }
        if (!allowMatchClause) {
            list.remove(ExpressionType.MATCH);
        }
        if (!allowAggregateFunctions) {
            list.remove(ExpressionType.AGGREGATE_FUNCTION);
        }
        if (!allowSubqueries) {
            list.remove(ExpressionType.RANDOM_QUERY);
        }
        if (!globalState.getDbmsSpecificOptions().testFunctions) {
            list.remove(ExpressionType.FUNCTION);
        }
        if (!globalState.getDbmsSpecificOptions().testMatch) {
            list.remove(ExpressionType.MATCH);
        }
        if (!globalState.getDbmsSpecificOptions().testIn) {
            list.remove(ExpressionType.IN_OPERATOR);
        }
        ExpressionType randomExpressionType = Randomly.fromList(list);
        switch (randomExpressionType) {
            case AND_OR_CHAIN:
                return getAndOrChain(depth + 1);
            case LITERAL_VALUE:
                return getRandomLiteralValue(globalState);
            case COLUMN_NAME:
                return getRandomColumn();
            case UNARY_OPERATOR:
                return getRandomUnaryOperator(depth + 1);
            case POSTFIX_UNARY_OPERATOR:
                return getRandomPostfixUnaryOperator(depth + 1);
            case BINARY_OPERATOR:
                return getBinaryOperator(depth + 1);
            case BINARY_COMPARISON_OPERATOR:
                return getBinaryComparisonOperator(depth + 1);
            case BETWEEN_OPERATOR:
                return getBetweenOperator(depth + 1);
            case CAST_EXPRESSION:
                return getCastOperator(depth + 1);
            case FUNCTION:
                return getFunction(globalState, depth);
            case IN_OPERATOR:
                return getInOperator(depth + 1);
            case COLLATE:
                return new CollateOperation(
                    getRandomExpression(depth + 1),
                    TursoCollateSequence.random()
                );
            case CASE_OPERATOR:
                return getCaseOperator(depth + 1);
            case MATCH:
                return getMatchClause(depth);
            case AGGREGATE_FUNCTION:
                return getAggregateFunction(depth);
            case ROW_VALUE_COMPARISON:
                return getRowValueComparison(depth + 1);
            case RANDOM_QUERY:
                // TODO: pass schema from the outside
                // TODO: depth
                return TursoRandomQuerySynthesizer.generate(globalState, 1);
            default:
                throw new AssertionError(randomExpressionType);
        }
    }

    private TursoExpression getAndOrChain(int depth) {
        int num = Randomly.smallNumber() + 2;
        TursoExpression expr = getRandomExpression(depth + 1);
        for (int i = 0; i < num; i++) {
            BinaryOperator operator = Randomly.fromOptions(
                BinaryOperator.AND,
                BinaryOperator.OR
            );
            expr = new TursoBinaryOperation(
                expr,
                getRandomExpression(depth + 1),
                operator
            );
        }
        return expr;
    }

    public TursoExpression getAggregateFunction(boolean asWindowFunction) {
        TursoAggregateFunction random = TursoAggregateFunction.getRandom();
        if (asWindowFunction) {
            while (
                /* random == TursoAggregateFunction.ZIPFILE || */random ==
                    TursoAggregateFunction.MAX ||
                random == TursoAggregateFunction.MIN
            ) {
                // ZIPFILE() may not be used as a window function
                random = TursoAggregateFunction.getRandom();
            }
        }
        return getAggregate(0, random);
    }

    private TursoExpression getAggregateFunction(int depth) {
        TursoAggregateFunction random = TursoAggregateFunction.getRandom();
        return getAggregate(depth, random);
    }

    private TursoExpression getAggregate(
        int depth,
        TursoAggregateFunction random
    ) {
        int nrArgs;
        // if (random == TursoAggregateFunction.ZIPFILE) {
        // nrArgs = Randomly.fromOptions(2, 4);
        // } else {
        // nrArgs = 1;
        // }
        nrArgs = 1;
        return new TursoAggregate(
            getRandomExpressions(nrArgs, depth + 1),
            random
        );
    }

    private enum RowValueComparison {
        STANDARD_COMPARISON,
        BETWEEN,
        IN,
    }

    /*
     * https://www.sqlite.org/rowvalue.html
     */
    private TursoExpression getRowValueComparison(int depth) {
        int size = Randomly.smallNumber() + 1;
        List<TursoExpression> left = getRandomExpressions(size, depth + 1);
        List<TursoExpression> right = getRandomExpressions(size, depth + 1);
        RowValueComparison randomOption;
        // if (Randomly.getBooleanWithSmallProbability()) {
        // // for the right hand side a random query is required, which is expensive
        // randomOption = RowValueComparison.IN;
        // } else {
        randomOption = Randomly.fromOptions(
            RowValueComparison.STANDARD_COMPARISON,
            RowValueComparison.BETWEEN
        );
        // }
        switch (randomOption) {
            // TODO case
            case STANDARD_COMPARISON:
                return new BinaryComparisonOperation(
                    new TursoRowValueExpression(left),
                    new TursoRowValueExpression(right),
                    BinaryComparisonOperator.getRandomRowValueOperator()
                );
            case BETWEEN:
                return new BetweenOperation(
                    getRandomRowValue(depth + 1, size),
                    Randomly.getBoolean(),
                    new TursoRowValueExpression(left),
                    new TursoRowValueExpression(right)
                );
            // case IN:
            // return new TursoExpression.InOperation(new TursoRowValue(left),
            // TursoRandomQuerySynthesizer.generate(globalState, size));
            default:
                throw new AssertionError(randomOption);
        }
    }

    private TursoRowValueExpression getRandomRowValue(int depth, int size) {
        return new TursoRowValueExpression(
            getRandomExpressions(size, depth + 1)
        );
    }

    private TursoExpression getMatchClause(int depth) {
        TursoExpression left = getRandomExpression(depth + 1);
        TursoExpression right;
        if (Randomly.getBoolean()) {
            right = getRandomExpression(depth + 1);
        } else {
            right = TursoConstant.createTextConstant(
                TursoMatchStringGenerator.generateMatchString(r)
            );
        }
        return new MatchOperation(left, right);
    }

    private TursoExpression getRandomColumn() {
        TursoColumn c = Randomly.fromList(columns);
        return new TursoColumnName(
            c,
            rw == null ? null : rw.getValues().get(c)
        );
    }

    enum Attribute {
        VARIADIC,
        NONDETERMINISTIC,
    }

    private enum AnyFunction {
        ABS("ABS", 1), //
        CHAR("CHAR", 1, Attribute.VARIADIC), //
        COALESCE("COALESCE", 2, Attribute.VARIADIC), //
        GLOB("GLOB", 2), //
        HEX("HEX", 1), //
        IFNULL("IFNULL", 2), //
        INSTR("INSTR", 2), //
        LAST_INSERT_ROWID("LAST_INSERT_ROWID", 0, Attribute.NONDETERMINISTIC), //
        LENGTH("LENGTH", 1), //
        LIKE("LIKE", 2), //
        LIKE2("LIKE", 3) {
            @Override
            List<TursoExpression> generateArguments(
                int nrArgs,
                int depth,
                TursoExpressionGenerator gen
            ) {
                List<TursoExpression> args = super.generateArguments(
                    nrArgs,
                    depth,
                    gen
                );
                args.set(2, gen.getRandomSingleCharString());
                return args;
            }
        }, //
        LIKELIHOOD("LIKELIHOOD", 2), //
        LIKELY("LIKELY", 1), //
        LOWER("LOWER", 1), //
        LTRIM1("LTRIM", 1), //
        LTRIM2("LTRIM", 2), //
        MAX("MAX", 2, Attribute.VARIADIC), //
        MIN("MIN", 2, Attribute.VARIADIC), //
        NULLIF("NULLIF", 2), //
        QUOTE("QUOTE", 1), //
        ROUND("ROUND", 2), //
        RTRIM("RTRIM", 1), //
        SOUNDEX("soundex", 1), //
        SQLITE_SOURCE_ID("SQLITE_SOURCE_ID", 0, Attribute.NONDETERMINISTIC),
        SQLITE_VERSION("SQLITE_VERSION", 0, Attribute.NONDETERMINISTIC), //
        SUBSTR("SUBSTR", 2), //
        TRIM("TRIM", 1), //
        TYPEOF("TYPEOF", 1), //
        UNICODE("UNICODE", 1),
        UPPER("UPPER", 1),
        DATE("DATE", 3, Attribute.VARIADIC), //
        TIME("TIME", 3, Attribute.VARIADIC), //
        DATETIME("DATETIME", 3, Attribute.VARIADIC), //
        JULIANDAY("JULIANDAY", 3, Attribute.VARIADIC), //
        STRFTIME("STRFTIME", 3, Attribute.VARIADIC),
        // json functions
        JSON("json", 1), //
        JSON_ARRAY("json_array", 2, Attribute.VARIADIC),
        JSON_ARRAY_LENGTH("json_array_length", 1), //
        JSON_ARRAY_LENGTH2("json_array_length", 2), //
        JSON_EXTRACT("json_extract", 2, Attribute.VARIADIC),
        JSON_INSERT("json_insert", 3, Attribute.VARIADIC),
        JSON_OBJECT("json_object", 2, Attribute.VARIADIC),
        JSON_PATCH("json_patch", 2),
        JSON_REMOVE("json_remove", 2, Attribute.VARIADIC),
        JSON_TYPE("json_type", 1), //
        JSON_VALID("json_valid", 1), //
        JSON_QUOTE("json_quote", 1);

        private int minNrArgs;
        private boolean variadic;
        private boolean deterministic;
        private String name;

        AnyFunction(String name, int minNrArgs, Attribute... attributes) {
            this.name = name;
            List<Attribute> attrs = Arrays.asList(attributes);
            this.minNrArgs = minNrArgs;
            this.variadic = attrs.contains(Attribute.VARIADIC);
            this.deterministic = !attrs.contains(Attribute.NONDETERMINISTIC);
        }

        public boolean isVariadic() {
            return variadic;
        }

        public int getMinNrArgs() {
            return minNrArgs;
        }

        static AnyFunction getRandom(TursoGlobalState globalState) {
            return Randomly.fromList(getAllFunctions(globalState));
        }

        private static List<AnyFunction> getAllFunctions(
            TursoGlobalState globalState
        ) {
            List<AnyFunction> functions = new ArrayList<>(
                Arrays.asList(AnyFunction.values())
            );
            if (!globalState.getDbmsSpecificOptions().testSoundex) {
                boolean removed = functions.removeIf(f ->
                    f.name.equals("soundex")
                );
                if (!removed) {
                    throw new IllegalStateException();
                }
            }
            return functions;
        }

        static AnyFunction getRandomDeterministic(
            TursoGlobalState globalState
        ) {
            return Randomly.fromList(
                getAllFunctions(globalState)
                    .stream()
                    .filter(f -> f.deterministic)
                    .collect(Collectors.toList())
            );
        }

        @Override
        public String toString() {
            return name;
        }

        List<TursoExpression> generateArguments(
            int nrArgs,
            int depth,
            TursoExpressionGenerator gen
        ) {
            List<TursoExpression> expressions = new ArrayList<>();
            for (int i = 0; i < nrArgs; i++) {
                expressions.add(gen.getRandomExpression(depth + 1));
            }
            return expressions;
        }
    }

    private TursoExpression getFunction(
        TursoGlobalState globalState,
        int depth
    ) {
        if (tryToGenerateKnownResult || Randomly.getBoolean()) {
            return getComputableFunction(depth + 1);
        } else {
            AnyFunction randomFunction;
            if (deterministicOnly) {
                randomFunction = AnyFunction.getRandomDeterministic(
                    globalState
                );
            } else {
                randomFunction = AnyFunction.getRandom(globalState);
            }
            int nrArgs = randomFunction.getMinNrArgs();
            if (randomFunction.isVariadic()) {
                nrArgs += Randomly.smallNumber();
            }
            List<TursoExpression> expressions =
                randomFunction.generateArguments(nrArgs, depth + 1, this);
            // The second argument of LIKELIHOOD must be a float number within 0.0 -1.0
            if (randomFunction == AnyFunction.LIKELIHOOD) {
                TursoExpression lastArg = TursoConstant.createRealConstant(
                    Randomly.getPercentage()
                );
                expressions.remove(expressions.size() - 1);
                expressions.add(lastArg);
            }
            return new TursoExpression.Function(
                randomFunction.toString(),
                expressions.toArray(new TursoExpression[0])
            );
        }
    }

    protected TursoExpression getRandomSingleCharString() {
        String s;
        do {
            s = r.getString();
        } while (s.isEmpty());
        return new TursoTextConstant(String.valueOf(s.charAt(0)));
    }

    private TursoExpression getCaseOperator(int depth) {
        int nrCaseExpressions = 1 + Randomly.smallNumber();
        CasePair[] pairs = new CasePair[nrCaseExpressions];
        for (int i = 0; i < pairs.length; i++) {
            TursoExpression whenExpr = getRandomExpression(depth + 1);
            TursoExpression thenExpr = getRandomExpression(depth + 1);
            CasePair pair = new CasePair(whenExpr, thenExpr);
            pairs[i] = pair;
        }
        TursoExpression elseExpr;
        if (Randomly.getBoolean()) {
            elseExpr = getRandomExpression(depth + 1);
        } else {
            elseExpr = null;
        }
        if (Randomly.getBoolean()) {
            return new TursoCaseWithoutBaseExpression(pairs, elseExpr);
        } else {
            TursoExpression baseExpr = getRandomExpression(depth + 1);
            return new TursoCaseWithBaseExpression(baseExpr, pairs, elseExpr);
        }
    }

    private TursoExpression getCastOperator(int depth) {
        TursoExpression expr = getRandomExpression(depth + 1);
        TypeLiteral type = new TursoExpression.TypeLiteral(
            Randomly.fromOptions(TursoExpression.TypeLiteral.Type.values())
        );
        return new TursoExpression.Cast(type, expr);
    }

    private TursoExpression getComputableFunction(int depth) {
        ComputableFunction func = ComputableFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        TursoExpression[] args = new TursoExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = getRandomExpression(depth + 1);
            if (i == 0 && Randomly.getBoolean()) {
                args[i] = new TursoDistinct(args[i]);
            }
        }
        // The second argument of LIKELIHOOD must be a float number within 0.0 -1.0
        if (func == ComputableFunction.LIKELIHOOD) {
            TursoExpression lastArg = TursoConstant.createRealConstant(
                Randomly.getPercentage()
            );
            args[args.length - 1] = lastArg;
        }
        return new TursoFunction(func, args);
    }

    private TursoExpression getBetweenOperator(int depth) {
        boolean tr = Randomly.getBoolean();
        TursoExpression expr = getRandomExpression(depth + 1);
        TursoExpression left = getRandomExpression(depth + 1);
        TursoExpression right = getRandomExpression(depth + 1);
        return new TursoExpression.BetweenOperation(expr, tr, left, right);
    }

    // TODO: incomplete
    private TursoExpression getBinaryOperator(int depth) {
        TursoExpression leftExpression = getRandomExpression(depth + 1);
        // TODO: operators
        BinaryOperator operator = BinaryOperator.getRandomOperator();
        // while (operator == BinaryOperator.DIVIDE) {
        // operator = BinaryOperator.getRandomOperator();
        // }
        TursoExpression rightExpression = getRandomExpression(depth + 1);
        return new TursoExpression.TursoBinaryOperation(
            leftExpression,
            rightExpression,
            operator
        );
    }

    private TursoExpression getInOperator(int depth) {
        TursoExpression leftExpression = getRandomExpression(depth + 1);
        List<TursoExpression> right = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            right.add(getRandomExpression(depth + 1));
        }
        return new TursoExpression.InOperation(leftExpression, right);
    }

    private TursoExpression getBinaryComparisonOperator(int depth) {
        TursoExpression leftExpression = getRandomExpression(depth + 1);
        BinaryComparisonOperator operator =
            BinaryComparisonOperator.getRandomOperator();
        TursoExpression rightExpression = getRandomExpression(depth + 1);
        return new TursoExpression.BinaryComparisonOperation(
            leftExpression,
            rightExpression,
            operator
        );
    }

    // complete
    private TursoExpression getRandomPostfixUnaryOperator(int depth) {
        TursoExpression subExpression = getRandomExpression(depth + 1);
        PostfixUnaryOperator operator =
            PostfixUnaryOperator.getRandomOperator();
        return new TursoExpression.TursoPostfixUnaryOperation(
            operator,
            subExpression
        );
    }

    // complete
    public TursoExpression getRandomUnaryOperator(int depth) {
        TursoExpression subExpression = getRandomExpression(depth + 1);
        UnaryOperator unaryOperation = Randomly.fromOptions(
            UnaryOperator.values()
        );
        return new TursoUnaryOperation(unaryOperation, subExpression);
    }

    public TursoExpression getHavingClause() {
        allowAggreates = true;
        return generateExpression();
    }

    @Override
    public TursoExpression generatePredicate() {
        return generateExpression();
    }

    @Override
    public TursoExpression negatePredicate(TursoExpression predicate) {
        return new TursoUnaryOperation(UnaryOperator.NOT, predicate);
    }

    @Override
    public TursoExpression isNull(TursoExpression expr) {
        return new TursoPostfixUnaryOperation(
            PostfixUnaryOperator.ISNULL,
            expr
        );
    }

    public TursoExpression generateResultKnownExpression() {
        TursoExpression expr;
        do {
            expr = generateExpression();
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    @Override
    public TursoExpressionGenerator setTablesAndColumns(
        AbstractTables<TursoTable, TursoColumn> targetTables
    ) {
        TursoExpressionGenerator gen = new TursoExpressionGenerator(this);
        gen.targetTables = targetTables.getTables();
        gen.columns = targetTables.getColumns();
        return gen;
    }

    @Override
    public TursoExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public TursoSelect generateSelect() {
        return new TursoSelect();
    }

    @Override
    public List<Join> getRandomJoinClauses() {
        return getRandomJoinClauses(targetTables);
    }

    @Override
    public List<TursoExpression> getTableRefs() {
        List<TursoExpression> tableRefs = new ArrayList<>();
        for (TursoTable t : targetTables) {
            tableRefs.add(new TursoTableReference(t));
        }
        return tableRefs;
    }

    @Override
    public List<TursoExpression> generateFetchColumns(
        boolean shouldCreateDummy
    ) {
        List<TursoExpression> columns = new ArrayList<>();
        if (shouldCreateDummy && Randomly.getBoolean()) {
            columns.add(
                new TursoColumnName(TursoColumn.createDummy("*"), null)
            );
        } else {
            columns = Randomly.nonEmptySubset(this.columns)
                .stream()
                .map(c -> new TursoColumnName(c, null))
                .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    public String generateOptimizedQueryString(
        TursoSelect select,
        TursoExpression whereCondition,
        boolean shouldUseAggregate
    ) {
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(generateOrderBys());
        }
        if (shouldUseAggregate) {
            select.setFetchColumns(
                Arrays.asList(
                    new TursoAggregate(
                        Collections.emptyList(),
                        TursoAggregate.TursoAggregateFunction.COUNT_ALL
                    )
                )
            );
        } else {
            TursoColumnName aggr = new TursoColumnName(
                TursoColumn.createDummy("*"),
                null
            );
            select.setFetchColumns(Arrays.asList(aggr));
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(
        TursoSelect select,
        TursoExpression whereCondition
    ) {
        TursoPostfixUnaryOperation isTrue = new TursoPostfixUnaryOperation(
            PostfixUnaryOperator.IS_TRUE,
            whereCondition
        );
        TursoPostfixText asText = new TursoPostfixText(
            isTrue,
            " as count",
            null
        );
        select.setFetchColumns(Arrays.asList(asText));
        select.setWhereClause(null);

        return "SELECT SUM(count) FROM (" + select.asString() + ")";
    }
}
