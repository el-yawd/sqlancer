package sqlancer.limbo.gen;

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
import sqlancer.limbo.LimboGlobalState;
import sqlancer.limbo.ast.LimboAggregate;
import sqlancer.limbo.ast.LimboAggregate.LimboAggregateFunction;
import sqlancer.limbo.ast.LimboCase.CasePair;
import sqlancer.limbo.ast.LimboCase.LimboCaseWithBaseExpression;
import sqlancer.limbo.ast.LimboCase.LimboCaseWithoutBaseExpression;
import sqlancer.limbo.ast.LimboConstant;
import sqlancer.limbo.ast.LimboConstant.LimboTextConstant;
import sqlancer.limbo.ast.LimboExpression;
import sqlancer.limbo.ast.LimboExpression.BetweenOperation;
import sqlancer.limbo.ast.LimboExpression.BinaryComparisonOperation;
import sqlancer.limbo.ast.LimboExpression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.limbo.ast.LimboExpression.CollateOperation;
import sqlancer.limbo.ast.LimboExpression.Join;
import sqlancer.limbo.ast.LimboExpression.Join.JoinType;
import sqlancer.limbo.ast.LimboExpression.MatchOperation;
import sqlancer.limbo.ast.LimboExpression.LimboColumnName;
import sqlancer.limbo.ast.LimboExpression.LimboDistinct;
import sqlancer.limbo.ast.LimboExpression.LimboOrderingTerm;
import sqlancer.limbo.ast.LimboExpression.LimboOrderingTerm.Ordering;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixText;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation;
import sqlancer.limbo.ast.LimboExpression.LimboPostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.limbo.ast.LimboExpression.LimboTableReference;
import sqlancer.limbo.ast.LimboExpression.LimboBinaryOperation;
import sqlancer.limbo.ast.LimboExpression.LimboBinaryOperation.BinaryOperator;
import sqlancer.limbo.ast.LimboExpression.TypeLiteral;
import sqlancer.limbo.ast.LimboFunction;
import sqlancer.limbo.ast.LimboFunction.ComputableFunction;
import sqlancer.limbo.ast.LimboRowValueExpression;
import sqlancer.limbo.ast.LimboSelect;
import sqlancer.limbo.ast.LimboUnaryOperation;
import sqlancer.limbo.ast.LimboUnaryOperation.UnaryOperator;
import sqlancer.limbo.oracle.LimboRandomQuerySynthesizer;
import sqlancer.limbo.schema.LimboSchema.LimboColumn;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;
import sqlancer.limbo.schema.LimboSchema.LimboRowValue;
import sqlancer.limbo.schema.LimboSchema.LimboTable;

public class LimboExpressionGenerator implements ExpressionGenerator<LimboExpression>,
        NoRECGenerator<LimboSelect, Join, LimboExpression, LimboTable, LimboColumn>,
        TLPWhereGenerator<LimboSelect, Join, LimboExpression, LimboTable, LimboColumn> {

    private LimboRowValue rw;
    private final LimboGlobalState globalState;
    private boolean tryToGenerateKnownResult;
    private List<LimboColumn> columns = Collections.emptyList();
    private List<LimboTable> targetTables;
    private final Randomly r;
    private boolean deterministicOnly;
    private boolean allowMatchClause;
    private boolean allowAggregateFunctions;
    private boolean allowSubqueries;
    private boolean allowAggreates;

    public LimboExpressionGenerator(LimboExpressionGenerator other) {
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
        INTEGER, NUMERIC, STRING, BLOB_LITERAL, NULL
    }

    public LimboExpressionGenerator(LimboGlobalState globalState) {
        this.globalState = globalState;
        this.r = globalState.getRandomly();
    }

    public LimboExpressionGenerator deterministicOnly() {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.deterministicOnly = true;
        return gen;
    }

    public LimboExpressionGenerator allowAggregateFunctions() {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.allowAggregateFunctions = true;
        return gen;
    }

    public LimboExpressionGenerator setColumns(List<LimboColumn> columns) {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.columns = new ArrayList<>(columns);
        return gen;
    }

    public LimboExpressionGenerator setRowValue(LimboRowValue rw) {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.rw = rw;
        return gen;
    }

    public LimboExpressionGenerator allowMatchClause() {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.allowMatchClause = true;
        return gen;
    }

    public LimboExpressionGenerator allowSubqueries() {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.allowSubqueries = true;
        return gen;
    }

    public LimboExpressionGenerator tryToGenerateKnownResult() {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.tryToGenerateKnownResult = true;
        return gen;
    }

    public static LimboExpression getRandomLiteralValue(LimboGlobalState globalState) {
        return new LimboExpressionGenerator(globalState).getRandomLiteralValueInternal(globalState.getRandomly());
    }

    @Override
    public List<LimboExpression> generateOrderBys() {
        List<LimboExpression> expressions = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            expressions.add(generateOrderingTerm());
        }
        return expressions;
    }

    public List<Join> getRandomJoinClauses(List<LimboTable> tables) {
        List<Join> joinStatements = new ArrayList<>();
        if (!globalState.getDbmsSpecificOptions().testJoins) {
            return joinStatements;
        }
        List<JoinType> options = new ArrayList<>(Arrays.asList(JoinType.values()));
        if (Randomly.getBoolean() && tables.size() > 1) {
            int nrJoinClauses = (int) Randomly.getNotCachedInteger(0, tables.size());
            // Natural join is incompatible with other joins
            // because it needs unique column names
            // while other joins will produce duplicate column names
            if (nrJoinClauses > 1) {
                options.remove(JoinType.NATURAL);
            }
            for (int i = 0; i < nrJoinClauses; i++) {
                LimboExpression joinClause = generateExpression();
                LimboTable table = Randomly.fromList(tables);
                tables.remove(table);
                JoinType selectedOption = Randomly.fromList(options);
                if (selectedOption == JoinType.NATURAL) {
                    // NATURAL joins do not have an ON clause
                    joinClause = null;
                }
                Join j = new LimboExpression.Join(table, joinClause, selectedOption);
                joinStatements.add(j);
            }

        }
        return joinStatements;
    }

    public LimboExpression generateOrderingTerm() {
        LimboExpression expr = generateExpression();
        // COLLATE is potentially already generated
        if (Randomly.getBoolean()) {
            expr = new LimboOrderingTerm(expr, Ordering.getRandomValue());
        }
        if (globalState.getDbmsSpecificOptions().testNullsFirstLast && Randomly.getBoolean()) {
            expr = new LimboPostfixText(expr, Randomly.fromOptions(" NULLS FIRST", " NULLS LAST"),
                    null /* expr.getExpectedValue() */) {
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
    private LimboExpression getRandomLiteralValueInternal(Randomly r) {
        LiteralValueType randomLiteral = Randomly.fromOptions(LiteralValueType.values());
        switch (randomLiteral) {
        case INTEGER:
            if (Randomly.getBoolean()) {
                return LimboConstant.createIntConstant(r.getInteger(), Randomly.getBoolean());
            } else {
                return LimboConstant.createTextConstant(String.valueOf(r.getInteger()));
            }
        case NUMERIC:
            return LimboConstant.createRealConstant(r.getDouble());
        case STRING:
            return LimboConstant.createTextConstant(r.getString());
        case BLOB_LITERAL:
            return LimboConstant.getRandomBinaryConstant(r);
        case NULL:
            return LimboConstant.createNullConstant();
        default:
            throw new AssertionError(randomLiteral);
        }
    }

    enum ExpressionType {
        RANDOM_QUERY, COLUMN_NAME, LITERAL_VALUE, UNARY_OPERATOR, POSTFIX_UNARY_OPERATOR, BINARY_OPERATOR,
        BETWEEN_OPERATOR, CAST_EXPRESSION, BINARY_COMPARISON_OPERATOR, FUNCTION, IN_OPERATOR, COLLATE, CASE_OPERATOR,
        MATCH, AGGREGATE_FUNCTION, ROW_VALUE_COMPARISON, AND_OR_CHAIN
    }

    public LimboExpression generateExpression() {
        return getRandomExpression(0);
    }

    public List<LimboExpression> getRandomExpressions(int size) {
        List<LimboExpression> expressions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expressions.add(generateExpression());
        }
        return expressions;
    }

    public List<LimboExpression> getRandomExpressions(int size, int depth) {
        List<LimboExpression> expressions = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            expressions.add(getRandomExpression(depth));
        }
        return expressions;
    }

    public LimboExpression getRandomExpression(int depth) {
        if (allowAggreates && Randomly.getBoolean()) {
            return getAggregateFunction(depth + 1);
        }
        if (depth >= globalState.getOptions().getMaxExpressionDepth()) {
            if (Randomly.getBooleanWithRatherLowProbability() || columns.isEmpty()) {
                return getRandomLiteralValue(globalState);
            } else {
                return getRandomColumn();
            }
        }

        List<ExpressionType> list = new ArrayList<>(Arrays.asList(ExpressionType.values()));
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
            return new CollateOperation(getRandomExpression(depth + 1), LimboCollateSequence.random());
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
            return LimboRandomQuerySynthesizer.generate(globalState, 1);
        default:
            throw new AssertionError(randomExpressionType);
        }
    }

    private LimboExpression getAndOrChain(int depth) {
        int num = Randomly.smallNumber() + 2;
        LimboExpression expr = getRandomExpression(depth + 1);
        for (int i = 0; i < num; i++) {
            BinaryOperator operator = Randomly.fromOptions(BinaryOperator.AND, BinaryOperator.OR);
            expr = new LimboBinaryOperation(expr, getRandomExpression(depth + 1), operator);
        }
        return expr;
    }

    public LimboExpression getAggregateFunction(boolean asWindowFunction) {
        LimboAggregateFunction random = LimboAggregateFunction.getRandom();
        if (asWindowFunction) {
            while (/* random == LimboAggregateFunction.ZIPFILE || */random == LimboAggregateFunction.MAX
                    || random == LimboAggregateFunction.MIN) {
                // ZIPFILE() may not be used as a window function
                random = LimboAggregateFunction.getRandom();
            }
        }
        return getAggregate(0, random);
    }

    private LimboExpression getAggregateFunction(int depth) {
        LimboAggregateFunction random = LimboAggregateFunction.getRandom();
        return getAggregate(depth, random);
    }

    private LimboExpression getAggregate(int depth, LimboAggregateFunction random) {
        int nrArgs;
        // if (random == LimboAggregateFunction.ZIPFILE) {
        // nrArgs = Randomly.fromOptions(2, 4);
        // } else {
        // nrArgs = 1;
        // }
        nrArgs = 1;
        return new LimboAggregate(getRandomExpressions(nrArgs, depth + 1), random);
    }

    private enum RowValueComparison {
        STANDARD_COMPARISON, BETWEEN, IN
    }

    /*
     * https://www.sqlite.org/rowvalue.html
     */
    private LimboExpression getRowValueComparison(int depth) {
        int size = Randomly.smallNumber() + 1;
        List<LimboExpression> left = getRandomExpressions(size, depth + 1);
        List<LimboExpression> right = getRandomExpressions(size, depth + 1);
        RowValueComparison randomOption;
        // if (Randomly.getBooleanWithSmallProbability()) {
        // // for the right hand side a random query is required, which is expensive
        // randomOption = RowValueComparison.IN;
        // } else {
        randomOption = Randomly.fromOptions(RowValueComparison.STANDARD_COMPARISON, RowValueComparison.BETWEEN);
        // }
        switch (randomOption) {
        // TODO case
        case STANDARD_COMPARISON:
            return new BinaryComparisonOperation(new LimboRowValueExpression(left),
                    new LimboRowValueExpression(right), BinaryComparisonOperator.getRandomRowValueOperator());
        case BETWEEN:
            return new BetweenOperation(getRandomRowValue(depth + 1, size), Randomly.getBoolean(),
                    new LimboRowValueExpression(left), new LimboRowValueExpression(right));
        // case IN:
        // return new LimboExpression.InOperation(new LimboRowValue(left),
        // LimboRandomQuerySynthesizer.generate(globalState, size));
        default:
            throw new AssertionError(randomOption);
        }
    }

    private LimboRowValueExpression getRandomRowValue(int depth, int size) {
        return new LimboRowValueExpression(getRandomExpressions(size, depth + 1));
    }

    private LimboExpression getMatchClause(int depth) {
        LimboExpression left = getRandomExpression(depth + 1);
        LimboExpression right;
        if (Randomly.getBoolean()) {
            right = getRandomExpression(depth + 1);
        } else {
            right = LimboConstant.createTextConstant(LimboMatchStringGenerator.generateMatchString(r));
        }
        return new MatchOperation(left, right);
    }

    private LimboExpression getRandomColumn() {
        LimboColumn c = Randomly.fromList(columns);
        return new LimboColumnName(c, rw == null ? null : rw.getValues().get(c));
    }

    enum Attribute {
        VARIADIC, NONDETERMINISTIC
    };

    private enum AnyFunction {
        ABS("ABS", 1), //
        CHANGES("CHANGES", 0, Attribute.NONDETERMINISTIC), //
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
            List<LimboExpression> generateArguments(int nrArgs, int depth, LimboExpressionGenerator gen) {
                List<LimboExpression> args = super.generateArguments(nrArgs, depth, gen);
                args.set(2, gen.getRandomSingleCharString());
                return args;
            }
        }, //
        LIKELIHOOD("LIKELIHOOD", 2), //
        LIKELY("LIKELY", 1), //
        LOAD_EXTENSION("load_extension", 1), //
        LOAD_EXTENSION2("load_extension", 2, Attribute.NONDETERMINISTIC), LOWER("LOWER", 1), //
        LTRIM1("LTRIM", 1), //
        LTRIM2("LTRIM", 2), //
        MAX("MAX", 2, Attribute.VARIADIC), //
        MIN("MIN", 2, Attribute.VARIADIC), //
        NULLIF("NULLIF", 2), //
        PRINTF("PRINTF", 1, Attribute.VARIADIC), //
        QUOTE("QUOTE", 1), //
        ROUND("ROUND", 2), //
        RTRIM("RTRIM", 1), //
        SOUNDEX("soundex", 1), //
        SQLITE_COMPILEOPTION_GET("SQLITE_COMPILEOPTION_GET", 1, Attribute.NONDETERMINISTIC), //
        SQLITE_COMPILEOPTION_USED("SQLITE_COMPILEOPTION_USED", 1, Attribute.NONDETERMINISTIC), //
        // SQLITE_OFFSET(1), //
        SQLITE_SOURCE_ID("SQLITE_SOURCE_ID", 0, Attribute.NONDETERMINISTIC),
        SQLITE_VERSION("SQLITE_VERSION", 0, Attribute.NONDETERMINISTIC), //
        SUBSTR("SUBSTR", 2), //
        TOTAL_CHANGES("TOTAL_CHANGES", 0, Attribute.NONDETERMINISTIC), //
        TRIM("TRIM", 1), //
        TYPEOF("TYPEOF", 1), //
        UNICODE("UNICODE", 1), UNLIKELY("UNLIKELY", 1), //
        UPPER("UPPER", 1), // "ZEROBLOB"
        // ZEROBLOB("ZEROBLOB", 1),
        DATE("DATE", 3, Attribute.VARIADIC), //
        TIME("TIME", 3, Attribute.VARIADIC), //
        DATETIME("DATETIME", 3, Attribute.VARIADIC), //
        JULIANDAY("JULIANDAY", 3, Attribute.VARIADIC), //
        STRFTIME("STRFTIME", 3, Attribute.VARIADIC),
        // json functions
        JSON("json", 1), //
        JSON_ARRAY("json_array", 2, Attribute.VARIADIC), JSON_ARRAY_LENGTH("json_array_length", 1), //
        JSON_ARRAY_LENGTH2("json_array_length", 2), //
        JSON_EXTRACT("json_extract", 2, Attribute.VARIADIC), JSON_INSERT("json_insert", 3, Attribute.VARIADIC),
        JSON_OBJECT("json_object", 2, Attribute.VARIADIC), JSON_PATCH("json_patch", 2),
        JSON_REMOVE("json_remove", 2, Attribute.VARIADIC), JSON_TYPE("json_type", 1), //
        JSON_VALID("json_valid", 1), //
        JSON_QUOTE("json_quote", 1), //

        RTREENODE("rtreenode", 2),

        // FTS
        HIGHLIGHT("highlight", 4);

        // testing functions
        // EXPR_COMPARE("expr_compare", 2), EXPR_IMPLIES_EXPR("expr_implies_expr", 2);

        // fts5_decode("fts5_decode", 2),
        // fts5_decode_none("fts5_decode_none", 2),
        // fts5_expr("fts5_expr", 1),
        // fts5_expr_tcl("fts5_expr_tcl", 1),
        // fts5_fold("fts5_fold", 1),
        // fts5_isalnum("fts5_isalnum", 1);

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

        static AnyFunction getRandom(LimboGlobalState globalState) {
            return Randomly.fromList(getAllFunctions(globalState));
        }

        private static List<AnyFunction> getAllFunctions(LimboGlobalState globalState) {
            List<AnyFunction> functions = new ArrayList<>(Arrays.asList(AnyFunction.values()));
            if (!globalState.getDbmsSpecificOptions().testSoundex) {
                boolean removed = functions.removeIf(f -> f.name.equals("soundex"));
                if (!removed) {
                    throw new IllegalStateException();
                }
            }
            return functions;
        }

        static AnyFunction getRandomDeterministic(LimboGlobalState globalState) {
            return Randomly.fromList(
                    getAllFunctions(globalState).stream().filter(f -> f.deterministic).collect(Collectors.toList()));
        }

        @Override
        public String toString() {
            return name;
        }

        List<LimboExpression> generateArguments(int nrArgs, int depth, LimboExpressionGenerator gen) {
            List<LimboExpression> expressions = new ArrayList<>();
            for (int i = 0; i < nrArgs; i++) {
                expressions.add(gen.getRandomExpression(depth + 1));
            }
            return expressions;
        }
    }

    private LimboExpression getFunction(LimboGlobalState globalState, int depth) {
        if (tryToGenerateKnownResult || Randomly.getBoolean()) {
            return getComputableFunction(depth + 1);
        } else {
            AnyFunction randomFunction;
            if (deterministicOnly) {
                randomFunction = AnyFunction.getRandomDeterministic(globalState);
            } else {
                randomFunction = AnyFunction.getRandom(globalState);
            }
            int nrArgs = randomFunction.getMinNrArgs();
            if (randomFunction.isVariadic()) {
                nrArgs += Randomly.smallNumber();
            }
            List<LimboExpression> expressions = randomFunction.generateArguments(nrArgs, depth + 1, this);
            // The second argument of LIKELIHOOD must be a float number within 0.0 -1.0
            if (randomFunction == AnyFunction.LIKELIHOOD) {
                LimboExpression lastArg = LimboConstant.createRealConstant(Randomly.getPercentage());
                expressions.remove(expressions.size() - 1);
                expressions.add(lastArg);
            }
            return new LimboExpression.Function(randomFunction.toString(),
                    expressions.toArray(new LimboExpression[0]));
        }

    }

    protected LimboExpression getRandomSingleCharString() {
        String s;
        do {
            s = r.getString();
        } while (s.isEmpty());
        return new LimboTextConstant(String.valueOf(s.charAt(0)));
    }

    private LimboExpression getCaseOperator(int depth) {
        int nrCaseExpressions = 1 + Randomly.smallNumber();
        CasePair[] pairs = new CasePair[nrCaseExpressions];
        for (int i = 0; i < pairs.length; i++) {
            LimboExpression whenExpr = getRandomExpression(depth + 1);
            LimboExpression thenExpr = getRandomExpression(depth + 1);
            CasePair pair = new CasePair(whenExpr, thenExpr);
            pairs[i] = pair;
        }
        LimboExpression elseExpr;
        if (Randomly.getBoolean()) {
            elseExpr = getRandomExpression(depth + 1);
        } else {
            elseExpr = null;
        }
        if (Randomly.getBoolean()) {
            return new LimboCaseWithoutBaseExpression(pairs, elseExpr);
        } else {
            LimboExpression baseExpr = getRandomExpression(depth + 1);
            return new LimboCaseWithBaseExpression(baseExpr, pairs, elseExpr);
        }
    }

    private LimboExpression getCastOperator(int depth) {
        LimboExpression expr = getRandomExpression(depth + 1);
        TypeLiteral type = new LimboExpression.TypeLiteral(
                Randomly.fromOptions(LimboExpression.TypeLiteral.Type.values()));
        return new LimboExpression.Cast(type, expr);
    }

    private LimboExpression getComputableFunction(int depth) {
        ComputableFunction func = ComputableFunction.getRandomFunction();
        int nrArgs = func.getNrArgs();
        if (func.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        LimboExpression[] args = new LimboExpression[nrArgs];
        for (int i = 0; i < args.length; i++) {
            args[i] = getRandomExpression(depth + 1);
            if (i == 0 && Randomly.getBoolean()) {
                args[i] = new LimboDistinct(args[i]);
            }
        }
        // The second argument of LIKELIHOOD must be a float number within 0.0 -1.0
        if (func == ComputableFunction.LIKELIHOOD) {
            LimboExpression lastArg = LimboConstant.createRealConstant(Randomly.getPercentage());
            args[args.length - 1] = lastArg;
        }
        return new LimboFunction(func, args);
    }

    private LimboExpression getBetweenOperator(int depth) {
        boolean tr = Randomly.getBoolean();
        LimboExpression expr = getRandomExpression(depth + 1);
        LimboExpression left = getRandomExpression(depth + 1);
        LimboExpression right = getRandomExpression(depth + 1);
        return new LimboExpression.BetweenOperation(expr, tr, left, right);
    }

    // TODO: incomplete
    private LimboExpression getBinaryOperator(int depth) {
        LimboExpression leftExpression = getRandomExpression(depth + 1);
        // TODO: operators
        BinaryOperator operator = BinaryOperator.getRandomOperator();
        // while (operator == BinaryOperator.DIVIDE) {
        // operator = BinaryOperator.getRandomOperator();
        // }
        LimboExpression rightExpression = getRandomExpression(depth + 1);
        return new LimboExpression.LimboBinaryOperation(leftExpression, rightExpression, operator);
    }

    private LimboExpression getInOperator(int depth) {
        LimboExpression leftExpression = getRandomExpression(depth + 1);
        List<LimboExpression> right = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            right.add(getRandomExpression(depth + 1));
        }
        return new LimboExpression.InOperation(leftExpression, right);
    }

    private LimboExpression getBinaryComparisonOperator(int depth) {
        LimboExpression leftExpression = getRandomExpression(depth + 1);
        BinaryComparisonOperator operator = BinaryComparisonOperator.getRandomOperator();
        LimboExpression rightExpression = getRandomExpression(depth + 1);
        return new LimboExpression.BinaryComparisonOperation(leftExpression, rightExpression, operator);
    }

    // complete
    private LimboExpression getRandomPostfixUnaryOperator(int depth) {
        LimboExpression subExpression = getRandomExpression(depth + 1);
        PostfixUnaryOperator operator = PostfixUnaryOperator.getRandomOperator();
        return new LimboExpression.LimboPostfixUnaryOperation(operator, subExpression);
    }

    // complete
    public LimboExpression getRandomUnaryOperator(int depth) {
        LimboExpression subExpression = getRandomExpression(depth + 1);
        UnaryOperator unaryOperation = Randomly.fromOptions(UnaryOperator.values());
        return new LimboUnaryOperation(unaryOperation, subExpression);
    }

    public LimboExpression getHavingClause() {
        allowAggreates = true;
        return generateExpression();
    }

    @Override
    public LimboExpression generatePredicate() {
        return generateExpression();
    }

    @Override
    public LimboExpression negatePredicate(LimboExpression predicate) {
        return new LimboUnaryOperation(UnaryOperator.NOT, predicate);
    }

    @Override
    public LimboExpression isNull(LimboExpression expr) {
        return new LimboPostfixUnaryOperation(PostfixUnaryOperator.ISNULL, expr);
    }

    public LimboExpression generateResultKnownExpression() {
        LimboExpression expr;
        do {
            expr = generateExpression();
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    @Override
    public LimboExpressionGenerator setTablesAndColumns(AbstractTables<LimboTable, LimboColumn> targetTables) {
        LimboExpressionGenerator gen = new LimboExpressionGenerator(this);
        gen.targetTables = targetTables.getTables();
        gen.columns = targetTables.getColumns();
        return gen;
    }

    @Override
    public LimboExpression generateBooleanExpression() {
        return generateExpression();
    }

    @Override
    public LimboSelect generateSelect() {
        return new LimboSelect();
    }

    @Override
    public List<Join> getRandomJoinClauses() {
        return getRandomJoinClauses(targetTables);
    }

    @Override
    public List<LimboExpression> getTableRefs() {
        List<LimboExpression> tableRefs = new ArrayList<>();
        for (LimboTable t : targetTables) {
            LimboTableReference tableRef;
            if (Randomly.getBooleanWithSmallProbability() && !globalState.getSchema().getIndexNames().isEmpty()) {
                tableRef = new LimboTableReference(globalState.getSchema().getRandomIndexOrBailout(), t);
            } else {
                tableRef = new LimboTableReference(t);
            }
            tableRefs.add(tableRef);
        }
        return tableRefs;
    }

    @Override
    public List<LimboExpression> generateFetchColumns(boolean shouldCreateDummy) {
        List<LimboExpression> columns = new ArrayList<>();
        if (shouldCreateDummy && Randomly.getBoolean()) {
            columns.add(new LimboColumnName(LimboColumn.createDummy("*"), null));
        } else {
            columns = Randomly.nonEmptySubset(this.columns).stream().map(c -> new LimboColumnName(c, null))
                    .collect(Collectors.toList());
        }
        return columns;
    }

    @Override
    public String generateOptimizedQueryString(LimboSelect select, LimboExpression whereCondition,
            boolean shouldUseAggregate) {
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(generateOrderBys());
        }
        if (shouldUseAggregate) {
            select.setFetchColumns(Arrays.asList(new LimboAggregate(Collections.emptyList(),
                    LimboAggregate.LimboAggregateFunction.COUNT_ALL)));
        } else {
            LimboColumnName aggr = new LimboColumnName(LimboColumn.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        select.setWhereClause(whereCondition);

        return select.asString();
    }

    @Override
    public String generateUnoptimizedQueryString(LimboSelect select, LimboExpression whereCondition) {
        LimboPostfixUnaryOperation isTrue = new LimboPostfixUnaryOperation(PostfixUnaryOperator.IS_TRUE,
                whereCondition);
        LimboPostfixText asText = new LimboPostfixText(isTrue, " as count", null);
        select.setFetchColumns(Arrays.asList(asText));
        select.setWhereClause(null);

        return "SELECT SUM(count) FROM (" + select.asString() + ")";
    }
}
