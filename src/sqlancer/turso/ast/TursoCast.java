package sqlancer.turso.ast;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Optional;
import java.util.regex.Pattern;

import sqlancer.IgnoreMeException;
import sqlancer.turso.schema.TursoDataType;

public final class TursoCast {

    private static final double MAX_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED = Math.pow(2, 51 - 1) - 1;
    private static final double MIN_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED = -Math.pow(2, 51 - 1);
    public static final Charset DEFAULT_ENCODING = Charset.forName("UTF-8");

    private static final byte FILE_SEPARATOR = 0x1c;
    private static final byte GROUP_SEPARATOR = 0x1d;
    private static final byte RECORD_SEPARATOR = 0x1e;
    private static final byte UNIT_SEPARATOR = 0x1f;
    private static final byte SYNCHRONOUS_IDLE = 0x16;

    static Connection castDatabase;

    private TursoCast() {
    }

    public static Optional<Boolean> isTrue(TursoConstant value) {
        TursoConstant numericValue;
        if (value.getDataType() == TursoDataType.NULL) {
            return Optional.empty();
        }
        if (value.getDataType() == TursoDataType.TEXT || value.getDataType() == TursoDataType.BINARY) {
            numericValue = castToNumeric(value);
        } else {
            numericValue = value;
        }
        assert numericValue.getDataType() != TursoDataType.TEXT : numericValue + "should have been converted";
        switch (numericValue.getDataType()) {
        case INT:
            return Optional.of(numericValue.asInt() != 0);
        case REAL:
            double doubleVal = numericValue.asDouble();
            return Optional.of(doubleVal != 0 && !Double.isNaN(doubleVal));
        default:
            throw new AssertionError(numericValue);
        }
    }

    public static void checkDoubleIsInsideDangerousRange(double doubleVal) {
        // high double-values might result in small rounding differences between Java and SQLite
        if (Math.abs(doubleVal) > 1e15) {
            throw new IgnoreMeException();
        }
    }

    // SELECT CAST('-1.370998801E9' AS INTEGER) == -1
    public static TursoConstant castToInt(TursoConstant originalCons) {
        TursoConstant cons = originalCons;
        if (cons.getDataType() == TursoDataType.BINARY) {
            String text = new String(cons.asBinary(), DEFAULT_ENCODING);
            cons = TursoConstant.createTextConstant(text);
        }
        switch (cons.getDataType()) {
        case NULL:
            return TursoConstant.createNullConstant();
        case INT:
            return cons;
        case REAL:
            checkDoubleIsInsideDangerousRange(cons.asDouble());
            return TursoConstant.createIntConstant((long) cons.asDouble());
        case TEXT:
            String asString = cons.asString();
            while (startsWithWhitespace(asString)) {
                asString = asString.substring(1);
            }
            if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
                return TursoConstant.createIntConstant(0);
            }
            for (int i = asString.length(); i >= 0; i--) {
                try {
                    String substring = asString.substring(0, i);
                    Pattern p = Pattern.compile("[+-]?\\d\\d*");
                    if (p.matcher(substring).matches()) {
                        BigDecimal bg = new BigDecimal(substring);
                        long result;
                        try {
                            result = bg.longValueExact();
                        } catch (ArithmeticException e) {
                            if (substring.startsWith("-")) {
                                result = Long.MIN_VALUE;
                            } else {
                                result = Long.MAX_VALUE;
                            }
                        }
                        return TursoConstant.createIntConstant(result);
                    }
                } catch (Exception e) {

                }
            }
            return TursoConstant.createIntConstant(0);
        default:
            throw new AssertionError();
        }

    }

    public static TursoConstant castToReal(TursoConstant cons) {
        TursoConstant numericValue = castToNumeric(cons);
        if (numericValue.getDataType() == TursoDataType.INT) {
            double val = numericValue.asInt();
            checkDoubleIsInsideDangerousRange(val);
            return TursoConstant.createRealConstant(val);
        } else {
            return numericValue;
        }
    }

    public static TursoConstant castToNumericNoNumAsRealZero(TursoConstant value) {
        return convertInternal(value, false, true, true);
    }

    public static TursoConstant castToNumericFromNumOperand(TursoConstant value) {
        return convertInternal(value, false, false, false);
    }

    /*
     * Applies numeric affinity to a value.
     */
    public static TursoConstant castToNumeric(TursoConstant value) {
        return convertInternal(value, true, false, false);
    }

    private static TursoConstant convertInternal(TursoConstant originalValue, boolean convertRealToInt,
            boolean noNumIsRealZero, boolean convertIntToReal) throws AssertionError {
        TursoConstant value = originalValue;
        if (value.getDataType() == TursoDataType.BINARY) {
            String text = new String(value.asBinary(), DEFAULT_ENCODING);
            value = TursoConstant.createTextConstant(text);
        }
        switch (value.getDataType()) {
        case NULL:
            return TursoConstant.createNullConstant();
        case INT:
        case REAL:
            return value;
        case TEXT:
            String asString = value.asString();
            while (startsWithWhitespace(asString)) {
                asString = asString.substring(1);
            }
            if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
                return TursoConstant.createIntConstant(0);
            }
            if (asString.toLowerCase().startsWith("-infinity") || asString.toLowerCase().startsWith("infinity")
                    || asString.startsWith("NaN")) {
                return TursoConstant.createIntConstant(0);
            }
            for (int i = asString.length(); i >= 0; i--) {
                try {
                    String substring = asString.substring(0, i);
                    double d = Double.parseDouble(substring);
                    BigDecimal first = new BigDecimal(substring);
                    long longValue = first.longValue();
                    BigDecimal second = BigDecimal.valueOf(longValue);
                    boolean isWithinConvertibleRange = longValue >= MIN_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED
                            && longValue <= MAX_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED && convertRealToInt;
                    boolean isFloatingPointNumber = substring.contains(".") || substring.toUpperCase().contains("E");
                    boolean doubleShouldBeConvertedToInt = isFloatingPointNumber && first.compareTo(second) == 0
                            && isWithinConvertibleRange;
                    boolean isInteger = !isFloatingPointNumber && first.compareTo(second) == 0;
                    if (doubleShouldBeConvertedToInt || isInteger && !convertIntToReal) {
                        // see https://www.sqlite.org/src/tktview/afdc5a29dc
                        return TursoConstant.createIntConstant(first.longValue());
                    } else {
                        return TursoConstant.createRealConstant(d);
                    }
                } catch (Exception e) {
                }
            }
            if (noNumIsRealZero) {
                return TursoConstant.createRealConstant(0.0);
            } else {
                return TursoConstant.createIntConstant(0);
            }
        default:
            throw new AssertionError(value);
        }
    }

    private static boolean startsWithWhitespace(String asString) {
        if (asString.isEmpty()) {
            return false;
        }
        char c = asString.charAt(0);
        switch (c) {
        case ' ':
        case '\t':
        case 0x0b:
        case '\f':
        case '\n':
        case '\r':
            return true;
        default:
            return false;
        }
    }

    private static boolean unprintAbleCharThatLetsBecomeNumberZero(String s) {
        // non-printable characters are ignored by Double.valueOf
        for (int i = 0; i < s.length(); i++) {
            char charAt = s.charAt(i);
            if (!Character.isISOControl(charAt) && !Character.isWhitespace(charAt)) {
                return false;
            }
            switch (charAt) {
            case GROUP_SEPARATOR:
            case FILE_SEPARATOR:
            case RECORD_SEPARATOR:
            case UNIT_SEPARATOR:
            case SYNCHRONOUS_IDLE:
                return true;
            default:
                // fall through
            }

            if (Character.isWhitespace(charAt)) {
                continue;
            } else {
                return true;
            }
        }
        return false;
    }

    public static TursoConstant castToText(TursoConstant cons) {
        if (cons.getDataType() == TursoDataType.TEXT) {
            return cons;
        }
        if (cons.getDataType() == TursoDataType.NULL) {
            return cons;
        }
        if (cons.getDataType() == TursoDataType.REAL) {
            if (cons.asDouble() == Double.POSITIVE_INFINITY) {
                return TursoConstant.createTextConstant("Inf");
            } else if (cons.asDouble() == Double.NEGATIVE_INFINITY) {
                return TursoConstant.createTextConstant("-Inf");
            } else {
                return null;
            }
        }
        if (cons.getDataType() == TursoDataType.INT) {
            return TursoConstant.createTextConstant(String.valueOf(cons.asInt()));
        }
        return null;
    }

    public static TursoConstant asBoolean(TursoConstant val) {
        Optional<Boolean> boolVal = isTrue(val);
        if (boolVal.isPresent()) {
            return TursoConstant.createBoolean(boolVal.get());
        } else {
            return TursoConstant.createNullConstant();
        }
    }

    public static TursoConstant castToBlob(TursoConstant cons) {
        if (cons.isNull()) {
            return cons;
        } else {
            TursoConstant stringVal = TursoCast.castToText(cons);
            if (stringVal == null) {
                return null;
            } else {
                return TursoConstant.createBinaryConstant(stringVal.asString().getBytes(DEFAULT_ENCODING));
            }
        }
    }

}
