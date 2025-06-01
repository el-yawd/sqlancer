package sqlancer.limbo.ast;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Optional;
import java.util.regex.Pattern;

import sqlancer.IgnoreMeException;
import sqlancer.limbo.schema.LimboDataType;

public final class LimboCast {

    private static final double MAX_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED = Math.pow(2, 51 - 1) - 1;
    private static final double MIN_INT_FOR_WHICH_CONVERSION_TO_INT_IS_TRIED = -Math.pow(2, 51 - 1);
    public static final Charset DEFAULT_ENCODING = Charset.forName("UTF-8");

    private static final byte FILE_SEPARATOR = 0x1c;
    private static final byte GROUP_SEPARATOR = 0x1d;
    private static final byte RECORD_SEPARATOR = 0x1e;
    private static final byte UNIT_SEPARATOR = 0x1f;
    private static final byte SYNCHRONOUS_IDLE = 0x16;

    static Connection castDatabase;

    private LimboCast() {
    }

    public static Optional<Boolean> isTrue(LimboConstant value) {
        LimboConstant numericValue;
        if (value.getDataType() == LimboDataType.NULL) {
            return Optional.empty();
        }
        if (value.getDataType() == LimboDataType.TEXT || value.getDataType() == LimboDataType.BINARY) {
            numericValue = castToNumeric(value);
        } else {
            numericValue = value;
        }
        assert numericValue.getDataType() != LimboDataType.TEXT : numericValue + "should have been converted";
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
    public static LimboConstant castToInt(LimboConstant originalCons) {
        LimboConstant cons = originalCons;
        if (cons.getDataType() == LimboDataType.BINARY) {
            String text = new String(cons.asBinary(), DEFAULT_ENCODING);
            cons = LimboConstant.createTextConstant(text);
        }
        switch (cons.getDataType()) {
        case NULL:
            return LimboConstant.createNullConstant();
        case INT:
            return cons;
        case REAL:
            checkDoubleIsInsideDangerousRange(cons.asDouble());
            return LimboConstant.createIntConstant((long) cons.asDouble());
        case TEXT:
            String asString = cons.asString();
            while (startsWithWhitespace(asString)) {
                asString = asString.substring(1);
            }
            if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
                return LimboConstant.createIntConstant(0);
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
                        return LimboConstant.createIntConstant(result);
                    }
                } catch (Exception e) {

                }
            }
            return LimboConstant.createIntConstant(0);
        default:
            throw new AssertionError();
        }

    }

    public static LimboConstant castToReal(LimboConstant cons) {
        LimboConstant numericValue = castToNumeric(cons);
        if (numericValue.getDataType() == LimboDataType.INT) {
            double val = numericValue.asInt();
            checkDoubleIsInsideDangerousRange(val);
            return LimboConstant.createRealConstant(val);
        } else {
            return numericValue;
        }
    }

    public static LimboConstant castToNumericNoNumAsRealZero(LimboConstant value) {
        return convertInternal(value, false, true, true);
    }

    public static LimboConstant castToNumericFromNumOperand(LimboConstant value) {
        return convertInternal(value, false, false, false);
    }

    /*
     * Applies numeric affinity to a value.
     */
    public static LimboConstant castToNumeric(LimboConstant value) {
        return convertInternal(value, true, false, false);
    }

    private static LimboConstant convertInternal(LimboConstant originalValue, boolean convertRealToInt,
            boolean noNumIsRealZero, boolean convertIntToReal) throws AssertionError {
        LimboConstant value = originalValue;
        if (value.getDataType() == LimboDataType.BINARY) {
            String text = new String(value.asBinary(), DEFAULT_ENCODING);
            value = LimboConstant.createTextConstant(text);
        }
        switch (value.getDataType()) {
        case NULL:
            return LimboConstant.createNullConstant();
        case INT:
        case REAL:
            return value;
        case TEXT:
            String asString = value.asString();
            while (startsWithWhitespace(asString)) {
                asString = asString.substring(1);
            }
            if (!asString.isEmpty() && unprintAbleCharThatLetsBecomeNumberZero(asString)) {
                return LimboConstant.createIntConstant(0);
            }
            if (asString.toLowerCase().startsWith("-infinity") || asString.toLowerCase().startsWith("infinity")
                    || asString.startsWith("NaN")) {
                return LimboConstant.createIntConstant(0);
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
                        return LimboConstant.createIntConstant(first.longValue());
                    } else {
                        return LimboConstant.createRealConstant(d);
                    }
                } catch (Exception e) {
                }
            }
            if (noNumIsRealZero) {
                return LimboConstant.createRealConstant(0.0);
            } else {
                return LimboConstant.createIntConstant(0);
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

    public static LimboConstant castToText(LimboConstant cons) {
        if (cons.getDataType() == LimboDataType.TEXT) {
            return cons;
        }
        if (cons.getDataType() == LimboDataType.NULL) {
            return cons;
        }
        if (cons.getDataType() == LimboDataType.REAL) {
            if (cons.asDouble() == Double.POSITIVE_INFINITY) {
                return LimboConstant.createTextConstant("Inf");
            } else if (cons.asDouble() == Double.NEGATIVE_INFINITY) {
                return LimboConstant.createTextConstant("-Inf");
            } else {
                return null;
            }
        }
        if (cons.getDataType() == LimboDataType.INT) {
            return LimboConstant.createTextConstant(String.valueOf(cons.asInt()));
        }
        return null;
    }

    public static LimboConstant asBoolean(LimboConstant val) {
        Optional<Boolean> boolVal = isTrue(val);
        if (boolVal.isPresent()) {
            return LimboConstant.createBoolean(boolVal.get());
        } else {
            return LimboConstant.createNullConstant();
        }
    }

    public static LimboConstant castToBlob(LimboConstant cons) {
        if (cons.isNull()) {
            return cons;
        } else {
            LimboConstant stringVal = LimboCast.castToText(cons);
            if (stringVal == null) {
                return null;
            } else {
                return LimboConstant.createBinaryConstant(stringVal.asString().getBytes(DEFAULT_ENCODING));
            }
        }
    }

}
