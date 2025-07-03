package sqlancer.turso.ast;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.turso.TursoVisitor;
import sqlancer.turso.schema.TursoDataType;
import sqlancer.turso.schema.TursoSchema.TursoColumn.TursoCollateSequence;

public abstract class TursoConstant extends TursoExpression {

    public static class TursoNullConstant extends TursoConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public TursoDataType getDataType() {
            return TursoDataType.NULL;
        }

        @Override
        public TursoConstant applyEquals(TursoConstant right, TursoCollateSequence collate) {
            return TursoConstant.createNullConstant();
        }

        @Override
        public TursoConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public TursoConstant applyTextAffinity() {
            return this;
        }

        @Override
        String getStringRepresentation() {
            return "NULL";
        }

        @Override
        public TursoConstant castToBoolean() {
            return TursoCast.asBoolean(this);
        }

        @Override
        public TursoConstant applyLess(TursoConstant right, TursoCollateSequence collate) {
            return TursoConstant.createNullConstant();
        }

    }

    public static class TursoIntConstant extends TursoConstant {

        private final long value;
        private final boolean isHex;

        public TursoIntConstant(long value, boolean isHex) {
            this.value = value;
            this.isHex = isHex;
        }

        public TursoIntConstant(long value) {
            this.value = value;
            this.isHex = false;
        }

        @Override
        public boolean isHex() {
            return isHex;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public long asInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public TursoDataType getDataType() {
            return TursoDataType.INT;
        }

        @Override
        public TursoConstant applyEquals(TursoConstant right, TursoCollateSequence collate) {
            if (right instanceof TursoRealConstant) {
                if (Double.isInfinite(right.asDouble())) {
                    return TursoConstant.createFalse();
                }
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asDouble());
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                return TursoConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) == 0);
            } else if (right instanceof TursoIntConstant) {
                return TursoConstant.createBoolean(value == right.asInt());
            } else if (right instanceof TursoNullConstant) {
                return TursoConstant.createNullConstant();
            } else {
                return TursoConstant.createFalse();
            }
        }

        @Override
        public TursoConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public TursoConstant applyTextAffinity() {
            return TursoConstant.createTextConstant(String.valueOf(value));
        }

        @Override
        String getStringRepresentation() {
            return String.valueOf(value);
        }

        @Override
        public TursoConstant castToBoolean() {
            return TursoCast.asBoolean(this);
        }

        @Override
        public TursoConstant applyLess(TursoConstant right, TursoCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == TursoDataType.TEXT || right.getDataType() == TursoDataType.BINARY) {
                return TursoConstant.createTrue();
            } else if (right.getDataType() == TursoDataType.INT) {
                long rightValue = right.asInt();
                return TursoConstant.createBoolean(value < rightValue);
            } else {
                if (Double.POSITIVE_INFINITY == right.asDouble()) {
                    return TursoConstant.createTrue();
                } else if (Double.NEGATIVE_INFINITY == right.asDouble()) {
                    return TursoConstant.createFalse();
                }
                assert right.getDataType() == TursoDataType.REAL;
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asDouble());
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                return TursoConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) < 0);
            }
        }

    }

    public static class TursoRealConstant extends TursoConstant {

        private final double value;

        public TursoRealConstant(double value) {
            this.value = value;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public double asDouble() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public TursoDataType getDataType() {
            return TursoDataType.REAL;
        }

        @Override
        public TursoConstant applyEquals(TursoConstant right, TursoCollateSequence collate) {
            if (right instanceof TursoRealConstant) {
                return TursoConstant.createBoolean(value == right.asDouble());
            } else if (right instanceof TursoIntConstant) {
                if (Double.isInfinite(value)) {
                    return TursoConstant.createFalse();
                }
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asInt());
                return TursoConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) == 0);
            } else if (right instanceof TursoNullConstant) {
                return TursoConstant.createNullConstant();
            } else {
                return TursoConstant.createFalse();
            }
        }

        @Override
        public TursoConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public TursoConstant applyTextAffinity() {
            return TursoCast.castToText(this);
        }

        @Override
        String getStringRepresentation() {
            return String.valueOf(value);
        }

        @Override
        public TursoConstant castToBoolean() {
            return TursoCast.asBoolean(this);
        }

        @Override
        public TursoConstant applyLess(TursoConstant right, TursoCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == TursoDataType.TEXT || right.getDataType() == TursoDataType.BINARY) {
                return TursoConstant.createTrue();
            } else if (right.getDataType() == TursoDataType.REAL) {
                double rightValue = right.asDouble();
                return TursoConstant.createBoolean(value < rightValue);
            } else {
                if (Double.POSITIVE_INFINITY == value) {
                    return TursoConstant.createFalse();
                } else if (Double.NEGATIVE_INFINITY == value) {
                    return TursoConstant.createTrue();
                }
                assert right.getDataType() == TursoDataType.INT;
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asInt());
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                return TursoConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) < 0);
            }
        }

    }

    public static class TursoTextConstant extends TursoConstant {

        private final String text;

        public TursoTextConstant(String text) {
            this.text = text;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public String asString() {
            return text;
        }

        @Override
        public Object getValue() {
            return text;
        }

        @Override
        public TursoDataType getDataType() {
            return TursoDataType.TEXT;
        }

        @Override
        public TursoConstant applyEquals(TursoConstant right, TursoCollateSequence collate) {
            if (right.isNull()) {
                return TursoConstant.createNullConstant();
            } else if (right instanceof TursoTextConstant) {
                String other = right.asString();
                boolean equals;
                switch (collate) {
                case BINARY:
                    equals = text.equals(other);
                    break;
                case NOCASE:
                    equals = toLower(text).equals(toLower(other));
                    break;
                case RTRIM:
                    equals = trimTrailing(text).equals(trimTrailing(other));
                    break;
                default:
                    throw new AssertionError(collate);
                }
                return TursoConstant.createBoolean(equals);
            } else {
                return TursoConstant.createFalse();
            }
        }

        public static String toLower(String t) {
            StringBuilder text = new StringBuilder(t);
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c >= 'A' && c <= 'Z') {
                    text.setCharAt(i, Character.toLowerCase(c));
                }
            }
            return text.toString();
        }

        public static String toUpper(String t) {
            StringBuilder text = new StringBuilder(t);
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c >= 'a' && c <= 'z') {
                    text.setCharAt(i, Character.toUpperCase(c));
                }
            }
            return text.toString();
        }

        public static String trim(String str) {
            return trimLeading(trimTrailing(str));
        }

        public static String trimLeading(String str) {
            if (str != null) {
                for (int i = 0; i < str.length(); i++) {
                    if (str.charAt(i) != ' ') {
                        return str.substring(i);
                    }
                }
            }
            return "";
        }

        public static String trimTrailing(String str) {
            if (str != null) {
                for (int i = str.length() - 1; i >= 0; --i) {
                    if (str.charAt(i) != ' ') {
                        return str.substring(0, i + 1);
                    }
                }
            }
            return "";
        }

        @Override
        public TursoConstant applyNumericAffinity() {
            Pattern leadingDigitPattern = Pattern
                    .compile("[-+]?((\\d(\\d)*(\\.(\\d)*)?)|\\.(\\d)(\\d)*)([Ee][+-]?(\\d)(\\d)*)?");
            String trimmedString = text.trim();
            if (trimmedString.isEmpty()) {
                return this;
            }
            Matcher matcher = leadingDigitPattern.matcher(trimmedString);
            if (matcher.matches()) {
                return TursoCast.castToNumeric(this);
            } else {
                return this;
            }
        }

        @Override
        public TursoConstant applyTextAffinity() {
            return this;
        }

        @Override
        String getStringRepresentation() {
            return text;
        }

        @Override
        public TursoConstant castToBoolean() {
            return TursoCast.asBoolean(this);
        }

        @Override
        public TursoConstant applyLess(TursoConstant right, TursoCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == TursoDataType.BINARY) {
                return TursoConstant.createTrue();
            } else if (right.getDataType() == TursoDataType.TEXT) {
                String other = right.asString();
                boolean lessThan;
                switch (collate) {
                case BINARY:
                    lessThan = text.compareTo(other) < 0;
                    break;
                case NOCASE:
                    lessThan = toLower(text).compareTo(toLower(other)) < 0;
                    break;
                case RTRIM:
                    lessThan = trimTrailing(text).compareTo(trimTrailing(other)) < 0;
                    break;
                default:
                    throw new AssertionError(collate);
                }
                return TursoConstant.createBoolean(lessThan);
            } else {
                assert right.getDataType() == TursoDataType.REAL || right.getDataType() == TursoDataType.INT;
                return TursoConstant.createFalse();
            }
        }
    }

    public static class TursoBinaryConstant extends TursoConstant {

        private final byte[] bytes;

        public TursoBinaryConstant(byte[] bytes) {
            this.bytes = bytes.clone();
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public TursoDataType getDataType() {
            return TursoDataType.BINARY;
        }

        @Override
        public Object getValue() {
            return bytes;
        }

        @Override
        public byte[] asBinary() {
            return bytes.clone();
        }

        @Override
        public TursoConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public TursoConstant applyTextAffinity() {
            return this;
            /*
             * if (bytes.length == 0) { return this; } else { StringBuilder sb = new StringBuilder(); for (byte b :
             * bytes) { if (isPrintableChar(b)) { sb.append((char) b); } } return
             * TursoConstant.createTextConstant(sb.toString()); }
             */
        }

        public boolean isPrintableChar(byte b) {
            return Math.abs(b) >= 32;
        }

        @Override
        String getStringRepresentation() {
            String hexRepr = TursoVisitor.byteArrayToHex(bytes);
            return String.format("x'%s'", hexRepr);
        }

        @Override
        public TursoConstant applyEquals(TursoConstant right, TursoCollateSequence collate) {
            if (right.isNull()) {
                return TursoConstant.createNullConstant();
            } else if (right.getDataType() == TursoDataType.BINARY) {
                byte[] otherArr = right.asBinary();
                if (bytes.length == otherArr.length) {
                    for (int i = 0; i < bytes.length; i++) {
                        if (bytes[i] != otherArr[i]) {
                            return TursoConstant.createFalse();
                        }
                    }
                    return TursoConstant.createTrue();
                } else {
                    return TursoConstant.createFalse();
                }
            } else {
                return TursoConstant.createFalse();
            }
        }

        @Override
        public TursoConstant castToBoolean() {
            return TursoCast.asBoolean(this);
        }

        @Override
        public TursoConstant applyLess(TursoConstant right, TursoCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == TursoDataType.TEXT || right.getDataType() == TursoDataType.INT
                    || right.getDataType() == TursoDataType.REAL) {
                return TursoConstant.createFalse();
            } else {
                byte[] otherArr = right.asBinary();
                int minLength = Math.min(bytes.length, otherArr.length);
                for (int i = 0; i < minLength; i++) {
                    if (bytes[i] != otherArr[i]) {
                        return TursoConstant.createBoolean((bytes[i] & 0xff) < (otherArr[i] & 0xff));
                    } else if (bytes[i] > otherArr[i]) {
                        return TursoConstant.createFalse();
                    }
                }
                return TursoConstant.createBoolean(bytes.length < otherArr.length);
            }
        }

    }

    abstract String getStringRepresentation();

    public abstract boolean isNull();

    public abstract Object getValue();

    public boolean isHex() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public double asDouble() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public byte[] asBinary() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.getDataType().toString());
    }

    public abstract TursoDataType getDataType();

    public static TursoConstant createIntConstant(long val) {
        return new TursoIntConstant(val);
    }

    public static TursoConstant createIntConstant(long val, boolean isHex) {
        return new TursoIntConstant(val, isHex);
    }

    public static TursoConstant createBinaryConstant(byte[] val) {
        return new TursoBinaryConstant(val);
    }

    public static TursoConstant createBinaryConstant(String val) {
        return new TursoBinaryConstant(TursoVisitor.hexStringToByteArray(val));
    }

    public static TursoConstant createRealConstant(double real) {
        return new TursoRealConstant(real);
    }

    public static TursoConstant createTextConstant(String text) {
        return new TursoTextConstant(text);
    }

    public static TursoConstant createNullConstant() {
        return new TursoNullConstant();
    }

    public static TursoConstant getRandomBinaryConstant(Randomly r) {
        return new TursoBinaryConstant(r.getBytes());
    }

    @Override
    public TursoConstant getExpectedValue() {
        return this;
    }

    @Override
    public TursoCollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("(%s) %s", getDataType(), getStringRepresentation());
    }

    public abstract TursoConstant applyNumericAffinity();

    public abstract TursoConstant applyTextAffinity();

    public static TursoConstant createTrue() {
        return new TursoConstant.TursoIntConstant(1);
    }

    public static TursoConstant createFalse() {
        return new TursoConstant.TursoIntConstant(0);
    }

    public static TursoConstant createBoolean(boolean tr) {
        return new TursoConstant.TursoIntConstant(tr ? 1 : 0);
    }

    public abstract TursoConstant applyEquals(TursoConstant right, TursoCollateSequence collate);

    public abstract TursoConstant applyLess(TursoConstant right, TursoCollateSequence collate);

    public abstract TursoConstant castToBoolean();

    public TursoConstant applyEquals(TursoConstant right) {
        return applyEquals(right, TursoCollateSequence.BINARY);
    }

    public boolean isReal() {
        return getDataType() == TursoDataType.REAL;
    }

}
