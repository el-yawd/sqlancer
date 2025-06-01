package sqlancer.limbo.ast;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.limbo.LimboVisitor;
import sqlancer.limbo.schema.LimboDataType;
import sqlancer.limbo.schema.LimboSchema.LimboColumn.LimboCollateSequence;

public abstract class LimboConstant extends LimboExpression {

    public static class LimboNullConstant extends LimboConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public LimboDataType getDataType() {
            return LimboDataType.NULL;
        }

        @Override
        public LimboConstant applyEquals(LimboConstant right, LimboCollateSequence collate) {
            return LimboConstant.createNullConstant();
        }

        @Override
        public LimboConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public LimboConstant applyTextAffinity() {
            return this;
        }

        @Override
        String getStringRepresentation() {
            return "NULL";
        }

        @Override
        public LimboConstant castToBoolean() {
            return LimboCast.asBoolean(this);
        }

        @Override
        public LimboConstant applyLess(LimboConstant right, LimboCollateSequence collate) {
            return LimboConstant.createNullConstant();
        }

    }

    public static class LimboIntConstant extends LimboConstant {

        private final long value;
        private final boolean isHex;

        public LimboIntConstant(long value, boolean isHex) {
            this.value = value;
            this.isHex = isHex;
        }

        public LimboIntConstant(long value) {
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
        public LimboDataType getDataType() {
            return LimboDataType.INT;
        }

        @Override
        public LimboConstant applyEquals(LimboConstant right, LimboCollateSequence collate) {
            if (right instanceof LimboRealConstant) {
                if (Double.isInfinite(right.asDouble())) {
                    return LimboConstant.createFalse();
                }
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asDouble());
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                return LimboConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) == 0);
            } else if (right instanceof LimboIntConstant) {
                return LimboConstant.createBoolean(value == right.asInt());
            } else if (right instanceof LimboNullConstant) {
                return LimboConstant.createNullConstant();
            } else {
                return LimboConstant.createFalse();
            }
        }

        @Override
        public LimboConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public LimboConstant applyTextAffinity() {
            return LimboConstant.createTextConstant(String.valueOf(value));
        }

        @Override
        String getStringRepresentation() {
            return String.valueOf(value);
        }

        @Override
        public LimboConstant castToBoolean() {
            return LimboCast.asBoolean(this);
        }

        @Override
        public LimboConstant applyLess(LimboConstant right, LimboCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == LimboDataType.TEXT || right.getDataType() == LimboDataType.BINARY) {
                return LimboConstant.createTrue();
            } else if (right.getDataType() == LimboDataType.INT) {
                long rightValue = right.asInt();
                return LimboConstant.createBoolean(value < rightValue);
            } else {
                if (Double.POSITIVE_INFINITY == right.asDouble()) {
                    return LimboConstant.createTrue();
                } else if (Double.NEGATIVE_INFINITY == right.asDouble()) {
                    return LimboConstant.createFalse();
                }
                assert right.getDataType() == LimboDataType.REAL;
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asDouble());
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                return LimboConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) < 0);
            }
        }

    }

    public static class LimboRealConstant extends LimboConstant {

        private final double value;

        public LimboRealConstant(double value) {
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
        public LimboDataType getDataType() {
            return LimboDataType.REAL;
        }

        @Override
        public LimboConstant applyEquals(LimboConstant right, LimboCollateSequence collate) {
            if (right instanceof LimboRealConstant) {
                return LimboConstant.createBoolean(value == right.asDouble());
            } else if (right instanceof LimboIntConstant) {
                if (Double.isInfinite(value)) {
                    return LimboConstant.createFalse();
                }
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asInt());
                return LimboConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) == 0);
            } else if (right instanceof LimboNullConstant) {
                return LimboConstant.createNullConstant();
            } else {
                return LimboConstant.createFalse();
            }
        }

        @Override
        public LimboConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public LimboConstant applyTextAffinity() {
            return LimboCast.castToText(this);
        }

        @Override
        String getStringRepresentation() {
            return String.valueOf(value);
        }

        @Override
        public LimboConstant castToBoolean() {
            return LimboCast.asBoolean(this);
        }

        @Override
        public LimboConstant applyLess(LimboConstant right, LimboCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == LimboDataType.TEXT || right.getDataType() == LimboDataType.BINARY) {
                return LimboConstant.createTrue();
            } else if (right.getDataType() == LimboDataType.REAL) {
                double rightValue = right.asDouble();
                return LimboConstant.createBoolean(value < rightValue);
            } else {
                if (Double.POSITIVE_INFINITY == value) {
                    return LimboConstant.createFalse();
                } else if (Double.NEGATIVE_INFINITY == value) {
                    return LimboConstant.createTrue();
                }
                assert right.getDataType() == LimboDataType.INT;
                BigDecimal otherColumnValue = BigDecimal.valueOf(right.asInt());
                BigDecimal thisColumnValue = BigDecimal.valueOf(value);
                return LimboConstant.createBoolean(thisColumnValue.compareTo(otherColumnValue) < 0);
            }
        }

    }

    public static class LimboTextConstant extends LimboConstant {

        private final String text;

        public LimboTextConstant(String text) {
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
        public LimboDataType getDataType() {
            return LimboDataType.TEXT;
        }

        @Override
        public LimboConstant applyEquals(LimboConstant right, LimboCollateSequence collate) {
            if (right.isNull()) {
                return LimboConstant.createNullConstant();
            } else if (right instanceof LimboTextConstant) {
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
                return LimboConstant.createBoolean(equals);
            } else {
                return LimboConstant.createFalse();
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
        public LimboConstant applyNumericAffinity() {
            Pattern leadingDigitPattern = Pattern
                    .compile("[-+]?((\\d(\\d)*(\\.(\\d)*)?)|\\.(\\d)(\\d)*)([Ee][+-]?(\\d)(\\d)*)?");
            String trimmedString = text.trim();
            if (trimmedString.isEmpty()) {
                return this;
            }
            Matcher matcher = leadingDigitPattern.matcher(trimmedString);
            if (matcher.matches()) {
                return LimboCast.castToNumeric(this);
            } else {
                return this;
            }
        }

        @Override
        public LimboConstant applyTextAffinity() {
            return this;
        }

        @Override
        String getStringRepresentation() {
            return text;
        }

        @Override
        public LimboConstant castToBoolean() {
            return LimboCast.asBoolean(this);
        }

        @Override
        public LimboConstant applyLess(LimboConstant right, LimboCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == LimboDataType.BINARY) {
                return LimboConstant.createTrue();
            } else if (right.getDataType() == LimboDataType.TEXT) {
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
                return LimboConstant.createBoolean(lessThan);
            } else {
                assert right.getDataType() == LimboDataType.REAL || right.getDataType() == LimboDataType.INT;
                return LimboConstant.createFalse();
            }
        }
    }

    public static class LimboBinaryConstant extends LimboConstant {

        private final byte[] bytes;

        public LimboBinaryConstant(byte[] bytes) {
            this.bytes = bytes.clone();
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public LimboDataType getDataType() {
            return LimboDataType.BINARY;
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
        public LimboConstant applyNumericAffinity() {
            return this;
        }

        @Override
        public LimboConstant applyTextAffinity() {
            return this;
            /*
             * if (bytes.length == 0) { return this; } else { StringBuilder sb = new StringBuilder(); for (byte b :
             * bytes) { if (isPrintableChar(b)) { sb.append((char) b); } } return
             * LimboConstant.createTextConstant(sb.toString()); }
             */
        }

        public boolean isPrintableChar(byte b) {
            return Math.abs(b) >= 32;
        }

        @Override
        String getStringRepresentation() {
            String hexRepr = LimboVisitor.byteArrayToHex(bytes);
            return String.format("x'%s'", hexRepr);
        }

        @Override
        public LimboConstant applyEquals(LimboConstant right, LimboCollateSequence collate) {
            if (right.isNull()) {
                return LimboConstant.createNullConstant();
            } else if (right.getDataType() == LimboDataType.BINARY) {
                byte[] otherArr = right.asBinary();
                if (bytes.length == otherArr.length) {
                    for (int i = 0; i < bytes.length; i++) {
                        if (bytes[i] != otherArr[i]) {
                            return LimboConstant.createFalse();
                        }
                    }
                    return LimboConstant.createTrue();
                } else {
                    return LimboConstant.createFalse();
                }
            } else {
                return LimboConstant.createFalse();
            }
        }

        @Override
        public LimboConstant castToBoolean() {
            return LimboCast.asBoolean(this);
        }

        @Override
        public LimboConstant applyLess(LimboConstant right, LimboCollateSequence collate) {
            if (right.isNull()) {
                return right;
            } else if (right.getDataType() == LimboDataType.TEXT || right.getDataType() == LimboDataType.INT
                    || right.getDataType() == LimboDataType.REAL) {
                return LimboConstant.createFalse();
            } else {
                byte[] otherArr = right.asBinary();
                int minLength = Math.min(bytes.length, otherArr.length);
                for (int i = 0; i < minLength; i++) {
                    if (bytes[i] != otherArr[i]) {
                        return LimboConstant.createBoolean((bytes[i] & 0xff) < (otherArr[i] & 0xff));
                    } else if (bytes[i] > otherArr[i]) {
                        return LimboConstant.createFalse();
                    }
                }
                return LimboConstant.createBoolean(bytes.length < otherArr.length);
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

    public abstract LimboDataType getDataType();

    public static LimboConstant createIntConstant(long val) {
        return new LimboIntConstant(val);
    }

    public static LimboConstant createIntConstant(long val, boolean isHex) {
        return new LimboIntConstant(val, isHex);
    }

    public static LimboConstant createBinaryConstant(byte[] val) {
        return new LimboBinaryConstant(val);
    }

    public static LimboConstant createBinaryConstant(String val) {
        return new LimboBinaryConstant(LimboVisitor.hexStringToByteArray(val));
    }

    public static LimboConstant createRealConstant(double real) {
        return new LimboRealConstant(real);
    }

    public static LimboConstant createTextConstant(String text) {
        return new LimboTextConstant(text);
    }

    public static LimboConstant createNullConstant() {
        return new LimboNullConstant();
    }

    public static LimboConstant getRandomBinaryConstant(Randomly r) {
        return new LimboBinaryConstant(r.getBytes());
    }

    @Override
    public LimboConstant getExpectedValue() {
        return this;
    }

    @Override
    public LimboCollateSequence getExplicitCollateSequence() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("(%s) %s", getDataType(), getStringRepresentation());
    }

    public abstract LimboConstant applyNumericAffinity();

    public abstract LimboConstant applyTextAffinity();

    public static LimboConstant createTrue() {
        return new LimboConstant.LimboIntConstant(1);
    }

    public static LimboConstant createFalse() {
        return new LimboConstant.LimboIntConstant(0);
    }

    public static LimboConstant createBoolean(boolean tr) {
        return new LimboConstant.LimboIntConstant(tr ? 1 : 0);
    }

    public abstract LimboConstant applyEquals(LimboConstant right, LimboCollateSequence collate);

    public abstract LimboConstant applyLess(LimboConstant right, LimboCollateSequence collate);

    public abstract LimboConstant castToBoolean();

    public LimboConstant applyEquals(LimboConstant right) {
        return applyEquals(right, LimboCollateSequence.BINARY);
    }

    public boolean isReal() {
        return getDataType() == LimboDataType.REAL;
    }

}
