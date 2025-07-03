package sqlancer.turso.schema;

import sqlancer.IgnoreMeException;

public enum TursoDataType {
    NULL, INT, TEXT, REAL, NONE, BINARY;

    public static TursoDataType getTypeFromName(String name) {
        if (name.equals("integer")) {
            return INT;
        } else if (name.equals("real")) {
            return REAL;
        } else if (name.equals("text")) {
            return TEXT;
        } else if (name.equals("blob")) {
            return NONE;
        } else if (name.equals("null")) {
            return NULL;
        } else {
            throw new IgnoreMeException();
        }
    }
}
