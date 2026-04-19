package io.jpointdb.core.sql;

import io.jpointdb.core.schema.ColumnType;

public enum ValueType {
    I32, I64, F64, STRING, BOOL;

    public static ValueType fromColumn(ColumnType t) {
        return switch (t) {
            case I32 -> I32;
            case I64 -> I64;
            case F64 -> F64;
            case STRING -> STRING;
        };
    }

    public boolean isNumeric() {
        return this == I32 || this == I64 || this == F64;
    }

    /** Type promotion for binary numeric ops: widest wins. */
    public static ValueType widerNumeric(ValueType a, ValueType b) {
        if (a == F64 || b == F64)
            return F64;
        if (a == I64 || b == I64)
            return I64;
        return I32;
    }
}
