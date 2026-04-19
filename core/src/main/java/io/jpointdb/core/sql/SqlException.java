package io.jpointdb.core.sql;

public final class SqlException extends RuntimeException {

    private final int position;

    public SqlException(String message, int position) {
        super(message + " at position " + position);
        this.position = position;
    }

    public int position() {
        return position;
    }
}
