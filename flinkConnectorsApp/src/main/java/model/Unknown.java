package model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Unknown implements MessageNormalized {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private String op;
    @JsonIgnore
    private String table;

    private String rawBefore;
    private String rawAfter;

    public Unknown() {}

    public Unknown(String op, String table, String rawBefore, String rawAfter) {
        this.op = op;
        this.table = table;
        this.rawBefore = rawBefore;
        this.rawAfter = rawAfter;
    }

    @Override
    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    @Override
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getRawBefore() {
        return rawBefore;
    }

    public void setRawBefore(String rawBefore) {
        this.rawBefore = rawBefore;
    }

    public String getRawAfter() {
        return rawAfter;
    }

    public void setRawAfter(String rawAfter) {
        this.rawAfter = rawAfter;
    }

    @Override
    public String toString() {
        return "Unknown{" +
                "table='" + table + '\'' +
                ", op='" + op + '\'' +
                ", Warning='Unknown table schema, unable to normalize'" +
                '}';
    }
}
