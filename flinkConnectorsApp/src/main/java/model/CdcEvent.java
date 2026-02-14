package model;

import java.io.Serializable;

/**
 * Generic CDC event that works with any table.
 *
 * Unlike ShipmentCdcEvent which hardcodes the Shipment POJO,
 * this stores before/after as JSON strings so it can represent
 * rows from any table. Use the `table` field to identify which
 * table the event came from, then parse the JSON downstream.
 *
 * Operation types (op field):
 * - "c" = create (INSERT)
 * - "u" = update (UPDATE)
 * - "d" = delete (DELETE)
 * - "r" = read (initial snapshot)
 */
public class CdcEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String op;
    private String before;  // JSON string of the row before the change
    private String after;   // JSON string of the row after the change
    private long tsMs;
    private String database;
    private String table;

    public CdcEvent() {}

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getBefore() {
        return before;
    }

    public void setBefore(String before) {
        this.before = before;
    }

    public String getAfter() {
        return after;
    }

    public void setAfter(String after) {
        this.after = after;
    }

    public long getTsMs() {
        return tsMs;
    }

    public void setTsMs(long tsMs) {
        this.tsMs = tsMs;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isInsert() {
        return "c".equals(op) || "r".equals(op);
    }

    public boolean isUpdate() {
        return "u".equals(op);
    }

    public boolean isDelete() {
        return "d".equals(op);
    }

    /**
     * Returns the current state JSON (after for insert/update, before for delete).
     */
    public String getCurrentValue() {
        return after != null ? after : before;
    }

    @Override
    public String toString() {
        return "CdcEvent{" +
                "op='" + op + '\'' +
                ", table='" + table + '\'' +
                ", database='" + database + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", tsMs=" + tsMs +
                '}';
    }
}
