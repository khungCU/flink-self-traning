package model;

import java.io.Serializable;

/**
 * Wrapper class for Debezium CDC events for the Shipment table.
 *
 * Operation types (op field):
 * - "c" = create (INSERT)
 * - "u" = update (UPDATE)
 * - "d" = delete (DELETE)
 * - "r" = read (initial snapshot)
 */
public class ShipmentCdcEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String op;
    private Shipment before;
    private Shipment after;
    private long tsMs;
    private String database;
    private String table;

    public ShipmentCdcEvent() {}

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Shipment getBefore() {
        return before;
    }

    public void setBefore(Shipment before) {
        this.before = before;
    }

    public Shipment getAfter() {
        return after;
    }

    public void setAfter(Shipment after) {
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

    /**
     * Returns the current state of the record (after for insert/update, before for delete).
     */
    public Shipment getCurrentValue() {
        return after != null ? after : before;
    }

    /**
     * Check if this is an insert operation.
     */
    public boolean isInsert() {
        return "c".equals(op) || "r".equals(op);
    }

    /**
     * Check if this is an update operation.
     */
    public boolean isUpdate() {
        return "u".equals(op);
    }

    /**
     * Check if this is a delete operation.
     */
    public boolean isDelete() {
        return "d".equals(op);
    }

    @Override
    public String toString() {
        return "ShipmentCdcEvent{" +
                "op='" + op + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", tsMs=" + tsMs +
                ", database='" + database + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
