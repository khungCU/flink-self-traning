package model;

import java.io.Serializable;

public interface MessageNormalized extends Serializable {
    String getOp();
    void setOp(String op);
    String getTable();
    void setTable(String table);
}
