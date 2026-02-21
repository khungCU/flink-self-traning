package model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class ShipmentV0 implements MessageNormalized {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    private String op;
    @JsonIgnore
    private String table;

    private Integer shipmentId;
    private Integer orderId;
    private String origin;
    private String destination;
    private Boolean isArrived;

    public ShipmentV0() {}

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

    public Integer getShipmentId() {
        return shipmentId;
    }

    public void setShipmentId(Integer shipmentId) {
        this.shipmentId = shipmentId;
    }

    public Integer getOrderId() {
        return orderId;
    }

    public void setOrderId(Integer orderId) {
        this.orderId = orderId;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public Boolean getIsArrived() {
        return isArrived;
    }

    public void setIsArrived(Boolean isArrived) {
        this.isArrived = isArrived;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShipmentV0 shipment = (ShipmentV0) o;
        return Objects.equals(shipmentId, shipment.shipmentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shipmentId);
    }

    @Override
    public String toString() {
        return "ShipmentV0{" +
                "shipmentId=" + shipmentId +
                ", orderId=" + orderId +
                ", origin='" + origin + '\'' +
                ", destination='" + destination + '\'' +
                ", isArrived=" + isArrived +
                '}';
    }
}
