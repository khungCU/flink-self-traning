package model;

import java.io.Serializable;
import java.util.Objects;

public class Shipment implements Serializable {
    private static final long serialVersionUID = 1L;

    private Integer shipmentId;
    private Integer orderId;
    private String origin;
    private String destination;
    private Boolean isArrived;

    public Shipment() {}

    public Shipment(Integer shipmentId, Integer orderId, String origin, String destination, Boolean isArrived) {
        this.shipmentId = shipmentId;
        this.orderId = orderId;
        this.origin = origin;
        this.destination = destination;
        this.isArrived = isArrived;
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
        Shipment shipment = (Shipment) o;
        return Objects.equals(shipmentId, shipment.shipmentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shipmentId);
    }

    @Override
    public String toString() {
        return "Shipment{" +
                "shipmentId=" + shipmentId +
                ", orderId=" + orderId +
                ", origin='" + origin + '\'' +
                ", destination='" + destination + '\'' +
                ", isArrived=" + isArrived +
                '}';
    }
}
