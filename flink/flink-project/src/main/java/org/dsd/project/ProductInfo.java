package org.dsd.project;

public class ProductInfo {
    private String productId;
    private String categoryCode;
    private String brand;
    private String eventType; // "view" or "purchase"

    // Default constructor for Flink's internal serialization
    public ProductInfo() {}

    // Constructor with fields (excluding price)
    public ProductInfo(String productId, String categoryCode, String brand, String eventType) {
        this.productId = productId;
        this.categoryCode = categoryCode;
        this.brand = brand;
        this.eventType = eventType;
    }

    // Getters and setters for all fields
    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCategoryCode() {
        return categoryCode;
    }

    public void setCategoryCode(String categoryCode) {
        this.categoryCode = categoryCode;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "ProductInfo{" +
                "productId='" + productId + '\'' +
                ", categoryCode='" + categoryCode + '\'' +
                ", brand='" + brand + '\'' +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
