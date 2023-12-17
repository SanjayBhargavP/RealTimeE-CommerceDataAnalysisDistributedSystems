package org.apacheStreams;

public class CsvRecord {
    private String eventTime;
    private String eventType;
    private String productId;
    private String categoryId;
    private String categoryCode;
    private String brand;
    private double price;

    @Override
    public String toString() {
        return "CsvRecord{" +
                "eventTime='" + eventTime + '\'' +
                ", eventType='" + eventType + '\'' +
                ", productId='" + productId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", categoryCode='" + categoryCode + '\'' +
                ", brand='" + brand + '\'' +
                ", price=" + price +
                ", userId='" + userId + '\'' +
                ", userSession='" + userSession + '\'' +
                '}';
    }

    public CsvRecord() {
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
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

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserSession() {
        return userSession;
    }

    public void setUserSession(String userSession) {
        this.userSession = userSession;
    }

    private String userId;
    private String userSession;

}
