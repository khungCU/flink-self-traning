package flink.self.traning.asyncLookup;

public class Location {
    
    private double latitude;
    private double longitude;
    private String city;

    public Location(){}

    public Location(
        double latitude,
        double longitude,
        String city
    ) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.city = city;
    }

    public double getLatitude(){
        return latitude;
    }
    
    public double getLongitude(){
        return longitude;
    }

    public String getCity(){
        return city;
    }
}
