package flink.self.traning.asyncLookup;

public class EnrichLocation {
    private double latitude;
    private double longitude;
    private String city;
    private String weather;
    private double tempature;
    private double bodyTempature;

    public EnrichLocation(){}

    public EnrichLocation(
        double latitude,
        double longitude,
        String city,
        String weather,
        double tempature,
        double bodyTempature
    ) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.city = city;
        this.weather = weather;
        this.tempature = tempature;
        this.bodyTempature = bodyTempature;
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

    public String weather(){
        return weather;
    }

    public double tempature(){
        return tempature;
    }

    public double bodyTempature(){
        return bodyTempature;
    }

    @Override
    public String toString(){
        return "City: " + this.getCity() + "\n" +
               "Tempature: " + this.tempature() + "\n" +
               "Body Tempature: " + this.bodyTempature() + "\n" +
               "Weather: " + this.weather() + "\n";
    }
    
}
