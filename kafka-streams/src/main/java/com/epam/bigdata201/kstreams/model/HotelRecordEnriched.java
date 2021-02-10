package com.epam.bigdata201.kstreams.model;

public class HotelRecordEnriched extends HotelRecord {
    Double avgTemperatureF;
    Double avgTemperatureC;

    public HotelRecordEnriched() {
    }

    public HotelRecordEnriched(HotelRecord hotel, WeatherRecord weather){
        super(hotel.id, hotel.name, hotel.country, hotel.city, hotel.latitude, hotel.longitude, hotel.geohash, hotel.date);
        if(weather == null) {
            this.avgTemperatureF = null;
            this.avgTemperatureC = null;
        }
        else {
            this.avgTemperatureC = weather.getAvgTemperatureC();
            this.avgTemperatureF = weather.getAvgTemperatureF();
        }
    }

    public HotelRecordEnriched(String id, String name, String country, String city, String latitude,
                               String longitude, String geohash, String date,
                               Double avgTemperatureF, Double avgTemperatureC) {
        super(id, name, country, city, latitude, longitude, geohash, date);
        this.avgTemperatureF = avgTemperatureF;
        this.avgTemperatureC = avgTemperatureC;
    }

    public Double getAvgTemperatureF() {
        return avgTemperatureF;
    }

    public Double getAvgTemperatureC() {
        return avgTemperatureC;
    }

    public void setAvgTemperatureF(Double avgTemperatureF) {
        this.avgTemperatureF = avgTemperatureF;
    }

    public void setAvgTemperatureC(Double avgTemperatureC) {
        this.avgTemperatureC = avgTemperatureC;
    }

    @Override
    public String toString() {
        return "HotelRecordEnriched{" +
            "id='" + id + '\'' +
            "name='" + name + '\'' +
            ", geohash='" + geohash + '\'' +
            ", date='" + date + '\'' +
            ", avgTemperatureF=" + avgTemperatureF +
            ", avgTemperatureC=" + avgTemperatureC +
            '}';
    }
}
