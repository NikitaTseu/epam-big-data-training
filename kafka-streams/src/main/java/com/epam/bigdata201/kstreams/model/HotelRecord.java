package com.epam.bigdata201.kstreams.model;

import com.epam.bigdata201.kstreams.utils.Timestampable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HotelRecord implements Timestampable {
    String id;
    String name;
    String country;
    String city;
    String latitude;
    String longitude;
    String geohash;
    String date;

    public HotelRecord() {
    }

    public HotelRecord(String id, String name, String country, String city, String latitude,
                       String longitude, String geohash, String date) {
        this.id = id;
        this.name = name;
        this.country = country;
        this.city = city;
        this.latitude = latitude;
        this.longitude = longitude;
        this.geohash = geohash;
        this.date = date;
    }
    static final Logger logger = LoggerFactory.getLogger(HotelRecord.class);
    public HotelRecordEnriched addWeather(WeatherRecord weather) {
        if(weather != null){
            logger.debug("adding weather {} to hotel {}", this.toString(), weather.toString());
        }
        else{
            logger.debug("adding null to hotel {}", this.toString());
        }
        return new HotelRecordEnriched(this, weather);
    }

    @Override
    public long getTimestamp() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            return sdf.parse(this.date).getTime();
        } catch (ParseException e) {
            return System.currentTimeMillis();
        }
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }

    public String getLatitude() {
        return latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public String getGeohash() {
        return geohash;
    }

    public String getDate() {
        return date;
    }

    public String getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "HotelRecord{" +
            "id='" + id + '\'' +
            ", name='" + name + '\'' +
            ", geohash='" + geohash + '\'' +
            ", date='" + date + '\'' +
            '}';
    }
}
