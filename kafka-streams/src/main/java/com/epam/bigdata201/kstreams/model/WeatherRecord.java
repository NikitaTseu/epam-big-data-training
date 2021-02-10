package com.epam.bigdata201.kstreams.model;

import com.epam.bigdata201.kstreams.utils.Timestampable;
import com.google.gson.annotations.SerializedName;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class WeatherRecord implements Timestampable {
    @SerializedName("lat")
    Double latitude;
    @SerializedName("lng")
    Double longitude;
    @SerializedName("avg_tmpr_f")
    Double avgTemperatureF;
    @SerializedName("avg_tmpr_c")
    Double avgTemperatureC;
    @SerializedName("wthr_date")
    String date;

    @Override
    public long getTimestamp() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            return sdf.parse(this.date).getTime();
        } catch (ParseException e) {
            return System.currentTimeMillis();
        }
    }

    public WeatherRecord() {
        this.latitude = 0.0;
        this.longitude = 0.0;
        this.avgTemperatureC = 0.0;
        this.avgTemperatureF = 0.0;
        this.date = null;
    }

    public WeatherRecord(Double latitude, Double longitude, Double avgTemperatureF,
                         Double avgTemperatureC, String date) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.avgTemperatureF = avgTemperatureF;
        this.avgTemperatureC = avgTemperatureC;
        this.date = date;
    }

    public WeatherRecord(Double avgTemperatureF, Double avgTemperatureC, String date) {
        this.latitude = 0.0;
        this.longitude = 0.0;
        this.avgTemperatureF = avgTemperatureF;
        this.avgTemperatureC = avgTemperatureC;
        this.date = date;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getAvgTemperatureF() {
        return avgTemperatureF;
    }

    public Double getAvgTemperatureC() {
        return avgTemperatureC;
    }

    public String getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "WeatherRecord{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                ", avgTemperatureF=" + avgTemperatureF +
                ", avgTemperatureC=" + avgTemperatureC +
                ", date='" + date + '\'' +
                '}';
    }
}
