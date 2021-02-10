package com.epam.bigdata201.kstreams.utils;

import com.epam.bigdata201.kstreams.HotelsEnrichment;
import com.epam.bigdata201.kstreams.model.HotelRecordEnriched;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is no more used in the program
 * */
public class LeftJoinResultFilter{
    //HotelRecordEnriched hotel;
    static final Logger logger = LoggerFactory.getLogger(LeftJoinResultFilter.class);
    String id;
    String name;
    String country;
    String city;
    String latitude;
    String longitude;
    String geohash;
    String date;
    Double avgTemperatureF;
    Double avgTemperatureC;

    boolean isInitial = true;

    public LeftJoinResultFilter() {
        logger.debug("INITIAL FILTER CREATED");
    }

    public LeftJoinResultFilter process(HotelRecordEnriched val) {
        if(this.isInitial) {
            this.id = val.getId();
            this.name = val.getName();
            this.country = val.getCountry();
            this.city = val.getCity();
            this.latitude = val.getLatitude();
            this.longitude = val.getLongitude();
            this.geohash = val.getGeohash();
            this.date = val.getDate();
            this.avgTemperatureC = val.getAvgTemperatureC();
            this.avgTemperatureF = val.getAvgTemperatureF();
            this.isInitial = false;
            logger.debug("INITIAL FILTER REPLACED FOR: {}", val.toString());
            /*this.hotel = new HotelRecordEnriched(val.getName(), val.getCountry(), val.getCity(),
                val.getLatitude(), val.getLongitude(), val.getGeohash(), val.getDate(),
                val.getAvgTemperatureF(), val.getAvgTemperatureC());*/
        }
        if(this.avgTemperatureF == null) {
            this.avgTemperatureF = val.getAvgTemperatureF();
            logger.debug("TEMPERATURE REPLACED FOR: {}", val.toString());
        }
        if(this.avgTemperatureC == null) {
            this.avgTemperatureC = val.getAvgTemperatureC();
        }
        return this;
    }

    public HotelRecordEnriched createHotelRecord() {
        return new HotelRecordEnriched(id, name, country, city, latitude, longitude,
            geohash, date, avgTemperatureF, avgTemperatureC);
    }
}
