package com.epam.bigdata201.kstreams.utils;

import com.epam.bigdata201.kstreams.model.WeatherRecord;

public class CountAndSumTemperatureAggregator {
    long count;
    Double sumTempC;
    Double sumTempF;
    String date;

    public CountAndSumTemperatureAggregator() {
        this.count = 0L;
        this.sumTempC = 0.0;
        this.sumTempF = 0.0;
        this.date = null;
    }

    public CountAndSumTemperatureAggregator addValue(WeatherRecord val) {
        if(this.date == null) {
            this.date = val.getDate();
        }
        this.sumTempF += (val.getAvgTemperatureF() != null) ? val.getAvgTemperatureF() : 0.0;
        this.sumTempC += (val.getAvgTemperatureC() != null) ? val.getAvgTemperatureC() : 0.0;
        this.count += 1;
        return this;
    }

    public long getCount() {
        return count;
    }

    public Double getSumTempC() {
        return sumTempC;
    }

    public Double getSumTempF() {
        return sumTempF;
    }

    public String getDate() {
        return date;
    }

    @Override
    public String toString() {
        return "Aggregator {" +
                "count=" + count +
                ", sumTempC=" + sumTempC +
                ", sumTempF=" + sumTempF +
                ", date='" + date + '\'' +
                '}';
    }
}
