/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.epam.bigdata201.kstreams;

import ch.hsr.geohash.GeoHash;
import com.epam.bigdata201.kstreams.model.HotelRecord;
import com.epam.bigdata201.kstreams.model.HotelRecordEnriched;
import com.epam.bigdata201.kstreams.model.WeatherRecord;
import com.epam.bigdata201.kstreams.serde.JSONSerde;
import com.epam.bigdata201.kstreams.utils.CountAndSumTemperatureAggregator;
import com.epam.bigdata201.kstreams.utils.CustomTimestampExtractor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this application, we join enriched by date dimension hotels with weather by date and 4-characters geo-hash.
 *
 * In case of will be no mapping - weather data will be empty for this hotel.
 *
 * In case of multiple results for a hotel in a particular day -
 * they are grouped in a single entity, calculating average weather parameters.
 *
 * Final dataset should contain hotels data enriched with weather per day and output should go to separate Kafka topic.
 */
public class HotelsEnrichment{
    static final Logger logger = LoggerFactory.getLogger(HotelsEnrichment.class);

    static final String APPLICATION_ID = "hotels-weather-join";
    static final String HOTELS_TOPIC = "hotels";
    static final String WEATHER_TOPIC = "weather";
    static final String OUTPUT_TOPIC = "hotels-with-weather";
    static final String INTERMEDIATE_TOPIC = "tmp-out";

    public static String buildKey(String date, String geohash) {
        logger.debug("New key has been built: {}", date.concat(geohash));
        return date.concat(geohash);
    }

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 256*1000*1000);
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, String.valueOf(5L*365*24*60*60*1000));

        final StreamsBuilder builder = new StreamsBuilder();

        JSONSerde<WeatherRecord> weatherSerde = new JSONSerde<>(WeatherRecord.class);
        JSONSerde<HotelRecord> hotelSerde = new JSONSerde<>(HotelRecord.class);
        JSONSerde<HotelRecordEnriched> hotelEnrichedSerde = new JSONSerde<>(HotelRecordEnriched.class);
        JSONSerde<CountAndSumTemperatureAggregator> aggregatorSerde = new JSONSerde<>(CountAndSumTemperatureAggregator.class);

        // this stream consumes hotels records from Kafka topic
        // and sets "geohash+date" concatenation result as a key
        KStream<String, HotelRecord> inputHotels = builder
            .stream(HOTELS_TOPIC, Consumed.with(Serdes.String(), hotelSerde))
            .selectKey((k, v) -> buildKey(v.getDate(), v.getGeohash()));

        // this stream consumes weather data from Kafka topic
        // and sets "geohash+date" concatenation result as a key
        KStream<String, WeatherRecord> inputWeather = builder
            .stream(WEATHER_TOPIC, Consumed.with(Serdes.String(), weatherSerde))
            .selectKey((k, v) -> buildKey(v.getDate(), GeoHash.geoHashStringWithCharacterPrecision(
                v.getLatitude(), v.getLongitude(), 4)));

        // Weather records are grouped by geohash and date, and then
        // average temperature value is calculated for every group
        //
        // Suppression is used in order to emit only the "final results" from the window
        KStream<String, WeatherRecord> groupedWeather = inputWeather
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofDays(1)))
            .aggregate(
                () -> {
                    logger.debug("Aggregation initialized");
                    return new CountAndSumTemperatureAggregator();
                },
                (k, v, aggregate) -> aggregate.addValue(v),
                Materialized.with(Serdes.String(), aggregatorSerde))
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .mapValues(value -> {
                    logger.debug("Converting aggregated value: {}", value.toString());
                    return new WeatherRecord(
                        value.getSumTempF() / value.getCount(),
                        value.getSumTempC() / value.getCount(),
                        value.getDate());
                })
            .toStream()
            .selectKey((key, value) -> key.key())
            .peek((key, value) -> logger.debug("Record {}->{} is produced to grouped stream", key, value.toString()));

        inputHotels
            .join(groupedWeather,
                (hotel_val, weather_val) -> {
                    logger.debug("Joining hotel {} with weather {}", hotel_val, weather_val);
                    return hotel_val.addWeather(weather_val);
                },
                JoinWindows.of(Duration.ofDays(1)).grace(Duration.ofDays(365)),
                StreamJoined.with(Serdes.String(), hotelSerde, weatherSerde))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), hotelEnrichedSerde));

        final Topology topology = builder.build();
        logger.info("Topology description: {}", topology.describe().toString());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            if(System.getProperty("no-cleanup") == null){
                logger.info("Cleanup running");
                streams.cleanUp();
            }
            streams.start();
            logger.info("Hotel Enrichment started");
            latch.await();
        } catch (Throwable e) {
            logger.info("Hotel Enrichment exited with exit code 1 because of the following exception: {}", e.getMessage());
            System.exit(1);
        }
        logger.info("Hotel Enrichment exited with exit code 0");
        System.exit(0);
    }
}
