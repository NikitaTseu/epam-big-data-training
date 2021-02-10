package com.epam.bigdata201.kstreams.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTimestampExtractor implements TimestampExtractor {
    static final Logger logger = LoggerFactory.getLogger(CustomTimestampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Timestampable recordValue = (Timestampable) record.value();
        if(recordValue != null) {
            logger.debug("Timestamp {} is calculated for record {}", recordValue.getTimestamp(), record.value().toString());
            return recordValue.getTimestamp();
        }
        else {
            // hack
            return System.currentTimeMillis();
        }
    }
}
