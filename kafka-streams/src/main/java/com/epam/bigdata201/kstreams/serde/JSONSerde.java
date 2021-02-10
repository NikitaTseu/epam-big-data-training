package com.epam.bigdata201.kstreams.serde;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JSONSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    private Gson gson = new GsonBuilder().serializeNulls().create();
    private Class<T> deserializedClass;

    public JSONSerde(Class<T> deserializedClass) {
        if(deserializedClass == null){
            throw new NullPointerException("NULL class has been passed");
        }
        this.deserializedClass = deserializedClass;
    }

    public JSONSerde() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        return gson.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    static final Logger logger = LoggerFactory.getLogger(JSONSerde.class);

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        else {
            try{
                return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), deserializedClass);
            }
            catch (Exception e){
                logger.error("ERROR DURING DESERIALIZING OF: {}", new String(bytes, StandardCharsets.UTF_8));
                throw e;
            }

        }
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
