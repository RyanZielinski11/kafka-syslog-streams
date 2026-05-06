package com.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Predicate;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

public class SyslogStreamsApp {

    private static final String APP_ID = setting("APP_ID", "syslog-ng-streams");
    private static final String BOOTSTRAP_SERVERS = setting("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String SCHEMA_REGISTRY_URL = setting("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    private static final String INPUT_TOPIC = setting("INPUT_TOPIC", "syslog.raw.avro");
    private static final String INFOBLOX_TOPIC = setting("INFOBLOX_TOPIC", "syslog.infoblox");
    private static final String SECURITY_TOPIC = setting("SECURITY_TOPIC", "syslog.security");
    private static final String DEFAULT_TOPIC = setting("DEFAULT_TOPIC", "syslog.default");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();
        GenericAvroSerde valueSerde = new GenericAvroSerde();

        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        valueSerde.configure(serdeConfig, false);

        KStream<String, GenericRecord> syslog =
            builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), valueSerde));

        syslog.peek((k, v) -> {
            if (v != null) {
                System.out.println("RECEIVED: " + v);
            }
        });

        syslog.filter(matches(raw -> raw.contains("netauto_")))
            .to(INFOBLOX_TOPIC,
             Produced.with(Serdes.String(), valueSerde));

        syslog.filter(matches(raw ->
                raw.contains("sshd")
                    || raw.contains("LOGIN")
                    || raw.contains("AUTH_FAIL")))
            .to(SECURITY_TOPIC,
             Produced.with(Serdes.String(), valueSerde));

        syslog.filter(matches(raw -> !raw.contains("netauto_")))
            .to(DEFAULT_TOPIC,
             Produced.with(Serdes.String(), valueSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            System.err.println("Stream thread failed: " + thread.getName());
            throwable.printStackTrace(System.err);
        });
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static String setting(String key, String defaultValue) {
        String systemValue = System.getProperty(key);
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue;
        }

        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        return defaultValue;
    }

    private static Predicate<GenericRecord> matches(Predicate<String> rawMatcher) {
        return value -> {
            if (value == null) {
                return false;
            }

            Object rawObj = value.get("rawMessage");
            if (rawObj == null) {
                return false;
            }

            return rawMatcher.test(rawObj.toString());
        };
    }
}
