package com.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    private static final String DEFAULT_CONFIG_FILE = "syslog-streams.properties";
    private static final String DEFAULT_MATCH_FIELD = "rawMessage";

    public static void main(String[] args) throws IOException {
        String configFile = args.length > 0 ? args[0] : DEFAULT_CONFIG_FILE;
        Properties appConfig = loadConfig(configFile);
        AppConfig config = AppConfig.from(appConfig);

        Properties streamProperties = new Properties();
        streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId);
        streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        streamProperties.put("schema.registry.url", config.schemaRegistryUrl);
        streamProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        GenericAvroSerde valueSerde = new GenericAvroSerde();
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", config.schemaRegistryUrl);
        valueSerde.configure(serdeConfig, false);

        logStartup(configFile, config);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> syslog =
            builder.stream(config.sourceTopic, Consumed.with(Serdes.String(), valueSerde));

        if (config.logIncomingRecords) {
            syslog.peek((key, value) -> {
                if (value != null) {
                    System.out.println("RECEIVED: " + value);
                }
            });
        }

        for (FilterRule rule : config.rules) {
            syslog.filter((key, value) -> rule.matches(value))
                .to(rule.outputTopic, Produced.with(Serdes.String(), valueSerde));
            System.out.println("Rule loaded: " + rule.describe());
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), streamProperties);
        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            System.err.println("Stream thread failed: " + thread.getName());
            throwable.printStackTrace(System.err);
        });
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties loadConfig(String configFile) throws IOException {
        Properties properties = new Properties();

        try (InputStream inputStream = new FileInputStream(configFile)) {
            properties.load(inputStream);
        }

        return properties;
    }

    private static void logStartup(String configFile, AppConfig config) {
        System.out.println("Starting Syslog Streams App");
        System.out.println("Config file: " + configFile);
        System.out.println("Application ID: " + config.applicationId);
        System.out.println("Source topic: " + config.sourceTopic);
        System.out.println("Schema Registry URL: " + config.schemaRegistryUrl);
        System.out.println("Loaded rules: " + config.rules.size());
    }

    private static String required(Properties props, String key) {
        String value = props.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }

        return value.trim();
    }

    private static List<String> parseList(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(rawValue.split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .collect(Collectors.toList());
    }

    private static Set<String> detectRuleNames(Properties props) {
        Set<String> ruleNames = new TreeSet<>();

        for (String key : props.stringPropertyNames()) {
            if (!key.startsWith("rule.")) {
                continue;
            }

            String suffix = key.substring("rule.".length());
            int separatorIndex = suffix.indexOf('.');
            if (separatorIndex > 0) {
                ruleNames.add(suffix.substring(0, separatorIndex));
            }
        }

        return ruleNames;
    }

    private static Object resolveField(GenericRecord record, String fieldPath) {
        Object current = record;

        for (String segment : fieldPath.split("\\.")) {
            if (current == null) {
                return null;
            }

            if (current instanceof GenericRecord) {
                GenericRecord genericRecord = (GenericRecord) current;
                current = genericRecord.get(segment);
                continue;
            }

            if (current instanceof Map<?, ?>) {
                Map<?, ?> map = (Map<?, ?>) current;
                current = map.get(segment);
                continue;
            }

            return null;
        }

        return current;
    }

    private enum MatchType {
        CONTAINS,
        REGEX
    }

    private enum MatchMode {
        INCLUDE,
        EXCLUDE
    }

    private static final class AppConfig {
        private final String applicationId;
        private final String bootstrapServers;
        private final String schemaRegistryUrl;
        private final String sourceTopic;
        private final boolean logIncomingRecords;
        private final List<FilterRule> rules;

        private AppConfig(
            String applicationId,
            String bootstrapServers,
            String schemaRegistryUrl,
            String sourceTopic,
            boolean logIncomingRecords,
            List<FilterRule> rules
        ) {
            this.applicationId = applicationId;
            this.bootstrapServers = bootstrapServers;
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.sourceTopic = sourceTopic;
            this.logIncomingRecords = logIncomingRecords;
            this.rules = rules;
        }

        private static AppConfig from(Properties props) {
            String applicationId = props.getProperty("application.id", "syslog-ng-configurable-poc").trim();
            String bootstrapServers = required(props, "bootstrap.servers");
            String schemaRegistryUrl = required(props, "schema.registry.url");
            String sourceTopic = required(props, "source.topic");
            boolean logIncomingRecords = Boolean.parseBoolean(
                props.getProperty("log.incoming.records", "true"));

            List<FilterRule> rules = loadRules(props);
            if (rules.isEmpty()) {
                throw new IllegalArgumentException(
                    "No filter rules found. Add rules like rule.infoblox.patterns=netauto_");
            }

            return new AppConfig(
                applicationId,
                bootstrapServers,
                schemaRegistryUrl,
                sourceTopic,
                logIncomingRecords,
                rules
            );
        }

        private static List<FilterRule> loadRules(Properties props) {
            String defaultField = props.getProperty("default.match.field", DEFAULT_MATCH_FIELD).trim();
            MatchType defaultMatchType = MatchType.valueOf(
                props.getProperty("default.match.type", MatchType.CONTAINS.name()).trim()
                    .toUpperCase(Locale.ROOT));
            boolean defaultIgnoreCase = Boolean.parseBoolean(
                props.getProperty("default.ignore.case", "false"));

            List<FilterRule> rules = new ArrayList<>();
            for (String ruleName : detectRuleNames(props)) {
                String outputTopic = required(props, "rule." + ruleName + ".topic");
                String field = props.getProperty("rule." + ruleName + ".field", defaultField).trim();
                MatchType matchType = MatchType.valueOf(
                    props.getProperty("rule." + ruleName + ".matchType", defaultMatchType.name()).trim()
                        .toUpperCase(Locale.ROOT));
                MatchMode mode = MatchMode.valueOf(
                    props.getProperty("rule." + ruleName + ".mode", MatchMode.INCLUDE.name()).trim()
                        .toUpperCase(Locale.ROOT));
                boolean ignoreCase = Boolean.parseBoolean(
                    props.getProperty("rule." + ruleName + ".ignoreCase",
                        Boolean.toString(defaultIgnoreCase)));
                boolean enabled = Boolean.parseBoolean(
                    props.getProperty("rule." + ruleName + ".enabled", "true"));
                boolean requireAllPatterns = Boolean.parseBoolean(
                    props.getProperty("rule." + ruleName + ".requireAllPatterns", "false"));

                List<String> patterns = parseList(required(props, "rule." + ruleName + ".patterns"));
                if (patterns.isEmpty()) {
                    throw new IllegalArgumentException(
                        "Rule " + ruleName + " must define at least one non-empty pattern.");
                }

                rules.add(new FilterRule(
                    ruleName,
                    field,
                    outputTopic,
                    matchType,
                    mode,
                    ignoreCase,
                    enabled,
                    requireAllPatterns,
                    patterns
                ));
            }

            return rules.stream()
                .filter(rule -> rule.enabled)
                .collect(Collectors.toList());
        }
    }

    private static final class FilterRule {
        private final String name;
        private final String fieldPath;
        private final String outputTopic;
        private final MatchType matchType;
        private final MatchMode mode;
        private final boolean ignoreCase;
        private final boolean enabled;
        private final boolean requireAllPatterns;
        private final List<String> rawPatterns;
        private final List<Pattern> compiledPatterns;

        private FilterRule(
            String name,
            String fieldPath,
            String outputTopic,
            MatchType matchType,
            MatchMode mode,
            boolean ignoreCase,
            boolean enabled,
            boolean requireAllPatterns,
            List<String> rawPatterns
        ) {
            this.name = name;
            this.fieldPath = fieldPath;
            this.outputTopic = outputTopic;
            this.matchType = matchType;
            this.mode = mode;
            this.ignoreCase = ignoreCase;
            this.enabled = enabled;
            this.requireAllPatterns = requireAllPatterns;
            this.rawPatterns = List.copyOf(rawPatterns);
            this.compiledPatterns = compilePatterns(rawPatterns, matchType, ignoreCase);
        }

        private boolean matches(GenericRecord record) {
            Object fieldValue = resolveField(record, fieldPath);
            if (fieldValue == null) {
                return false;
            }

            String normalizedValue = fieldValue.toString();
            if (ignoreCase && matchType == MatchType.CONTAINS) {
                normalizedValue = normalizedValue.toLowerCase(Locale.ROOT);
            }

            boolean foundMatch = requireAllPatterns
                ? allPatternsMatch(normalizedValue)
                : anyPatternMatches(normalizedValue);

            return mode == MatchMode.INCLUDE ? foundMatch : !foundMatch;
        }

        private String describe() {
            return name
                + " field=" + fieldPath
                + " type=" + matchType
                + " mode=" + mode
                + " topic=" + outputTopic
                + " patterns=" + rawPatterns;
        }

        private static List<Pattern> compilePatterns(
            List<String> patterns,
            MatchType matchType,
            boolean ignoreCase
        ) {
            int flags = ignoreCase ? Pattern.CASE_INSENSITIVE : 0;
            List<Pattern> compiled = new ArrayList<>(patterns.size());

            for (String pattern : patterns) {
                String expression = matchType == MatchType.CONTAINS
                    ? Pattern.quote(normalizeContainsPattern(pattern, ignoreCase))
                    : pattern;
                compiled.add(Pattern.compile(expression, flags));
            }

            return compiled;
        }

        private static String normalizeContainsPattern(String pattern, boolean ignoreCase) {
            return ignoreCase ? pattern.toLowerCase(Locale.ROOT) : pattern;
        }

        private boolean allPatternsMatch(String value) {
            for (Pattern pattern : compiledPatterns) {
                if (!matchesPattern(value, pattern)) {
                    return false;
                }
            }

            return true;
        }

        private boolean anyPatternMatches(String value) {
            for (Pattern pattern : compiledPatterns) {
                if (matchesPattern(value, pattern)) {
                    return true;
                }
            }

            return false;
        }

        private boolean matchesPattern(String value, Pattern pattern) {
            return pattern.matcher(value).find();
        }
    }
}
