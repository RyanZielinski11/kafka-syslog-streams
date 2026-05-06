package com.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
    private static final String SOURCE_STAGE = "source";

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
        KStream<String, GenericRecord> sourceStream =
            builder.stream(config.sourceTopic, Consumed.with(Serdes.String(), valueSerde));

        if (config.logIncomingRecords) {
            sourceStream.peek((key, value) -> {
                if (value != null) {
                    System.out.println("RECEIVED: " + value);
                }
            });
        }

        Map<String, RouteStage> stagesByName = config.stages.stream()
            .collect(Collectors.toMap(
                stage -> stage.name,
                stage -> stage,
                (left, right) -> {
                    throw new IllegalArgumentException("Duplicate stage name: " + left.name);
                },
                LinkedHashMap::new
            ));

        Map<String, KStream<String, GenericRecord>> builtStages = new HashMap<>();
        Set<String> visiting = new HashSet<>();
        for (RouteStage stage : config.stages) {
            materializeStage(stage.name, stagesByName, builtStages, visiting, sourceStream, valueSerde);
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), streamProperties);
        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            System.err.println("Stream thread failed: " + thread.getName());
            throwable.printStackTrace(System.err);
        });
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KStream<String, GenericRecord> materializeStage(
        String stageName,
        Map<String, RouteStage> stagesByName,
        Map<String, KStream<String, GenericRecord>> builtStages,
        Set<String> visiting,
        KStream<String, GenericRecord> sourceStream,
        GenericAvroSerde valueSerde
    ) {
        KStream<String, GenericRecord> existing = builtStages.get(stageName);
        if (existing != null) {
            return existing;
        }

        if (!visiting.add(stageName)) {
            throw new IllegalArgumentException("Cycle detected in stage definitions around " + stageName);
        }

        RouteStage stage = stagesByName.get(stageName);
        if (stage == null) {
            throw new IllegalArgumentException("Unknown stage referenced as input: " + stageName);
        }

        KStream<String, GenericRecord> parentStream = SOURCE_STAGE.equals(stage.inputStage)
            ? sourceStream
            : materializeStage(stage.inputStage, stagesByName, builtStages, visiting, sourceStream, valueSerde);

        KStream<String, GenericRecord> stageStream = parentStream
            .filter((key, value) -> stage.matches(value));

        stageStream.to(stage.outputTopic, Produced.with(Serdes.String(), valueSerde));
        builtStages.put(stageName, stageStream);
        visiting.remove(stageName);

        System.out.println("Stage loaded: " + stage.describe());
        return stageStream;
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
        System.out.println("Loaded stages: " + config.stages.size());
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

    private static Set<String> detectNames(Properties props, String prefix) {
        Set<String> names = new TreeSet<>();

        for (String key : props.stringPropertyNames()) {
            if (!key.startsWith(prefix)) {
                continue;
            }

            String suffix = key.substring(prefix.length());
            int separatorIndex = suffix.indexOf('.');
            if (separatorIndex > 0) {
                names.add(suffix.substring(0, separatorIndex));
            }
        }

        return names;
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
        private final List<RouteStage> stages;

        private AppConfig(
            String applicationId,
            String bootstrapServers,
            String schemaRegistryUrl,
            String sourceTopic,
            boolean logIncomingRecords,
            List<RouteStage> stages
        ) {
            this.applicationId = applicationId;
            this.bootstrapServers = bootstrapServers;
            this.schemaRegistryUrl = schemaRegistryUrl;
            this.sourceTopic = sourceTopic;
            this.logIncomingRecords = logIncomingRecords;
            this.stages = stages;
        }

        private static AppConfig from(Properties props) {
            String applicationId = props.getProperty("application.id", "syslog-ng-configurable-poc").trim();
            String bootstrapServers = required(props, "bootstrap.servers");
            String schemaRegistryUrl = required(props, "schema.registry.url");
            String sourceTopic = required(props, "source.topic");
            boolean logIncomingRecords = Boolean.parseBoolean(
                props.getProperty("log.incoming.records", "true"));

            List<RouteStage> stages = loadStages(props);
            if (stages.isEmpty()) {
                throw new IllegalArgumentException(
                    "No stages found. Add stage definitions like stage.security.topic=...");
            }

            return new AppConfig(
                applicationId,
                bootstrapServers,
                schemaRegistryUrl,
                sourceTopic,
                logIncomingRecords,
                stages
            );
        }

        private static List<RouteStage> loadStages(Properties props) {
            String defaultField = props.getProperty("default.match.field", DEFAULT_MATCH_FIELD).trim();
            MatchType defaultMatchType = MatchType.valueOf(
                props.getProperty("default.match.type", MatchType.CONTAINS.name()).trim()
                    .toUpperCase(Locale.ROOT));
            boolean defaultIgnoreCase = Boolean.parseBoolean(
                props.getProperty("default.ignore.case", "false"));

            List<RouteStage> stages = new ArrayList<>();
            Set<String> usedNames = new HashSet<>();

            for (String stageName : detectNames(props, "stage.")) {
                RouteStage stage = loadStage(
                    props,
                    "stage.",
                    stageName,
                    defaultField,
                    defaultMatchType,
                    defaultIgnoreCase,
                    false
                );

                if (stage != null && usedNames.add(stage.name)) {
                    stages.add(stage);
                }
            }

            for (String legacyRuleName : detectNames(props, "rule.")) {
                RouteStage stage = loadStage(
                    props,
                    "rule.",
                    legacyRuleName,
                    defaultField,
                    defaultMatchType,
                    defaultIgnoreCase,
                    true
                );

                if (stage != null) {
                    if (!usedNames.add(stage.name)) {
                        throw new IllegalArgumentException(
                            "Stage name conflict between stage.* and rule.* for " + stage.name);
                    }
                    stages.add(stage);
                }
            }

            return stages;
        }

        private static RouteStage loadStage(
            Properties props,
            String prefix,
            String name,
            String defaultField,
            MatchType defaultMatchType,
            boolean defaultIgnoreCase,
            boolean legacyRule
        ) {
            String propertyBase = prefix + name + ".";
            boolean enabled = Boolean.parseBoolean(
                props.getProperty(propertyBase + "enabled", "true"));
            if (!enabled) {
                return null;
            }

            String outputTopic = required(props, propertyBase + "topic");
            String field = props.getProperty(propertyBase + "field", defaultField).trim();
            MatchType matchType = MatchType.valueOf(
                props.getProperty(propertyBase + "matchType", defaultMatchType.name()).trim()
                    .toUpperCase(Locale.ROOT));
            MatchMode mode = MatchMode.valueOf(
                props.getProperty(propertyBase + "mode", MatchMode.INCLUDE.name()).trim()
                    .toUpperCase(Locale.ROOT));
            boolean ignoreCase = Boolean.parseBoolean(
                props.getProperty(propertyBase + "ignoreCase", Boolean.toString(defaultIgnoreCase)));
            boolean requireAllPatterns = Boolean.parseBoolean(
                props.getProperty(propertyBase + "requireAllPatterns", "false"));
            String inputStage = legacyRule
                ? SOURCE_STAGE
                : props.getProperty(propertyBase + "input", SOURCE_STAGE).trim();

            List<String> patterns = parseList(required(props, propertyBase + "patterns"));
            if (patterns.isEmpty()) {
                throw new IllegalArgumentException(
                    "Stage " + name + " must define at least one non-empty pattern.");
            }

            return new RouteStage(
                name,
                inputStage,
                field,
                outputTopic,
                matchType,
                mode,
                ignoreCase,
                requireAllPatterns,
                patterns
            );
        }
    }

    private static final class RouteStage {
        private final String name;
        private final String inputStage;
        private final String fieldPath;
        private final String outputTopic;
        private final MatchType matchType;
        private final MatchMode mode;
        private final boolean ignoreCase;
        private final boolean requireAllPatterns;
        private final List<String> rawPatterns;
        private final List<Pattern> compiledPatterns;

        private RouteStage(
            String name,
            String inputStage,
            String fieldPath,
            String outputTopic,
            MatchType matchType,
            MatchMode mode,
            boolean ignoreCase,
            boolean requireAllPatterns,
            List<String> rawPatterns
        ) {
            this.name = name;
            this.inputStage = inputStage;
            this.fieldPath = fieldPath;
            this.outputTopic = outputTopic;
            this.matchType = matchType;
            this.mode = mode;
            this.ignoreCase = ignoreCase;
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
                + " input=" + inputStage
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
