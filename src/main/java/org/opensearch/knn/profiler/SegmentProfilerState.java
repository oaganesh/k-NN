/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import java.time.format.DateTimeFormatter;
import java.time.Instant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.*;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.apache.lucene.store.Directory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import java.nio.charset.StandardCharsets;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import java.util.Map;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * SegmentProfilerState is responsible for analyzing and profiling vector data within segments.
 * This class calculates statistical measurements for each dimension of the vectors in a segment.
 */
@Log4j2
public class SegmentProfilerState {
    private static final String VECTOR_STATS_CODEC_NAME = "VectorStatsFormat";
    private static final String VECTOR_STATS_EXTENSION = "json";
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private static final int CURRENT_VERSION = 1;
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    @Getter
    private final List<SummaryStatistics> statistics;

    public SegmentProfilerState(final List<SummaryStatistics> statistics) {
        this.statistics = statistics;
    }

    /**
     * Writes statistics to a file in JSON format
     */
    private static void writeStatsToFile(Path outputFile, List<SummaryStatistics> statistics, String fieldName, int vectorCount)
        throws IOException {
        Files.createDirectories(outputFile.getParent());

        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.prettyPrint();
        jsonBuilder.startObject();
        {
            jsonBuilder.field("timestamp", ISO_FORMATTER.format(Instant.now()));
            jsonBuilder.field("fieldName", fieldName);
            jsonBuilder.field("vectorCount", vectorCount);
            jsonBuilder.field("dimension", statistics.size());

            jsonBuilder.startArray("dimensions");
            for (int i = 0; i < statistics.size(); i++) {
                SummaryStatistics stats = statistics.get(i);
                jsonBuilder.startObject();
                {
                    jsonBuilder.field("dimension", i);
                    jsonBuilder.field("count", stats.getN());
                    jsonBuilder.field("min", Double.parseDouble(DECIMAL_FORMAT.format(stats.getMin())));
                    jsonBuilder.field("max", Double.parseDouble(DECIMAL_FORMAT.format(stats.getMax())));
                    jsonBuilder.field("sum", Double.parseDouble(DECIMAL_FORMAT.format(stats.getSum())));
                    jsonBuilder.field("mean", Double.parseDouble(DECIMAL_FORMAT.format(stats.getMean())));
                    jsonBuilder.field("standardDeviation", Double.parseDouble(DECIMAL_FORMAT.format(Math.sqrt(stats.getVariance()))));
                    jsonBuilder.field("variance", Double.parseDouble(DECIMAL_FORMAT.format(stats.getVariance())));
                }
                jsonBuilder.endObject();
            }
            jsonBuilder.endArray();
        }
        jsonBuilder.endObject();

        Files.write(
            outputFile,
            jsonBuilder.toString().getBytes(StandardCharsets.UTF_8),
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        );
    }

    /**
     * Reads statistics from a JSON file
     */
    public static List<SummaryStatistics> readStatsFromFile(Path statsFile) throws IOException {
        if (!Files.exists(statsFile)) {
            throw new IOException("Stats file does not exist: " + statsFile);
        }

        List<SummaryStatistics> statistics = new ArrayList<>();
        String content = new String(Files.readAllBytes(statsFile), StandardCharsets.UTF_8);

        try {
            // Parse the JSON content and reconstruct statistics
            // This is a placeholder - implement actual JSON parsing based on your needs
            log.info("Reading stats from file: {}", statsFile);

            // Return reconstructed statistics
            return statistics;
        } catch (Exception e) {
            log.error("Error reading stats from file: {}", statsFile, e);
            throw new IOException("Failed to read stats from file", e);
        }
    }

    /**
     * Profiles vectors and generates statistics
     */
    public static SegmentProfilerState profileVectors(
        final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        final SegmentWriteState segmentWriteState,
        final String fieldName
    ) throws IOException {
        log.info("Starting vector profiling for field: {}", fieldName);

        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();
        if (vectorValues == null) {
            log.warn("No vector values available for field: {}", fieldName);
            return new SegmentProfilerState(new ArrayList<>());
        }

        SegmentProfilerState profilerState = new SegmentProfilerState(new ArrayList<>());

        try {
            // Process vectors and collect statistics
            KNNCodecUtil.initializeVectorValues(vectorValues);
            int dimension = vectorValues.dimension();
            int vectorCount = 0;

            // Initialize statistics collectors
            for (int i = 0; i < dimension; i++) {
                profilerState.statistics.add(new SummaryStatistics());
            }

            // Process all vectors
            while (vectorValues.docId() != NO_MORE_DOCS) {
                vectorCount++;
                Object vector = vectorValues.getVector();
                if (vector instanceof float[]) {
                    float[] floatVector = (float[]) vector;
                    for (int j = 0; j < floatVector.length; j++) {
                        profilerState.statistics.get(j).addValue(floatVector[j]);
                    }
                } else if (vector instanceof byte[]) {
                    byte[] byteVector = (byte[]) vector;
                    for (int j = 0; j < byteVector.length; j++) {
                        profilerState.statistics.get(j).addValue(byteVector[j] & 0xFF);
                    }
                }
                vectorValues.nextDoc();
            }

            log.info("Processed {} vectors with {} dimensions for field {}", vectorCount, dimension, fieldName);

            // Create stats file name
            String statsFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                VECTOR_STATS_EXTENSION
            );

            // Get directory path and create output file
            Directory directory = segmentWriteState.directory;
            while (directory instanceof FilterDirectory) {
                directory = ((FilterDirectory) directory).getDelegate();
            }

            if (!(directory instanceof FSDirectory)) {
                throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
            }

            Path directoryPath = ((FSDirectory) directory).getDirectory();
            Path statsFile = directoryPath.resolve(statsFileName);

            // Write statistics to file
            writeStatsToFile(statsFile, profilerState.statistics, fieldName, vectorCount);

            log.info("Successfully wrote vector stats for field {} to file: {}", fieldName, statsFileName);

            return profilerState;

        } catch (Exception e) {
            log.error("Error during vector profiling for field {}: ", fieldName, e);
            throw e;
        }
    }

    // Add to SegmentProfilerState.java

    public static void getIndexStats(IndexService indexService, XContentBuilder builder) throws IOException {
        builder.field("vector_sample_count", getTotalVectorCount(indexService));
        builder.field("last_updated", ISO_FORMATTER.format(Instant.now()));

        builder.startObject("field_stats");
        for (String fieldName : getKNNFields(indexService)) {
            builder.startObject(fieldName);

            // Get dimension stats
            builder.startArray("dimension_stats");
            List<SummaryStatistics> stats = getFieldStats(indexService, fieldName);
            for (SummaryStatistics stat : stats) {
                builder.startObject();
                builder.field("min", stat.getMin());
                builder.field("max", stat.getMax());
                builder.field("mean", stat.getMean());
                builder.field("variance", stat.getVariance());
                builder.endObject();
            }
            builder.endArray();

            // Add distance distribution
            builder.startObject("distance_distribution");
            addDistanceStats(indexService, fieldName, builder);
            builder.endObject();

            // Add other metrics
            builder.field("sparsity", calculateSparsity(stats));
            builder.field("clustering_coefficient", calculateClusteringCoefficient(stats));

            builder.endObject();
        }
        builder.endObject();
    }

    public static void getIndexStats(IndexStats indexStats, XContentBuilder builder, Environment environment) throws IOException {
        try {
            log.info("Starting to gather index stats for index: {}", indexStats.getIndex());

            // Basic index statistics
            builder.startObject("index_summary");
            builder.field("doc_count", indexStats.getTotal().getDocs().getCount());
            builder.field("size_in_bytes", indexStats.getTotal().getStore().getSizeInBytes());
            builder.field("timestamp", ISO_FORMATTER.format(Instant.now()));
            builder.endObject();

            // Vector statistics with summary stats
            builder.startObject("vector_stats");
            builder.field("sample_size", indexStats.getTotal().getDocs().getCount());

            // Add summary statistics
            builder.startObject("summary_stats");

            // Get the statistics from stored files
            List<SummaryStatistics> stats = getSummaryStatisticsForIndex(indexStats, environment);

            if (!stats.isEmpty()) {
                log.info("Found {} dimensions with statistics", stats.size());
                builder.startArray("dimensions");
                for (int i = 0; i < stats.size(); i++) {
                    SummaryStatistics dimStats = stats.get(i);
                    builder.startObject();
                    builder.field("dimension", i);
                    builder.field("min", formatDouble(dimStats.getMin()));
                    builder.field("max", formatDouble(dimStats.getMax()));
                    builder.field("mean", formatDouble(dimStats.getMean()));
                    builder.field("std_dev", formatDouble(dimStats.getStandardDeviation()));
                    builder.field("variance", formatDouble(dimStats.getVariance()));
                    builder.field("count", dimStats.getN());
                    builder.field("sum", formatDouble(dimStats.getSum()));
                    builder.endObject();
                }
                builder.endArray();
            } else {
                log.warn("No statistics found for index: {}", indexStats.getIndex());
                builder.field("status", "No statistics available");
            }

            builder.endObject(); // end summary_stats
            builder.endObject(); // end vector_stats

        } catch (Exception e) {
            log.error("Error generating index stats", e);
            builder.startObject("error");
            builder.field("message", "Failed to get statistics: " + e.getMessage());
            builder.endObject();
        }
    }

    private static List<SummaryStatistics> getSummaryStatisticsForIndex(IndexStats indexStats, Environment environment) {
        List<SummaryStatistics> stats = new ArrayList<>();
        try {
            ShardStats[] shardStats = indexStats.getShards();
            log.info("Processing {} shards for index: {}", shardStats != null ? shardStats.length : 0, indexStats.getIndex());

            if (shardStats != null) {
                for (ShardStats shard : shardStats) {
                    try {
                        int shardId = shard.getShardRouting().shardId().getId();
                        String indexUUID = shard.getShardRouting().shardId().getIndex().getUUID();

                        // Construct the path using nodes/0/indices structure
                        Path indexPath = environment.dataFiles()[0].resolve("nodes")
                            .resolve("0")
                            .resolve("indices")
                            .resolve(indexUUID)
                            .resolve(String.valueOf(shardId))
                            .resolve("index");

                        log.info("Looking for stats in directory: {}", indexPath);

                        // List all files in the directory
                        if (Files.exists(indexPath)) {
                            log.info("Files in directory {}:", indexPath);
                            Files.list(indexPath).forEach(path -> {
                                log.info("Found file: {}", path.getFileName());
                                // Check if file matches the pattern we're looking for
                                if (path.getFileName().toString().contains("NativeEngines990KnnVectors")) {
                                    log.info("Found potential stats file: {}", path.getFileName());
                                    try {
                                        String jsonContent = Files.readString(path);
                                        log.info("Successfully read content from file: {}", path);
                                        log.info(
                                            "Content preview: {}",
                                            jsonContent.length() > 100 ? jsonContent.substring(0, 100) + "..." : jsonContent
                                        );

                                        List<SummaryStatistics> shardStatsData = parseStatsFromJson(jsonContent);
                                        if (!shardStatsData.isEmpty()) {
                                            log.info("Successfully parsed statistics from file: {}", path);
                                            if (stats.isEmpty()) {
                                                stats.addAll(shardStatsData);
                                            } else {
                                                // Merge statistics
                                                for (int i = 0; i < stats.size(); i++) {
                                                    SummaryStatistics existing = stats.get(i);
                                                    SummaryStatistics current = shardStatsData.get(i);
                                                    existing.addValue(current.getSum());
                                                    existing.addValue(current.getMin());
                                                    existing.addValue(current.getMax());
                                                }
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error("Error reading or parsing file: {}", path, e);
                                    }
                                }
                            });
                        } else {
                            log.warn("Directory does not exist: {}", indexPath);
                            // Log parent directories to help debug
                            Path parent = indexPath.getParent();
                            while (parent != null && !parent.equals(environment.dataFiles()[0])) {
                                log.info("Checking parent directory: {}", parent);
                                if (Files.exists(parent)) {
                                    log.info("Contents of {}: ", parent);
                                    Files.list(parent).forEach(p -> log.info("  - {}", p.getFileName()));
                                } else {
                                    log.info("Parent directory does not exist: {}", parent);
                                }
                                parent = parent.getParent();
                            }
                        }
                    } catch (Exception e) {
                        log.error("Error processing shard stats: {}", e.getMessage(), e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error reading statistics files", e);
        }
        return stats;
    }

    private static List<SummaryStatistics> parseStatsFromJson(String jsonContent) throws IOException {
        List<SummaryStatistics> statistics = new ArrayList<>();

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                jsonContent
            )
        ) {

            XContentParser.Token token;
            String currentFieldName = null;

            while ((token = parser.nextToken()) != null) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ("dimensions".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                            SummaryStatistics stats = new SummaryStatistics();
                            double min = Double.MAX_VALUE;
                            double max = Double.MIN_VALUE;
                            double sum = 0;
                            long count = 0;

                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                String fieldName = parser.currentName();
                                parser.nextToken();

                                switch (fieldName) {
                                    case "min":
                                        min = parser.doubleValue();
                                        break;
                                    case "max":
                                        max = parser.doubleValue();
                                        break;
                                    case "sum":
                                        sum = parser.doubleValue();
                                        break;
                                    case "count":
                                        count = parser.longValue();
                                        break;
                                }
                            }

                            // Add the actual values to reconstruct the statistics
                            if (count > 0) {
                                // Add min and max values
                                stats.addValue(min);
                                if (count > 1) {
                                    stats.addValue(max);
                                }

                                // If there are values between min and max, add the mean value
                                // for the remaining count to maintain the correct sum and distribution
                                if (count > 2) {
                                    double remainingMean = (sum - min - max) / (count - 2);
                                    for (int i = 0; i < count - 2; i++) {
                                        stats.addValue(remainingMean);
                                    }
                                }
                            }

                            statistics.add(stats);
                        }
                    }
                }
            }
        }

        return statistics;
    }

    private static double formatDouble(double value) {
        // Format to 4 decimal places
        return Double.parseDouble(DECIMAL_FORMAT.format(value));
    }

    private static double calculateSparsity(List<SummaryStatistics> stats) {
        // Implementation for calculating vector sparsity
        return stats.stream().mapToDouble(s -> s.getN() > 0 ? s.getSum() / s.getN() : 0).filter(v -> Math.abs(v) < 0.0001).count()
            / (double) stats.size();
    }

    private static double calculateClusteringCoefficient(List<SummaryStatistics> stats) {
        // Implementation for calculating clustering coefficient
        // This is a simplified version - you may want to implement a more sophisticated algorithm
        return stats.stream().mapToDouble(SummaryStatistics::getVariance).average().orElse(0.0);
    }

    private static void addDistanceStats(IndexService indexService, String fieldName, XContentBuilder builder) throws IOException {
        // Calculate distance statistics
        builder.field("min_distance", 0.0123);  // Replace with actual calculation
        builder.field("max_distance", 1.9876);  // Replace with actual calculation
        builder.field("mean_distance", 0.7654); // Replace with actual calculation

        builder.startObject("percentiles");
        builder.field("p50", 0.7123);  // Replace with actual percentile calculation
        builder.field("p75", 1.0234);
        builder.field("p90", 1.3456);
        builder.field("p99", 1.7890);
        builder.endObject();
    }

    private static long getTotalVectorCount(IndexService indexService) throws IOException {
        long totalCount = 0;
        for (String fieldName : getKNNFields(indexService)) {
            // Use IndexShard's acquireSearcher() directly
            try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("knn-stats")) {
                for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                    SegmentReader segmentReader = Lucene.segmentReader(leafContext.reader());
                    FieldInfo fieldInfo = segmentReader.getFieldInfos().fieldInfo(fieldName);
                    if (fieldInfo != null && fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
                        totalCount += segmentReader.numDocs();
                    }
                }
            }
        }
        return totalCount;
    }

    private static List<String> getKNNFields(IndexService indexService) {
        List<String> knnFields = new ArrayList<>();
        MapperService mapperService = indexService.mapperService();

        // Convert Iterable to Map
        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        mapperService.fieldTypes().forEach(fieldType -> fieldTypes.put(fieldType.name(), fieldType));

        // Filter for KNN vector fields
        for (Map.Entry<String, MappedFieldType> entry : fieldTypes.entrySet()) {
            // Check if field is a KNN vector field using the content type
            if (KNNVectorFieldMapper.CONTENT_TYPE.equals(entry.getValue().typeName())) {
                knnFields.add(entry.getKey());
            }
        }

        return knnFields;
    }

    private static List<SummaryStatistics> getFieldStats(IndexService indexService, String fieldName) throws IOException {
        List<SummaryStatistics> fieldStats = new ArrayList<>();

        // Use IndexShard's acquireSearcher() directly
        try (Engine.Searcher searcher = indexService.getShard(0).acquireSearcher("knn-stats")) {
            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(leafContext.reader());
                String statsFileName = getStatsFileName(segmentReader, fieldName);

                if (statsFileName != null) {
                    Path statsFile = getStatsFilePath(segmentReader, statsFileName);
                    if (Files.exists(statsFile)) {
                        List<SummaryStatistics> segmentStats = readStatsFromFile(statsFile);
                        if (fieldStats.isEmpty()) {
                            fieldStats.addAll(segmentStats);
                        } else {
                            // Merge statistics from multiple segments
                            for (int i = 0; i < segmentStats.size(); i++) {
                                SummaryStatistics existing = fieldStats.get(i);
                                SummaryStatistics current = segmentStats.get(i);

                                // Clear existing stats
                                existing.clear();

                                // Add all values from both statistics
                                existing.addValue(current.getMin());
                                existing.addValue(current.getMax());

                                // Add the mean value weighted by count
                                double meanValue = current.getMean();
                                for (int j = 0; j < current.getN(); j++) {
                                    existing.addValue(meanValue);
                                }
                            }
                        }
                    }
                }
            }
        }

        return fieldStats;
    }

    private static String getStatsFileName(SegmentReader segmentReader, String fieldName) {
        // Use segmentInfo.info.name instead of just name
        return IndexFileNames.segmentFileName(
            segmentReader.getSegmentInfo().info.name,
            "", // Empty string instead of getSegmentSuffix()
            VECTOR_STATS_EXTENSION
        );
    }

    private static Path getStatsFilePath(SegmentReader segmentReader, String statsFileName) throws IOException {
        Directory directory = segmentReader.directory();
        while (directory instanceof FilterDirectory) {
            directory = ((FilterDirectory) directory).getDelegate();
        }

        if (!(directory instanceof FSDirectory)) {
            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
        }

        return ((FSDirectory) directory).getDirectory().resolve(statsFileName);
    }
}
