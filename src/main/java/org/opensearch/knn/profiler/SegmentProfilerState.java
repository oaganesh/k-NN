/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.knn.index.KNNIndexShard;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;
import org.opensearch.knn.index.vectorvalues.KNNVectorValuesFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * SegmentProfilerState is responsible for analyzing and profiling vector data within segments.
 * This class calculates statistical measurements for each dimension of the vectors in a segment.
 */
@Log4j2
public class SegmentProfilerState {
    private static final String VECTOR_STATS_EXTENSION = "json";
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
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
                    jsonBuilder.field("min", formatDouble(stats.getMin()));
                    jsonBuilder.field("max", formatDouble(stats.getMax()));
                    jsonBuilder.field("sum", formatDouble(stats.getSum()));
                    jsonBuilder.field("mean", formatDouble(stats.getMean()));
                    jsonBuilder.field("standardDeviation", formatDouble(Math.sqrt(stats.getVariance())));
                    jsonBuilder.field("variance", formatDouble(stats.getVariance()));
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
                StandardOpenOption.WRITE
        );

        log.info("Successfully wrote vector stats for field {} to file: {}", fieldName, outputFile);
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

        List<SummaryStatistics> statisticsList = new ArrayList<>();
        int vectorCount = 0;

        try {
            // Process vectors and collect statistics
            KNNCodecUtil.initializeVectorValues(vectorValues);
            int dimension = vectorValues.dimension();

            // Initialize statistics collectors
            for (int i = 0; i < dimension; i++) {
                statisticsList.add(new SummaryStatistics());
            }

            // Process all vectors
            while (vectorValues.docId() != NO_MORE_DOCS) {
                vectorCount++;
                Object vector = vectorValues.getVector();
                if (vector instanceof float[]) {
                    float[] floatVector = (float[]) vector;
                    for (int j = 0; j < floatVector.length; j++) {
                        statisticsList.get(j).addValue(floatVector[j]);
                    }
                } else if (vector instanceof byte[]) {
                    byte[] byteVector = (byte[]) vector;
                    for (int j = 0; j < byteVector.length; j++) {
                        statisticsList.get(j).addValue(byteVector[j] & 0xFF);
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
            Directory directory = getUnderlyingDirectory(segmentWriteState.directory);
            Path directoryPath = ((FSDirectory) directory).getDirectory();
            Path statsFile = directoryPath.resolve(statsFileName);

            // Write statistics to file
            writeStatsToFile(statsFile, statisticsList, fieldName, vectorCount);

            return new SegmentProfilerState(statisticsList);

        } catch (Exception e) {
            log.error("Error during vector profiling for field {}: ", fieldName, e);
            throw e;
        }
    }

    /**
     * Profile vectors from a LeafReader
     * @param reader LeafReader to extract vector data from
     * @param fieldName Name of the KNN vector field
     * @return SegmentProfilerState with calculated statistics
     */
    public static SegmentProfilerState profileVectorsFromLeafReader(LeafReader reader, String fieldName) throws IOException {
        log.info("Starting vector profiling for field: {} from LeafReader", fieldName);

        FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(fieldName);
        if (fieldInfo == null || !fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
            log.warn("No KNN vector field found with name: {}", fieldName);
            return new SegmentProfilerState(new ArrayList<>());
        }

        // Create custom KNNVectorValues from the reader
        KNNVectorValues<?> vectorValues = null;
        try {
            vectorValues = KNNVectorValuesFactory.getVectorValues(fieldInfo, reader);
        } catch (Exception e) {
            return new SegmentProfilerState(new ArrayList<>());
        }

        if (vectorValues == null) {
            log.warn("No vector values available for field: {}", fieldName);
            return new SegmentProfilerState(new ArrayList<>());
        }

        List<SummaryStatistics> statisticsList = new ArrayList<>();
        int vectorCount = 0;

        try {
            // Initialize vector values and prepare statistics collectors
            KNNCodecUtil.initializeVectorValues(vectorValues);
            int dimension = vectorValues.dimension();

            for (int i = 0; i < dimension; i++) {
                statisticsList.add(new SummaryStatistics());
            }

            // Process all vectors in the segment
            while (vectorValues.docId() != NO_MORE_DOCS) {
                vectorCount++;
                Object vector = vectorValues.getVector();
                if (vector instanceof float[]) {
                    float[] floatVector = (float[]) vector;
                    for (int j = 0; j < floatVector.length; j++) {
                        statisticsList.get(j).addValue(floatVector[j]);
                    }
                } else if (vector instanceof byte[]) {
                    byte[] byteVector = (byte[]) vector;
                    for (int j = 0; j < byteVector.length; j++) {
                        statisticsList.get(j).addValue(byteVector[j] & 0xFF);
                    }
                }
                vectorValues.nextDoc();
            }

            log.info("Processed {} vectors with {} dimensions for field {}", vectorCount, dimension, fieldName);

            if (reader instanceof SegmentReader) {
                SegmentReader segmentReader = (SegmentReader) reader;
                // Create stats file name
                String statsFileName = IndexFileNames.segmentFileName(
                        segmentReader.getSegmentInfo().info.name,
                        "",  // No segment suffix for already committed segments
                        VECTOR_STATS_EXTENSION
                );

                // Get directory path and create output file
                Directory directory = getUnderlyingDirectory(segmentReader.directory());
                Path directoryPath = ((FSDirectory) directory).getDirectory();
                Path statsFile = directoryPath.resolve(statsFileName);

                // Write statistics to file
                writeStatsToFile(statsFile, statisticsList, fieldName, vectorCount);
            }

            return new SegmentProfilerState(statisticsList);

        } catch (Exception e) {
            log.error("Error during vector profiling from LeafReader for field {}: ", fieldName, e);
            throw e;
        }
    }

    /**
     * Gets the underlying FSDirectory from a potentially nested Directory
     */
    private static Directory getUnderlyingDirectory(Directory directory) throws IOException {
        while (directory instanceof FilterDirectory) {
            directory = ((FilterDirectory) directory).getDelegate();
        }

        if (!(directory instanceof FSDirectory)) {
            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
        }

        return directory;
    }

    /**
     * Generate index statistics and output to XContentBuilder
     */
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
                    builder.field("std_dev", formatDouble(Math.sqrt(dimStats.getVariance())));
                    builder.field("variance", formatDouble(dimStats.getVariance()));
                    builder.field("count", dimStats.getN());
                    builder.field("sum", formatDouble(dimStats.getSum()));
                    builder.endObject();
                }
                builder.endArray();

                // Add derived metrics
                builder.field("sparsity", calculateSparsity(stats));
                builder.field("clustering_coefficient", calculateClusteringCoefficient(stats));
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

    /**
     * Get summary statistics for all shards in an index
     */
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

                        if (Files.exists(indexPath)) {
                            Files.list(indexPath).forEach(path -> {
                                // Check if file matches the stats file pattern
                                if (path.getFileName().toString().endsWith("." + VECTOR_STATS_EXTENSION)) {
                                    try {
                                        String jsonContent = Files.readString(path);
                                        List<SummaryStatistics> shardStatsData = parseStatsFromJson(jsonContent);
                                        if (!shardStatsData.isEmpty()) {
                                            if (stats.isEmpty()) {
                                                stats.addAll(shardStatsData);
                                            } else {
                                                // Merge statistics
                                                mergeStatistics(stats, shardStatsData);
                                            }
                                        }
                                    } catch (Exception e) {
                                        log.error("Error reading or parsing file: {}", path, e);
                                    }
                                }
                            });
                        } else {
                            log.warn("Directory does not exist: {}", indexPath);
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

    /**
     * Merge two sets of statistics
     */
    public static void mergeStatistics(List<SummaryStatistics> target, List<SummaryStatistics> source) {
        for (int i = 0; i < Math.min(target.size(), source.size()); i++) {
            SummaryStatistics existing = target.get(i);
            SummaryStatistics current = source.get(i);

            // Instead of trying to reconstruct the distribution, just add all values directly
            if (current.getN() > 0) {
                existing.addValue(current.getMin());
                if (current.getN() > 1) {
                    existing.addValue(current.getMax());
                }
            }
        }
    }

    /**
     * Parse statistics from JSON content
     */
    private static List<SummaryStatistics> parseStatsFromJson(String jsonContent) throws IOException {
        List<SummaryStatistics> statistics = new ArrayList<>();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                jsonContent)) {

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
                                }
                            }
                            // Only add the actual values we know
                            stats.addValue(min);
                            if (max > min) {
                                stats.addValue(max);
                            }

                            statistics.add(stats);
                        }
                    }
                }
            }
        }

        return statistics;
    }

    /**
     * Format double values to a consistent precision
     */
    public static double formatDouble(double value) {
        return Double.parseDouble(DECIMAL_FORMAT.format(value));
    }

    /**
     * Calculate vector sparsity (proportion of near-zero values)
     */
    public static double calculateSparsity(List<SummaryStatistics> stats) {
        return stats.stream()
                .mapToDouble(s -> s.getN() > 0 ? s.getMean() : 0)
                .filter(v -> Math.abs(v) < 0.0001)
                .count() / (double) stats.size();
    }

    /**
     * Calculate clustering coefficient based on variance
     */
    public static double calculateClusteringCoefficient(List<SummaryStatistics> stats) {
        return stats.stream()
                .mapToDouble(SummaryStatistics::getVariance)
                .average()
                .orElse(0.0);
    }
}
