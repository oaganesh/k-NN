/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

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
            jsonBuilder.field("timestamp", System.currentTimeMillis());
            jsonBuilder.field("fieldName", fieldName);
            jsonBuilder.field("vectorCount", vectorCount);
            jsonBuilder.field("dimension", statistics.size());

            jsonBuilder.startArray("dimensions");
            for (int i = 0; i < statistics.size(); i++) {
                SummaryStatistics stats = statistics.get(i);
                jsonBuilder.startObject();
                {
                    jsonBuilder.field("dimension", i);
                    jsonBuilder.field("mean", Double.parseDouble(DECIMAL_FORMAT.format(stats.getMean())));
                    jsonBuilder.field("standardDeviation", Double.parseDouble(DECIMAL_FORMAT.format(stats.getStandardDeviation())));
                    jsonBuilder.field("min", Double.parseDouble(DECIMAL_FORMAT.format(stats.getMin())));
                    jsonBuilder.field("max", Double.parseDouble(DECIMAL_FORMAT.format(stats.getMax())));
                    jsonBuilder.field("count", stats.getN());
                    jsonBuilder.field("sum", Double.parseDouble(DECIMAL_FORMAT.format(stats.getSum())));
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
}
