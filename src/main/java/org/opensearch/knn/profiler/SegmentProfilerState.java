/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    private static final String VECTOR_STATS_CODEC_EXTENSION = "json";
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.####");
    private static final int CURRENT_VERSION = 1;

    @Getter
    private final List<SummaryStatistics> statistics;
    private IndexOutput output;

    public SegmentProfilerState(final List<SummaryStatistics> statistics) {
        this.statistics = statistics;
    }

    public static SegmentProfilerState profileVectors(
            final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
            final SegmentWriteState segmentWriteState,
            final String fieldName
    ) throws IOException {
        log.info("Starting vector profiling for field: {}", fieldName);

        // Log directory details
        try {
            log.info("Directory class: {}", segmentWriteState.directory.getClass().getName());

            // List all existing files in the directory
            log.info("Existing files in directory:");
            String[] files = segmentWriteState.directory.listAll();
            for (String file : files) {
                log.info(" - {}", file);
            }
        } catch (Exception e) {
            log.warn("Could not list directory files: {}", e.getMessage());
        }

        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();
        if (vectorValues == null) {
            log.warn("No vector values available for field: {}", fieldName);
            return new SegmentProfilerState(new ArrayList<>());
        }

        SegmentProfilerState profilerState = new SegmentProfilerState(new ArrayList<>());

        // Create a properly formatted codec file name
        String codecFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                VECTOR_STATS_CODEC_EXTENSION
        );

        log.info("Creating stats file: {} for segment: {} with suffix: {}",
                codecFileName,
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix);

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

            // Create JSON content
            XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
            jsonBuilder.startObject();
            {
                jsonBuilder.field("fieldName", fieldName);
                jsonBuilder.field("vectorCount", vectorCount);
                jsonBuilder.field("dimension", dimension);
                jsonBuilder.field("segmentName", segmentWriteState.segmentInfo.name);
                jsonBuilder.startArray("dimensions");
                for (int i = 0; i < profilerState.statistics.size(); i++) {
                    SummaryStatistics stats = profilerState.statistics.get(i);
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
            String jsonContent = jsonBuilder.toString();
            log.info("Created JSON content: {}", jsonContent.substring(0, Math.min(200, jsonContent.length())) + "...");

            try {
                // Write a single codec file with proper headers and footers
                log.info("Opening output file: {}", codecFileName);
                profilerState.output = segmentWriteState.directory.createOutput(codecFileName, segmentWriteState.context);
                log.info("Successfully opened output file");

                log.info("Writing codec header");
                CodecUtil.writeIndexHeader(
                        profilerState.output,
                        VECTOR_STATS_CODEC_NAME,
                        CURRENT_VERSION,
                        segmentWriteState.segmentInfo.getId(),
                        segmentWriteState.segmentSuffix
                );
                log.info("Successfully wrote codec header");

                // Write the JSON content to codec file
                byte[] jsonBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
                log.info("Writing content length: {} bytes", jsonBytes.length);
                profilerState.output.writeVInt(jsonBytes.length);
                log.info("Writing content bytes");
                profilerState.output.writeBytes(jsonBytes, jsonBytes.length);
                log.info("Successfully wrote content bytes");

                log.info("Writing codec footer");
                CodecUtil.writeFooter(profilerState.output);
                log.info("Successfully wrote codec footer");

                // List all files after write operation
                try {
                    log.info("Files in directory after write:");
                    String[] filesAfter = segmentWriteState.directory.listAll();
                    for (String file : filesAfter) {
                        log.info(" - {}", file);
                    }
                } catch (Exception e) {
                    log.warn("Could not list directory files after write: {}", e.getMessage());
                }
            } catch (Exception e) {
                log.error("Error while writing output file: ", e);
                throw e;
            }

            log.info("Successfully wrote vector stats for field {} to file: {}", fieldName, codecFileName);

            return profilerState;
        } catch (Exception e) {
            log.error("Error during vector profiling for field {}: ", fieldName, e);
            throw e;
        } finally {
            if (profilerState.output != null) {
                try {
                    log.info("Closing output file");
                    profilerState.output.close();
                    log.info("Successfully closed output file");
                } catch (Exception e) {
                    log.error("Error closing output file: ", e);
                }
            }
        }
    }
}
