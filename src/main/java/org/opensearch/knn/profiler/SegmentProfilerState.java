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
    private static final String VECTOR_STATS_CODEC_SUFFIX = "vstats";
    private static final String VECTOR_STATS_JSON_SUFFIX = "vstats.json";
    private static final String VECTOR_STATS_FORMAT = "VectorStatsFormat";
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

        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();
        if (vectorValues == null) {
            log.warn("No vector values available for field: {}", fieldName);
            return new SegmentProfilerState(new ArrayList<>());
        }

        SegmentProfilerState profilerState = new SegmentProfilerState(new ArrayList<>());

        // Create both codec and JSON files
        String codecFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                "json"
        );

        String jsonFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                "json"
        );

        log.info("Creating stats files: {} and {}", codecFileName, jsonFileName);

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

            log.info("Processed {} vectors with {} dimensions", vectorCount, dimension);

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

            // Write JSON file
            try (IndexOutput jsonOutput = segmentWriteState.directory.createOutput(jsonFileName, segmentWriteState.context)) {
                byte[] jsonBytes = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);
                jsonOutput.writeBytes(jsonBytes, jsonBytes.length);
                log.info("Successfully wrote JSON statistics to file: {}", jsonFileName);
            }

            // Write codec file
            profilerState.output = segmentWriteState.directory.createOutput(codecFileName, segmentWriteState.context);
            CodecUtil.writeIndexHeader(
                    profilerState.output,
                    VECTOR_STATS_FORMAT,
                    CURRENT_VERSION,
                    segmentWriteState.segmentInfo.getId(),
                    segmentWriteState.segmentSuffix
            );

            // Write the same content to codec file
            byte[] jsonBytes = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);
            profilerState.output.writeVInt(jsonBytes.length);
            profilerState.output.writeBytes(jsonBytes, jsonBytes.length);
            CodecUtil.writeFooter(profilerState.output);

            log.info("Successfully wrote codec statistics to file: {}", codecFileName);

            return profilerState;
        } catch (Exception e) {
            log.error("Error during vector profiling: ", e);
            throw e;
        } finally {
            if (profilerState.output != null) {
                profilerState.output.close();
            }
        }
    }
}

/**
 * org.apache.lucene.index.CorruptIndexException: compound sub-files must have a valid codec header and footer: codec header mismatch: actual header=2065852009 vs expected header=1071082519 (resource=BufferedChecksumIndexInput(MemorySegmentIndexInput(path="/Users/oaganesh/k-NN/build/testclusters/integTest-0/data/nodes/0/indices/YfQ5ppPRQLCd69mXt3E2qw/0/index/_0_NativeEngines990KnnVectorsFormat_0.json")))
 */
