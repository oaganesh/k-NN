/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.query;

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.LeafReader;
import org.opensearch.knn.index.codec.KNN990Codec.QuantizationConfigKNNCollector;
import org.opensearch.knn.index.quantizationservice.QuantizationService;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.opensearch.knn.profiler.SegmentProfilerState.processVector;

/**
 * A utility class for doing Quantization related operation at a segment level. We can move this utility in {@link SegmentLevelQuantizationInfo}
 * but I am keeping it thinking that {@link SegmentLevelQuantizationInfo} free from these utility functions to reduce
 * the responsibilities of {@link SegmentLevelQuantizationInfo} class.
 */
@UtilityClass
@Log4j2
public class SegmentLevelQuantizationUtil {

    /**
     * A simple function to convert a vector to a quantized vector for a segment.
     * @param vector array of float
     * @return array of byte
     */
    @SuppressWarnings("unchecked")
    public static byte[] quantizeVector(final float[] vector, final SegmentLevelQuantizationInfo segmentLevelQuantizationInfo) {
        if (segmentLevelQuantizationInfo == null) {
            return null;
        }
        final QuantizationService quantizationService = QuantizationService.getInstance();
        // TODO: We are converting the output of Quantize to byte array for now. But this needs to be fixed when
        // other types of quantized outputs are returned like float[].
        return (byte[]) quantizationService.quantize(
            segmentLevelQuantizationInfo.getQuantizationState(),
            vector,
            quantizationService.createQuantizationOutput(segmentLevelQuantizationInfo.getQuantizationParams())
        );
    }

    /**
     * A utility function to get {@link QuantizationState} for a given segment and field.
     * @param leafReader {@link LeafReader}
     * @param fieldName {@link String}
     * @return {@link QuantizationState}
     * @throws IOException exception during reading the {@link QuantizationState}
     */
    static QuantizationState getQuantizationState(final LeafReader leafReader, String fieldName) throws IOException {
        final QuantizationConfigKNNCollector tempCollector = new QuantizationConfigKNNCollector();
        leafReader.searchNearestVectors(fieldName, new float[0], tempCollector, null);
        if (tempCollector.getQuantizationState() == null) {
            throw new IllegalStateException(String.format(Locale.ROOT, "No quantization state found for field %s", fieldName));
        }
        return tempCollector.getQuantizationState();
    }

//    /**
//     * Gets aggregate statistics for vectors in a leaf reader
//     * @param leafReader LeafReader to read vectors from
//     * @param fieldName Field name to get statistics for
//     * @return List of SummaryStatistics for each dimension
//     */
//    public static List<SummaryStatistics> getAggregateStatistics(final LeafReader leafReader, String fieldName) throws IOException {
//        final QuantizationConfigKNNCollector tempCollector = new QuantizationConfigKNNCollector();
//        leafReader.searchNearestVectors(fieldName, new float[0], tempCollector, null);
//
//        List<SummaryStatistics> statistics = new ArrayList<>();
//        QuantizationState quantState = tempCollector.getQuantizationState();
//
//        if (quantState == null) {
//            throw new IllegalStateException(String.format(Locale.ROOT, "No quantization state found for field %s", fieldName));
//        }
//
//        // Initialize statistics based on dimensions from quantization state
//        int dimensions = quantState.getDimensions();
//        for (int i = 0; i < dimensions; i++) {
//            statistics.add(new SummaryStatistics());
//        }
//
//        // Get vectors using the same collector type
//        QuantizationConfigKNNCollector vectorCollector = new QuantizationConfigKNNCollector();
//        leafReader.searchNearestVectors(fieldName, new float[dimensions], vectorCollector, null);
//
//        // Get KNNVectorValues from the collector's state
//        KNNVectorValues<?> vectorValues = vectorCollector.getQuantizationState().getVectorValues();
//        if (vectorValues != null) {
//            // Process each vector
//            while (vectorValues.nextDoc() != NO_MORE_DOCS) {
//                Object vector = vectorValues.getVector();
//                processVector(vector, statistics);
//            }
//        }
//
//        return statistics;
//    }

}
