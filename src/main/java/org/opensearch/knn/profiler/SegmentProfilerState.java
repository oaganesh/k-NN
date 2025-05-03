/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.quantizationservice.QuantizationService;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.knn.quantization.enums.ScalarQuantizationType;
import org.opensearch.knn.quantization.models.quantizationParams.QuantizationParams;
import org.opensearch.knn.quantization.models.quantizationParams.ScalarQuantizationParams;
import org.opensearch.knn.quantization.models.quantizationState.MultiBitScalarQuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.OneBitScalarQuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;
import org.opensearch.knn.quantization.models.quantizationOutput.QuantizationOutput;
import org.opensearch.knn.quantization.quantizer.MultiBitScalarQuantizer;
import org.opensearch.knn.quantization.quantizer.OneBitScalarQuantizer;
import org.opensearch.knn.quantization.quantizer.Quantizer;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * SegmentProfilerState is responsible for analyzing and profiling vector data within segments.
 * This class calculates statistical measurements for each dimension of the vectors in a segment.
 */
@Log4j2
@AllArgsConstructor
public class SegmentProfilerState implements Serializable {

    @Getter
    private final List<SummaryStatistics> statistics;

    @Getter
    private final int dimension;

    @Getter
    private final String segmentId;

    @Getter
    private final boolean quantizedStats;

    public SegmentProfilerState(List<SummaryStatistics> statistics, int dimension, String segmentId) {
        this(statistics, dimension, segmentId, false); // Default to original vectors
    }

    /**
     * Profiles vectors in a segment by analyzing their statistical values
     * @param knnVectorValuesSupplier
     * @return SegmentProfilerState
     * @throws IOException
     */
    public static SegmentProfilerState profileVectors(final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier, final String segmentId)
        throws IOException {
        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();

        if (vectorValues == null) {
            log.info("No vector values available");
            return new SegmentProfilerState(new ArrayList<>(), 0, segmentId);
        }

        // Initialize vector values
        KNNCodecUtil.initializeVectorValues(vectorValues);
        List<SummaryStatistics> statistics = new ArrayList<>();

        // Return empty state if no documents are present
        if (vectorValues.docId() == NO_MORE_DOCS) {
            log.info("No vectors to profile");
            return new SegmentProfilerState(statistics, vectorValues.dimension(), segmentId);
        }

        int dimension = vectorValues.dimension();
        log.info("Starting vector profiling with dimension: {} for segment: {}", dimension, segmentId);

        // Initialize statistics collectors for each dimension
        for (int i = 0; i < dimension; i++) {
            statistics.add(new SummaryStatistics());
        }

        // Process all vectors
        int vectorCount = 0;
        for (int doc = vectorValues.docId(); doc != NO_MORE_DOCS; doc = vectorValues.nextDoc()) {
            vectorCount++;
            processVectors(vectorValues.getVector(), statistics);
        }

        log.info("Vector profiling completed - processed {} vectors", vectorCount);

        logDimensionStatistics(statistics, dimension, segmentId);

        return new SegmentProfilerState(statistics, vectorValues.dimension(), segmentId, false);
    }

    /**
     * Helper method to process a vector and update statistics
     * @param vector
     * @param statistics
     */
    private static <T> void processVectors(T vector, List<SummaryStatistics> statistics) {
        if (vector instanceof float[]) {
            processFloatVector((float[]) vector, statistics);
        } else if (vector instanceof byte[]) {
            processByteVector((byte[]) vector, statistics);
        } else {
            log.warn("Unsupported vector type: {}.", vector.getClass());
        }
    }

    /**
     * Processes a float vector by updating the statistical summaries for each dimension
     * @param vector
     * @param statistics
     */
    private static void processFloatVector(float[] vector, List<SummaryStatistics> statistics) {
        for (int j = 0; j < vector.length; j++) {
            statistics.get(j).addValue(vector[j]);
        }
    }

    /**
     * Processes a byte vector by updating the statistical summaries for each dimension
     * @param vector
     * @param statistics
     */
    private static void processByteVector(byte[] vector, List<SummaryStatistics> statistics) {
        for (int j = 0; j < vector.length; j++) {
            statistics.get(j).addValue(vector[j] & 0xFF);
        }
    }

    /**
     * Helper method to log statistics for each dimension
     * @param statistics
     * @param dimension
     */
    private static void logDimensionStatistics(final List<SummaryStatistics> statistics, final int dimension, final String segmentId) {
        for (int i = 0; i < dimension; i++) {
            SummaryStatistics stats = statistics.get(i);
            log.info(
                "Segment {} - Dimension {} stats: mean={}, std={}, min={}, max={}",
                segmentId,
                i,
                stats.getMean(),
                stats.getStandardDeviation(),
                stats.getMin(),
                stats.getMax()
            );
        }
    }

    /**
     * Serializes a SegmentProfilerState to a byte array
     * @return
     */
    public byte[] toByteArray() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeObject(this);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize SegmentProfilerStates", e);
        }
    }

    /**
     * Deserializes a SegmentProfilerState from a byte array
     * @param bytes
     * @return
     */
    public static SegmentProfilerState fromBytes(byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (SegmentProfilerState) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize SegmentProfilerState", e);
        }
    }

    public static SegmentProfilerState profileQuantizedVectors(
            final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
            final String segmentId,
            final QuantizationState quantizationState,
            final QuantizationParams quantizationParams,
            final QuantizationService quantizationService) throws IOException {

        // Create appropriate quantizer based on the quantization type
        ScalarQuantizationType sqType = null;
        Quantizer<float[], byte[]> quantizer = null;

        if (quantizationParams instanceof ScalarQuantizationParams) {
            sqType = ((ScalarQuantizationParams) quantizationParams).getSqType();
            switch (sqType) {
                case ONE_BIT:
                    quantizer = new OneBitScalarQuantizer();
                    log.info("Using OneBitScalarQuantizer for profiling");
                    break;
                case TWO_BIT:
                    quantizer = new MultiBitScalarQuantizer(2);
                    log.info("Using MultiBitScalarQuantizer(2) for profiling");
                    break;
                case FOUR_BIT:
                    quantizer = new MultiBitScalarQuantizer(4);
                    log.info("Using MultiBitScalarQuantizer(4) for profiling");
                    break;
                default:
                    log.warn("Unsupported quantization type: {}", sqType);
                    return new SegmentProfilerState(new ArrayList<>(), 0, segmentId, true);
            }
        } else {
            log.warn("Unsupported quantization params type: {}", quantizationParams.getClass().getName());
            return new SegmentProfilerState(new ArrayList<>(), 0, segmentId, true);
        }

        // Get vector values
        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();
        if (vectorValues == null) {
            return new SegmentProfilerState(new ArrayList<>(), 0, segmentId, true);
        }

        // Initialize first vector
        if (vectorValues.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
            return new SegmentProfilerState(new ArrayList<>(), 0, segmentId, true);
        }

        // Get first vector and dimension
        float[] firstVector = (float[]) vectorValues.getVector();
        int dimension = firstVector.length;

        // Determine bits per value and prepare stats
        int bitsPerValue = sqType == ScalarQuantizationType.ONE_BIT ? 1 :
                sqType == ScalarQuantizationType.TWO_BIT ? 2 : 4;
        int valuesPerByte = 8 / bitsPerValue;
        int mask = (1 << bitsPerValue) - 1;

        log.info("Using {} bits per value for quantization profiling", bitsPerValue);

        // Create statistics arrays
        List<SummaryStatistics> originalStats = new ArrayList<>(dimension);
        List<SummaryStatistics> quantizedStats = new ArrayList<>(dimension);

        for (int i = 0; i < dimension; i++) {
            originalStats.add(new SummaryStatistics());
            quantizedStats.add(new SummaryStatistics());
        }

        // Process all vectors
        int vectorCount = 0;
        int[] valueCounts = new int[1 << bitsPerValue]; // Count occurrences of each value

        do {
            vectorCount++;
            float[] vector = (float[]) vectorValues.getVector();

            // Update original stats
            for (int i = 0; i < dimension; i++) {
                originalStats.get(i).addValue(vector[i]);
            }

            // Quantize using the actual quantizer
            QuantizationOutput<byte[]> output = (QuantizationOutput<byte[]>) quantizationService.createQuantizationOutput(quantizationParams);
            quantizer.quantize(vector, quantizationState, output);
            byte[] quantized = output.getQuantizedVector();

            // Analyze the quantized values
            for (int byteIdx = 0; byteIdx < quantized.length; byteIdx++) {
                int currentByte = quantized[byteIdx] & 0xFF;

                for (int valueIdx = 0; valueIdx < valuesPerByte; valueIdx++) {
                    int dimensionIdx = byteIdx * valuesPerByte + valueIdx;
                    if (dimensionIdx >= dimension) break; // Don't exceed the dimension

                    int shiftAmount = (valuesPerByte - 1 - valueIdx) * bitsPerValue;
                    int value = (currentByte >> shiftAmount) & mask;

                    quantizedStats.get(dimensionIdx).addValue(value);
                    valueCounts[value]++;
                }
            }
        } while (vectorValues.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);

        log.info("Processed {} vectors with {} dimensions", vectorCount, dimension);

        // Log distribution of quantized values
        log.info("Distribution of quantized values:");
        for (int i = 0; i < valueCounts.length; i++) {
            double percentage = vectorCount > 0 ?
                    (double) valueCounts[i] / (vectorCount * dimension) * 100 : 0;
            log.info("Value {}: {} occurrences ({}%)",
                    i, valueCounts[i], String.format("%.2f", percentage));
        }

        // Log a sample of the dimension statistics
        log.info("Sample dimension statistics:");
        for (int i = 0; i < Math.min(5, dimension); i++) {
            SummaryStatistics origStats = originalStats.get(i);
            SummaryStatistics quantStats = quantizedStats.get(i);

            log.info("Dimension {}: Original [min={}, max={}, mean={}], " +
                            "Quantized [min={}, max={}, mean={}]",
                    i,
                    String.format("%.3f", origStats.getMin()),
                    String.format("%.3f", origStats.getMax()),
                    String.format("%.3f", origStats.getMean()),
                    String.format("%.3f", quantStats.getMin()),
                    String.format("%.3f", quantStats.getMax()),
                    String.format("%.3f", quantStats.getMean()));
        }

        return new SegmentProfilerState(quantizedStats, dimension, segmentId, true);
    }
}
