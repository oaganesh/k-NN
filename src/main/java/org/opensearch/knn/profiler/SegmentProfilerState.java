/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math3.stat.descriptive.StatisticalSummaryValues;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * SegmentProfilerState is responsible for analyzing and profiling vector data within segments.
 * This class calculates statistical measurements for each dimension of the vectors in a segment.
 */
@Log4j2
@AllArgsConstructor
public class SegmentProfilerState implements Serializable, Writeable {

    // Stores statistical summaries for each dimension of the vectors
    @Getter
    private final List<SummaryStatistics> statistics;

    @Getter
    private final int dimension;

    // public SegmentProfilerState(StreamInput in) throws IOException {
    // int size = in.readVInt();
    // this.statistics = new ArrayList<>(size);
    // for (int i = 0; i < size; i++) {
    // SummaryStatistics stats = new SummaryStatistics();
    // // Read the values
    // double mean = in.readDouble();
    // double variance = in.readDouble();
    // long n = in.readVLong();
    // double min = in.readDouble();
    // double max = in.readDouble();
    // double sum = in.readDouble();
    //
    // // Add values to the statistics object
    // // We need to add values that will result in these summary statistics
    // for (int j = 0; j < n; j++) {
    // stats.addValue(mean); // This is a simplification
    // }
    // statistics.add(stats);
    // }
    // this.dimension = in.readInt();
    // }

    /**
     * Profiles vectors in a segment by analyzing their statistical values
     * @param knnVectorValuesSupplier
     * @return SegmentProfilerState
     * @throws IOException
     */
    public static SegmentProfilerState profileVectors(final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier) throws IOException {
        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();

        if (vectorValues == null) {
            log.info("No vector values available");
            return new SegmentProfilerState(new ArrayList<>(), 0);
        }

        // Initialize vector values
        KNNCodecUtil.initializeVectorValues(vectorValues);
        List<SummaryStatistics> statistics = new ArrayList<>();

        // Return empty state if no documents are present
        if (vectorValues.docId() == NO_MORE_DOCS) {
            log.info("No vectors to profile");
            return new SegmentProfilerState(statistics, vectorValues.dimension());
        }

        int dimension = vectorValues.dimension();
        log.info("Starting vector profiling with dimension: {}", dimension);

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

        logDimensionStatistics(statistics, dimension);

        return new SegmentProfilerState(statistics, vectorValues.dimension());
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
    private static void logDimensionStatistics(final List<SummaryStatistics> statistics, final int dimension) {
        for (int i = 0; i < dimension; i++) {
            SummaryStatistics stats = statistics.get(i);
            log.info(
                "Dimension {} stats: mean={}, std={}, min={}, max={}",
                i,
                stats.getMean(),
                stats.getStandardDeviation(),
                stats.getMin(),
                stats.getMax()
            );
        }
    }

    // /**
    // * Serializes a SegmentProfilerState to a byte array
    // * @return
    // */
    // public byte[] toByteArray() {
    // try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {
    //
    // oos.writeObject(this);
    // return baos.toByteArray();
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to serialize SegmentProfilerStates", e);
    // }
    // }
    //
    // /**
    // * Deserializes a SegmentProfilerState from a byte array
    // * @param bytes
    // * @return
    // */
    // public static SegmentProfilerState fromBytes(byte[] bytes) {
    // try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes); ObjectInputStream ois = new ObjectInputStream(bais)) {
    //
    // return (SegmentProfilerState) ois.readObject();
    // } catch (IOException | ClassNotFoundException e) {
    // throw new RuntimeException("Failed to deserialize SegmentProfilerState", e);
    // }
    // }

    // public void writeTo(StreamOutput out) throws IOException {
    // out.writeVInt(statistics.size());
    // for (SummaryStatistics stats : statistics) {
    // out.writeDouble(stats.getMean());
    // out.writeDouble(stats.getVariance());
    // out.writeVLong(stats.getN());
    // out.writeDouble(stats.getMin());
    // out.writeDouble(stats.getMax());
    // out.writeDouble(stats.getSum());
    // }
    // out.writeInt(dimension);
    // }
    //
    // public byte[] toByteArray() {
    // try (BytesStreamOutput out = new BytesStreamOutput()) {
    // writeTo(out);
    // return out.bytes().toBytesRef().bytes;
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to serialize SegmentProfilerState", e);
    // }
    // }
    //
    // public static SegmentProfilerState fromBytes(byte[] bytes) {
    // try (StreamInput in = StreamInput.wrap(bytes)) {
    // return new SegmentProfilerState(in);
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to deserialize SegmentProfilerState", e);
    // }
    // }

    // public static SegmentProfilerState fromSummaryStatistics(List<SummaryStatistics> stats, int dimension) {
    // List<StatisticalSummaryValues> values = stats.stream()
    // .map(stat -> new StatisticalSummaryValues(
    // stat.getMean(), stat.getVariance(), stat.getN(),
    // stat.getMin(), stat.getMax(), stat.getSum()))
    // .collect(Collectors.toList());
    //
    // return new SegmentProfilerState(values, dimension);
    // }

    // public SegmentProfilerState(StreamInput in) throws IOException {
    // int size = in.readVInt();
    // statistics = new ArrayList<>(size);
    // for (int i = 0; i < size; i++) {
    // SummaryStatistics stats = new SummaryStatistics();
    // stats.setN(in.readVLong());
    // stats.setMin(in.readDouble());
    // stats.setMax(in.readDouble());
    // stats.setMean(in.readDouble());
    // stats.setVariance(in.readDouble());
    // stats.setSum(in.readDouble());
    // statistics.add(stats);
    // }
    // dimension = in.readVInt();
    // }

    //
    // @Override
    // public void writeTo(StreamOutput out) throws IOException {
    // out.writeVInt(statistics.size());
    // for (SummaryStatistics stats : statistics) {
    // out.writeVLong((long)stats.getN());
    // out.writeDouble(stats.getMin());
    // out.writeDouble(stats.getMax());
    // out.writeDouble(stats.getMean());
    // out.writeDouble(stats.getVariance());
    // out.writeDouble(stats.getSum());
    // }
    // out.writeVInt(dimension);
    // }
    //
    // // Replace the existing toByteArray method
    // public byte[] toByteArray() {
    // try (BytesStreamOutput out = new BytesStreamOutput()) {
    // writeTo(out);
    // return BytesReference.toBytes(out.bytes());
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to serialize SegmentProfilerState", e);
    // }
    // }
    //
    // // Replace the existing fromBytes method
    //// public static SegmentProfilerState fromBytes(byte[] bytes) {
    //// try (StreamInput in = StreamInput.wrap(bytes)) {
    //// return new SegmentProfilerState(in);
    //// } catch (IOException e) {
    //// throw new RuntimeException("Failed to deserialize SegmentProfilerState", e);
    //// }
    //// }
    //
    // public static SegmentProfilerState fromBytes(byte[] bytes) {
    // // Check the magic number for Java serialization
    // if (bytes.length >= 2 && bytes[0] == (byte)0xac && bytes[1] == (byte)0xed) {
    // // Legacy format
    // try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    // ObjectInputStream ois = new ObjectInputStream(bais)) {
    // return (SegmentProfilerState) ois.readObject();
    // } catch (IOException | ClassNotFoundException e) {
    // throw new RuntimeException("Failed to deserialize SegmentProfilerState using legacy format", e);
    // }
    // } else {
    // // New format
    // try (StreamInput in = StreamInput.wrap(bytes)) {
    // //return new SegmentProfilerState();
    // return fromBytes(bytes);
    // //return (SegmentProfilerState) in.readObject();
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to deserialize SegmentProfilerState", e);
    // }
    // }
    // }

    // public byte[] toByteArray() {
    // try (BytesStreamOutput out = new BytesStreamOutput()) {
    // // Write dimension
    // out.writeVInt(dimension);
    //
    // // Write statistics list size
    // out.writeVInt(statistics.size());
    //
    // // Write each statistic's values
    // for (SummaryStatistics stat : statistics) {
    // out.writeDouble(stat.getMean());
    // out.writeDouble(stat.getVariance());
    // out.writeVLong(stat.getN());
    // out.writeDouble(stat.getMin());
    // out.writeDouble(stat.getMax());
    // out.writeDouble(stat.getSum());
    // }
    //
    // return out.bytes().toBytesRef().bytes;
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to serialize SegmentProfilerState", e);
    // }
    // }
    //
    // public static SegmentProfilerState fromBytes(byte[] bytes) {
    // try (StreamInput in = StreamInput.wrap(bytes)) {
    // // Read dimension
    // int dimension = in.readVInt();
    //
    // // Read statistics list
    // int statsSize = in.readVInt();
    // List<SummaryStatistics> statistics = new ArrayList<>(statsSize);
    //
    // // Read each statistic's values
    // for (int i = 0; i < statsSize; i++) {
    // SummaryStatistics stat = new SummaryStatistics();
    // double mean = in.readDouble();
    // double variance = in.readDouble();
    // long n = in.readVLong();
    // double min = in.readDouble();
    // double max = in.readDouble();
    // double sum = in.readDouble();
    //
    // // Reconstruct the statistics
    // for (int j = 0; j < n; j++) {
    // stat.addValue(mean); // This will update n and sum
    // }
    // // Manually set the other values since they can't be directly set
    // statistics.add(stat);
    // }
    //
    // return new SegmentProfilerState(statistics, dimension);
    // } catch (IOException e) {
    // throw new RuntimeException("Failed to deserialize SegmentProfilerState", e);
    // }
    // }

    public SegmentProfilerState(StreamInput in) throws IOException {
        this.dimension = in.readVInt();
        int size = in.readVInt();
        this.statistics = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            // Read the statistical values
            double mean = in.readDouble();
            double variance = in.readDouble();
            long n = in.readVLong();
            double min = in.readDouble();
            double max = in.readDouble();
            double sum = in.readDouble();

            // Create a new SummaryStatistics instance
            SummaryStatistics stats = new SummaryStatistics();

            // Use AggregateSummaryStatistics to create and aggregate a single StatisticalSummaryValues
            List<StatisticalSummaryValues> values = List.of(new StatisticalSummaryValues(mean, variance, n, min, max, sum));

            // Convert the aggregated result back to SummaryStatistics
            StatisticalSummaryValues aggregated = AggregateSummaryStatistics.aggregate(values);
            stats.addValue(aggregated.getMean()); // Add at least one value to initialize

            statistics.add(stats);
        }
    }

    // public SegmentProfilerState(StreamInput in) throws IOException {
    // this.dimension = in.readVInt();
    // int size = in.readVInt();
    //
    // log.debug("[KNN] Deserializing SegmentProfilerState with dimension: {} and size: {}", dimension, size);
    //
    // this.statistics = new ArrayList<>(size);
    // for (int i = 0; i < size; i++) {
    // try {
    // double mean = in.readDouble();
    // double variance = in.readDouble();
    // long n = in.readVLong();
    // double min = in.readDouble();
    // double max = in.readDouble();
    // double sum = in.readDouble();
    //
    // SummaryStatistics stats = new SummaryStatistics();
    // if (n > 0) {
    // stats.addValue(mean); // Initialize with mean to get valid statistics
    // }
    // statistics.add(stats);
    //
    // log.debug("[KNN] Deserialized dimension {}: n={}, mean={}, variance={}", i, n, mean, variance);
    // } catch (Exception e) {
    // log.error("[KNN] Error deserializing statistics for dimension {}: {}", i, e.getMessage());
    // statistics.add(new SummaryStatistics()); // Add empty statistics on error
    // }
    // }
    // }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(dimension);
        out.writeVInt(statistics.size());
        for (SummaryStatistics stat : statistics) {
            out.writeDouble(stat.getMean());
            out.writeDouble(stat.getVariance());
            out.writeVLong(stat.getN());
            out.writeDouble(stat.getMin());
            out.writeDouble(stat.getMax());
            out.writeDouble(stat.getSum());
        }
    }

    public static byte[] serialize(SegmentProfilerState state) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            state.writeTo(out);
            return out.bytes().toBytesRef().bytes;
        }
    }

    public byte[] toByteArray() throws IOException {
        return serialize(this);
    }

    public static SegmentProfilerState deserialize(byte[] bytes) throws IOException {
        try (StreamInput in = StreamInput.wrap(bytes)) {
            return new SegmentProfilerState(in);
        }
    }
}
