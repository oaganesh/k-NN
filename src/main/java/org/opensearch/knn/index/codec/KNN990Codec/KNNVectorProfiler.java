/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;
import java.util.*;
import java.util.stream.Collectors;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.FileOutputStream;

/**
 * A utility class for profiling vector data statistics.
 */
public class KNNVectorProfiler {

    private int dimensions;
    private double[] sum;
    private double[] sumOfSquares;
    private int count;
    private List<float[]> samples;
    private double sampleRate;
    private boolean enabled;
    private String outputFilePath;

    public KNNVectorProfiler(int dimensions, double sampleRate, boolean enabled, String outputFilePath) {
        this.dimensions = dimensions;
        this.sum = new double[dimensions];
        this.sumOfSquares = new double[dimensions];
        this.count = 0;
        this.samples = new ArrayList<>();
        this.sampleRate = sampleRate;
        this.enabled = enabled;
        this.outputFilePath = outputFilePath;
    }

    /**
     * Adds a vector to the statistics calculation.
     * @param vector the vector to add
     */
    public void addVector(float[] vector) {
        if (!enabled || vector.length != dimensions) {
            return;
        }
        if (Math.random() > sampleRate) {
            return;
        }
        for (int i = 0; i < dimensions; i++) {
            sum[i] += vector[i];
            sumOfSquares[i] += vector[i] * vector[i];
        }
        count++;
        samples.add(vector);
    }

    /**
     * Stores computed statistics to a file.
     */
    public void storeStatistics() {
        if (!enabled) return;
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFilePath))) {
            dos.writeInt(dimensions);
            dos.writeInt(count);
            for (double value : computeMean()) {
                dos.writeDouble(value);
            }
            for (double value : computeVariance()) {
                dos.writeDouble(value);
            }
            for (double value : computeSparsity()) {
                dos.writeDouble(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Computes the mean for each dimension.
     * @return mean values per dimension
     */
    public double[] computeMean() {
        double[] mean = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            mean[i] = count > 0 ? sum[i] / count : 0;
        }
        return mean;
    }

    /**
     * Computes the variance for each dimension.
     * @return variance values per dimension
     */
    public double[] computeVariance() {
        double[] variance = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            double mean = sum[i] / count;
            variance[i] = count > 1 ? (sumOfSquares[i] / count) - (mean * mean) : 0;
        }
        return variance;
    }

    /**
     * Computes a sparsity metric based on zero values.
     * @return sparsity metric per dimension
     */
    public double[] computeSparsity() {
        if (!enabled) return new double[dimensions];
        double[] sparsity = new double[dimensions];
        for (int i = 0; i < dimensions; i++) {
            final int dim = i;
            long zeroCount = samples.stream().filter(v -> v[dim] == 0).count();
            sparsity[i] = (double) zeroCount / count;
        }
        return sparsity;
    }

    /**
     * Retrieves the number of vectors processed.
     * @return count of vectors
     */
    public int getCount() {
        return count;
    }

    /**
     * Enables or disables profiling dynamically.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return "VectorProfiler{" +
                "dimensions=" + dimensions +
                ", count=" + count +
                ", mean=" + Arrays.toString(computeMean()) +
                ", variance=" + Arrays.toString(computeVariance()) +
                ", sparsity=" + Arrays.toString(computeSparsity()) +
                ", enabled=" + enabled +
                " sampleRate=" + sampleRate +
                " outputFilePath=" + outputFilePath +
                '}';
    }
}
