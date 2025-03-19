/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.lucene.store.FSDirectory;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Utility class for performing vector calculations and profiling operations
 * on collections of float arrays representing vectors.
 * @param <T>
 */
public class VectorProfiler<T extends Computation> {
    private static final ConcurrentHashMap<VectorProfiler<?>, VectorProfiler<?>> SEGMENT_CONTEXTS = new ConcurrentHashMap<>();

    private final String segBaseName;
    private final String segSuffix;
    private final Path directoryPath;
    private final float[] sumPerDimension;
    private final T computation;
    private long count;

    private final List<float[]> allVectors = new ArrayList<>();

    /**
     * Constructor for VectorProfiler
     * @param segBaseName
     * @param segSuffix
     * @param directoryPath
     * @param dimension
     * @param computation
     */
    public VectorProfiler(String segBaseName, String segSuffix, Path directoryPath, int dimension, T computation) {
        this.segBaseName = segBaseName;
        this.segSuffix = segSuffix;
        this.directoryPath = directoryPath;
        this.sumPerDimension = new float[dimension];
        this.computation = computation;
        this.count = 0;
    }

    /**
     * Calculates a result vector based on a collection of input vectors using the specified computation.
     *
     * @param <T> Generic type extending Computation interface
     * @param vectors Collection of float arrays representing input vectors
     * @param computation The computation to be performed on the vectors
     * @return float array representing the calculated result vector
     * @throws IllegalArgumentException if vectors is null, empty, or contains vectors of different dimensions
     */
    public static <T extends Computation> void recordReadTimeVectors(
        String segBaseName,
        String segSuffix,
        Path directoryPath,
        Collection<float[]> vectors,
        T computation
    ) {
        System.out.println("Entering recordReadTimeVectors");
        System.out.println("Vectors null? " + (vectors == null));
        System.out.println("Vectors empty? " + (vectors == null ? "N/A" : vectors.isEmpty()));

        if (vectors == null || vectors.isEmpty()) {
            System.out.println("Vectors collection is null or empty");
            return;
        }
        try {
            int dim = vectors.iterator().next().length;
            System.out.println("Processing vectors with dimension: " + dim);
            System.out.println("Number of vectors: " + vectors.size());

            VectorProfiler<T> key = new VectorProfiler<>(segBaseName, segSuffix, directoryPath, dim, computation);

            @SuppressWarnings("unchecked")
            VectorProfiler<T> context = (VectorProfiler<T>) SEGMENT_CONTEXTS.computeIfAbsent(
                key,
                k -> new VectorProfiler<>(segBaseName, segSuffix, directoryPath, dim, computation)
            );

            System.out.println("Adding vectors to context");
            context.addVectors(vectors);

            System.out.println("Writing read-time stats");
            writeReadTimeStats(context);

        } catch (Exception e) {
            System.err.println("Error in recordReadTimeVectors: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Calculates a result vector based on a collection of input vectors using the specified computation.
     * @param vectors
     * @param computation
     * @return
     * @param <T>
     */
    public static <T extends Computation> float[] calculateVector(Collection<float[]> vectors, T computation) {
        System.out.println("Entering calculateVector");
        System.out.println("Vectors null? " + (vectors == null));
        System.out.println("Vectors empty? " + (vectors == null ? "N/A" : vectors.isEmpty()));

        if (vectors == null || vectors.isEmpty()) {
            String errorMsg = "Vectors collection cannot be null or empty";
            System.err.println(errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

        try {
            float[] firstVector = vectors.iterator().next();
            int dim = firstVector.length;
            System.out.println("Vector dimension: " + dim);

            float[] result = new float[dim];
            int count = vectors.size();

            Arrays.fill(result, 0);

            for (float[] vec : vectors) {
                if (vec.length != dim) {
                    throw new IllegalArgumentException("All vectors must have same dimension");
                }
                for (int i = 0; i < dim; i++) {
                    result[i] = computation.apply(result[i], vec[i])[0];
                }
            }

            return result;
        } catch (Exception e) {
            System.err.println("Error in calculateVector: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Saves vector statistics (mean, variance, standard deviation) to a file.
     * @param segmentWriteState
     * @param vectors
     * @throws IOException
     */
    public static void saveVectorStats(SegmentWriteState segmentWriteState, Collection<float[]> vectors) throws IOException {
        // Calculate all statistics
        float[] meanVector = calculateVector(vectors, StatisticalOperators.MEAN);
        float[] varianceVector = calculateVector(vectors, StatisticalOperators.VARIANCE);
        float[] stdDevVector = calculateVector(vectors, StatisticalOperators.STANDARD_DEVIATION);

        String statsFileName = IndexFileNames.segmentFileName(segmentWriteState.segmentInfo.name, segmentWriteState.segmentSuffix, "txt");

        Directory directory = segmentWriteState.directory;
        while (directory instanceof FilterDirectory) {
            directory = ((FilterDirectory) directory).getDelegate();
        }

        if (!(directory instanceof FSDirectory)) {
            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
        }

        Path directoryPath = ((FSDirectory) directory).getDirectory();
        Path statsFile = directoryPath.resolve(statsFileName);

        writeAllVectorStats(statsFile, meanVector, varianceVector, stdDevVector, "Write-Time Stats");
    }

    /**
     * Writes vector statistics to a file.
     * @param outputFile
     * @param meanVector
     * @param varianceVector
     * @param stdDevVector
     * @param header
     * @throws IOException
     */
    private static void writeAllVectorStats(
        Path outputFile,
        float[] meanVector,
        float[] varianceVector,
        float[] stdDevVector,
        String header
    ) throws IOException {
        writeAllVectorStats(outputFile, meanVector, varianceVector, stdDevVector, header, -1);
    }

    /**
     * Writes vector statistics to a file for mean, standard deviation, and variance.
     * @param outputFile
     * @param meanVector
     * @param varianceVector
     * @param stdDevVector
     * @param header
     * @param count
     * @throws IOException
     */
    private static void writeAllVectorStats(
        Path outputFile,
        float[] meanVector,
        float[] varianceVector,
        float[] stdDevVector,
        String header,
        long count
    ) throws IOException {
        Files.createDirectories(outputFile.getParent());

        StringBuilder sb = new StringBuilder();
        sb.append("=== ").append(header).append(" @ ").append(System.currentTimeMillis()).append(" ===\n");
        if (count >= 0) {
            sb.append("count: ").append(count).append("\n");
        }

        // Write mean vector
        sb.append("mean vector: [");
        appendVector(sb, meanVector);
        sb.append("]\n");

        // Write variance vector
        sb.append("variance vector: [");
        appendVector(sb, varianceVector);
        sb.append("]\n");

        // Write standard deviation vector
        sb.append("standard deviation vector: [");
        appendVector(sb, stdDevVector);
        sb.append("]\n\n");

        Files.write(outputFile, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Helper method to append a vector to a StringBuilder.
     * @param sb
     * @param vector
     */
    private static void appendVector(StringBuilder sb, float[] vector) {
        for (int i = 0; i < vector.length; i++) {
            sb.append(vector[i]);
            if (i < vector.length - 1) {
                sb.append(", ");
            }
        }
    }

    /**
     * Writes read-time statistics for a segment to a file.
     * @param ctx
     * @param <T>
     */
    private static synchronized <T extends Computation> void writeReadTimeStats(VectorProfiler<T> ctx) {
        Collection<float[]> vectors = ctx.getAllVectors();
        if (vectors == null || vectors.isEmpty()) {
            System.out.println("No vectors available for statistics calculation");
            return;
        }

        String fileName = IndexFileNames.segmentFileName(ctx.segBaseName, ctx.segSuffix, "txt");
        Path outputFile = ctx.directoryPath.resolve(fileName);

        try {
            float[] meanVector = calculateVector(vectors, StatisticalOperators.MEAN);
            float[] varianceVector = calculateVector(vectors, StatisticalOperators.VARIANCE);
            float[] stdDevVector = calculateVector(vectors, StatisticalOperators.STANDARD_DEVIATION);

            writeAllVectorStats(outputFile, meanVector, varianceVector, stdDevVector, "Read-Time Stats", ctx.count);
        } catch (IOException ex) {
            System.err.println("Failed to write read-time profiler file for segment " + ctx.segBaseName + ": " + ex.getMessage());
        }
    }

    synchronized void addVectors(Collection<float[]> vectors) {
        if (vectors == null || vectors.isEmpty()) {
            return;
        }
        for (float[] vec : vectors) {
            allVectors.add(vec);
            for (int i = 0; i < sumPerDimension.length; i++) {
                sumPerDimension[i] = computation.apply(sumPerDimension[i], vec[i])[0];
            }
            count++;
        }
        System.out.println("Added " + vectors.size() + " vectors. Total vectors: " + allVectors.size());
    }

    synchronized Collection<float[]> getAllVectors() {
        System.out.println("Getting all vectors. Size: " + allVectors.size());
        return new ArrayList<>(allVectors);
    }

    synchronized float[] getVector() {
        if (count == 0) {
            System.out.println("No vectors available (count is 0)");
            return null;
        }
        System.out.println("Calculating vector statistics for " + count + " vectors");
        return computation.apply(sumPerDimension, count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VectorProfiler)) return false;
        VectorProfiler<?> that = (VectorProfiler<?>) o;
        return Objects.equals(segBaseName, that.segBaseName) && Objects.equals(segSuffix, that.segSuffix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segBaseName, segSuffix);
    }
}
