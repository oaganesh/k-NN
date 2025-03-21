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
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.index.KNNSettings;
import org.opensearch.knn.indices.ModelDao;
import org.opensearch.common.settings.Settings;
import org.opensearch.knn.index.KNNSettings;

import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Utility class for performing vector calculations and profiling operations
 * on collections of float arrays representing vectors.
 * @param <T>
 */
public class VectorProfiler<T extends Computation> {
    private static final ConcurrentHashMap<VectorProfiler<?>, VectorProfiler<?>> SEGMENT_CONTEXTS = new ConcurrentHashMap<>();
    private static final ThreadLocal<String> currentIndexName = new ThreadLocal<>();

    private final String segBaseName;
    private final String segSuffix;
    private final Path directoryPath;
    private final float[] sumPerDimension;
    private final T computation;
    private long count;
    private static boolean samplingEnabled = false;
    private static ModelDao.OpenSearchKNNModelDao modelDao;
    private static final KNNSettings knnSettings = KNNSettings.state();

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

    // public static boolean isSamplingEnabled() {
    // return samplingEnabled;
    // }
    //
    // // Add method to set sampling state
    // public static void setSamplingEnabled(boolean enabled) {
    // samplingEnabled = enabled;
    // }

    public static void setCurrentIndexName(String indexName) {
        currentIndexName.set(indexName);
    }

    public static void clearCurrentIndexName() {
        currentIndexName.remove();
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
        T computation,
        String indexName
    ) {
        // if (!samplingEnabled) {
        // return;
        // }
        Settings idxSettings = knnSettings.getIndexSettings(indexName);
        boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(idxSettings);

        if (!samplingEnabled) {
            return;
        }

        System.out.println("Sampling is enabled.");
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

    public static void saveVectorStats(SegmentWriteState segmentWriteState, Collection<float[]> vectors, String indexName)
        throws IOException {
        // String indexUUID = null;
        //
        // if(segmentWriteState.segmentInfo.getId() != null) {
        // //indexUUID = Arrays.toString(segmentWriteState.segmentInfo.getId());
        // indexUUID = segmentWriteState.segmentInfo.getId().toString();
        // }
        //
        // if(indexUUID == null || indexUUID.isEmpty()) {
        // System.out.println("VectorProfiler: No index UUID found");
        // return;
        // }
        //
        // ClusterService clusterService = KNNSettings.state().getClusterService();
        // if(clusterService == null) {
        // System.out.println("VectorProfiler: ClusterService is null");
        // return;
        // }
        //
        // IndexMetadata matchingMetadata = null;
        // for(IndexMetadata imd : clusterService.state().metadata())
        // if(imd.getIndexUUID().equals(indexUUID)) {
        // matchingMetadata = imd;
        // break;
        // }
        //
        // Settings idxSettings = matchingMetadata.getSettings();
        //
        // boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(idxSettings);
        // if(!samplingEnabled) {
        // System.out.println("VectorProfiler: index.knn.sampling is false or absent");
        // return;
        // }
        //
        // if (vectors == null || vectors.isEmpty()) {
        // System.out.println("VectorProfiler: Vectors collection is null or empty");
        // return;
        // }

        // if (indexName == null || indexName.isEmpty()) {
        // return;
        // }

        // Settings idxSettings = knnSettings.getIndexSettings(indexName);
        // boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(idxSettings);
        //
        // if(!samplingEnabled) {
        // System.out.println("VectorProfiler: index.knn.sampling is false or absent");
        // return;
        // }

        // ClusterService clusterService = KNNSettings.state().getClusterService();
        // if (clusterService == null) {
        // System.out.println("VectorProfiler: ClusterService is null");
        // return;
        // }

        // Get index metadata directly from cluster state
        // IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        // if (indexMetadata == null) {
        // System.out.println("VectorProfiler: No index metadata found for " + indexName);
        // return;
        // }

        // String indexName = segmentWriteState.segmentInfo.name;
        // String indexName = segmentWriteState.segmentInfo.name.split("_")[0]; // Extract index name from segment name
        // Settings indexSettings = KNNSettings.state().getIndexSettings(indexName);
        // boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(indexSettings);

        // boolean samplingEnabled = indexMetadata.getSettings().getAsBoolean(KNNSettings.KNN_SAMPLING, false);

        // String actualIndexName = extractIndexName(segmentWriteState.segmentInfo.name);
        // boolean samplingEnabled = Boolean.parseBoolean(KNNSettings.isSamplingEnabled(indexName));

        // String indexName = currentIndexName.get();

        // Settings idxSettings = knnSettings.getIndexSettings(indexName);
        // boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(idxSettings);

        // if (!KNNSettings.isSamplingEnabled(indexName)) {
        // System.out.println("Sampling is not enabled for index {}, skipping vector stats" + indexName);
        // return;
        // }
        // boolean samplingEnabled = KNNSettings.isSamplingEnabled(indexName);

        // boolean samplingEnabled = indexSettings.getAsBoolean(KNNSettings.KNN_SAMPLING, false);

        // boolean samplingEnabled = KNNSettings.isSamplingEnabled(indexSettings);

        // if (!samplingEnabled) {
        // System.out.println("Sampling is not enabled for index {}, skipping vector stats: " + indexName);
        // return;
        // }

        // Settings indexSettings = segmentWriteState.segmentInfo.info.getIndexSettings().getSettings();
        // boolean samplingEnabled = KNNSettings.isSamplingEnabled(indexSettings);

        // Settings idxSettings = knnSettings.getIndexSettings(indexName);
        // boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(idxSettings);

        if (indexName == null) {
            System.out.println("No index name set for current thread. Skipping vector stats.");
            return;
        }

        if (vectors == null || vectors.isEmpty()) {
            System.out.println("VectorProfiler: Vectors collection is null or empty");
            return;
        }

        System.out.println("Sampling enabled for index " + indexName + ": " + samplingEnabled);

        // if (!samplingEnabled) {
        // System.out.println("Sampling is not enabled for index " + indexName + ", skipping vector stats");
        // return;
        // }

        System.out.println("Processing vectors for index " + indexName + ", vector count: " + vectors.size());

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

        Settings idxSettings = knnSettings.getIndexSettings(indexName);
        boolean samplingEnabled = KNNSettings.IS_KNN_SAMPLE_SETTING.get(idxSettings);

        if (!samplingEnabled) {
            System.out.println("Sampling is not enabled for index {}, skipping vector stats: " + indexName);
            return;
        }

        writeAllVectorStats(statsFile, meanVector, varianceVector, stdDevVector, "Write-Time Stats");
    }

    private static void writeVectorStats(Path outputFile, float[] meanVector, String header) throws IOException {
        writeVectorStats(outputFile, meanVector, header, -1);
    }

    private static void writeAllVectorStats(
        Path outputFile,
        float[] meanVector,
        float[] varianceVector,
        float[] stdDevVector,
        String header
    ) throws IOException {
        writeAllVectorStats(outputFile, meanVector, varianceVector, stdDevVector, header, -1);
    }

    private static void writeVectorStats(Path outputFile, float[] meanVector, String header, long count) throws IOException {
        Files.createDirectories(outputFile.getParent());

        StringBuilder sb = new StringBuilder();
        sb.append("=== ").append(header).append(" @ ").append(System.currentTimeMillis()).append(" ===\n");
        if (count >= 0) {
            sb.append("count: ").append(count).append("\n");
        }

        sb.append("mean vector: [");
        for (int i = 0; i < meanVector.length; i++) {
            sb.append(meanVector[i]);
            if (i < meanVector.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]\n\n");

        Files.write(outputFile, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

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

    private static void appendVector(StringBuilder sb, float[] vector) {
        for (int i = 0; i < vector.length; i++) {
            sb.append(vector[i]);
            if (i < vector.length - 1) {
                sb.append(", ");
            }
        }
    }

    public static <T extends Computation> float[] getCurrentVector(String segBaseName, String segSuffix, T computation) {
        VectorProfiler<T> key = new VectorProfiler<>(segBaseName, segSuffix, null, 0, computation);
        @SuppressWarnings("unchecked")
        VectorProfiler<T> context = (VectorProfiler<T>) SEGMENT_CONTEXTS.get(key);
        return context != null ? context.getVector() : null;
    }

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
            // writeVectorStats(outputFile, vec, "Read-Time Stats", ctx.count);
        } catch (IOException ex) {
            System.err.println("Failed to write read-time profiler file for segment " + ctx.segBaseName + ": " + ex.getMessage());
        }
    }

    // private final List<float[]> allVectors = new ArrayList<>();

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

    // Get the knn-sampling setting from KNNSettings
    public static void initialize(ModelDao.OpenSearchKNNModelDao modelDaoInstance) {
        modelDao = modelDaoInstance;
        samplingEnabled = KNNSettings.state().getSettingValue(String.valueOf(KNNSettings.IS_KNN_SAMPLE_SETTING));
    }

    private static String extractIndexName(String segmentName) {
        // Segment names are often in the format: _0, _1, etc.
        // The actual index name is typically the part before the first underscore
        int underscoreIndex = segmentName.indexOf('_');
        if (underscoreIndex > 0) {
            return segmentName.substring(0, underscoreIndex);
        }
        // If there's no underscore, return the whole segment name
        return segmentName;
    }

    // private void initialize() {
    // initialize(
    // VectorProfiler.builder()
    // .isWeightLimited(KNNSettings.state().getSettingValue(KNNSettings.IS_KNN_SAMPLE_SETTING))
    // // Initially use cluster-level limit; will be updated later during cache refresh if node-specific limit exists
    // .maxWeight(KNNSettings.getClusterCbLimit().getKb())
    // .isExpirationLimited(KNNSettings.state().getSettingValue(KNNSettings.KNN_CACHE_ITEM_EXPIRY_ENABLED))
    // .expiryTimeInMin(
    // ((TimeValue) KNNSettings.state().getSettingValue(KNNSettings.KNN_CACHE_ITEM_EXPIRY_TIME_MINUTES)).getMinutes()
    // )
    // .build()
    // );
    // }
}
