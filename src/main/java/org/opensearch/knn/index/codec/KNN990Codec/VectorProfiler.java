/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;

import java.util.concurrent.ConcurrentHashMap;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
//import java.nio.file.Path;
import org.apache.lucene.store.FSDirectory;

import java.nio.file.Path;

import java.nio.file.StandardOpenOption;
import java.util.*;

//public class VectorProfiler {
//        /**
//         * Calculates and returns the mean vector for a collection of float vectors
//         *
//         * @param vectors Collection of float arrays to analyze
//         * @return mean vector as float array
//         */
//        public static float[] calculateMeanVector(Collection<float[]> vectors) {
//            if (vectors == null || vectors.isEmpty()) {
//                throw new IllegalArgumentException("Vectors collection cannot be null or empty");
//            }
//
//            float[] firstVector = vectors.iterator().next();
//            int dim = firstVector.length;
//            float[] meanVector = new float[dim];
//            int count = vectors.size();
//
//            Arrays.fill(meanVector, 0);
//
//            for (float[] vec : vectors) {
//                if (vec.length != dim) {
//                    throw new IllegalArgumentException("All vectors must have the same dimension");
//                }
//                for (int i = 0; i < dim; i++) {
//                    meanVector[i] += vec[i];
//                }
//            }
//
//            for (int i = 0; i < dim; i++) {
//                meanVector[i] /= count;
//            }
//
//            return meanVector;
//        }
//
////        public static float[] computeMeanAndLogSearch(List<LeafReaderContext> segmentContexts, SegmentWriteState segmentWriteState) throws IOException {
////            List<float[]> allVectors = new ArrayList<>();
////            List<float[]> searchedVectors = new ArrayList<>();
////
////            for(LeafReaderContext context : segmentContexts) {
////                KNNWeight knnWeight = new KNNWeight(null, 1.0f);
////                //KNNWeight.PerLeafResult result = knnWeight.searchLeaf(context, 10);
////                Map<Integer, Float> docIdToScoreMap = (Map<Integer, Float>) knnWeight.searchLeaf(context, 10);
////                //KNNWeight.PerLeafResult result = knnWeight.searchLeaf(context, 10);
////
////                for (Integer docId : docIdToScoreMap.keySet()) {
////                    searchedVectors.add(knnWeight.getVectorForDoc(context, docId));
////                    allVectors.add(knnWeight.getVectorForDoc(context, docId));
////                }
////            }
////            logSearchActivity("global_search", searchedVectors);
////            saveMeanVectorStats(segmentWriteState, calculateMeanVector(allVectors));
////
////            return calculateMeanVector(allVectors);
////        }
////
////        public static void logSearchActivity(String segmentName, List<float[]> searchedVectors) throws IOException {
////            if (searchedVectors.isEmpty()) {
////                return;
////            }
////
////            Path searchLogDir = Paths.get("search_logs");
////            Files.createDirectories(searchLogDir);
////
////            Path searchStatsFile = searchLogDir.resolve(segmentName + "_search_stats.txt");
////
////            StringBuilder sb = new StringBuilder();
////            sb.append("Timestamp: ").append(System.currentTimeMillis()).append("\n");
////            sb.append("Search Vectors (sampled):\n");
////            for (float[] vector : searchedVectors) {
////                sb.append(Arrays.toString(vector)).append("\n");
////            }
////
////            Files.write(searchStatsFile, sb.toString().getBytes(StandardCharsets.UTF_8),
////                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
////
////            System.out.println("Logged search activity for segment: " + segmentName);
////        }
//
//        /**
//         * Prints the mean vector statistics
//         *
//         * @param meanVector The calculated mean vector
//         */
//
////        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
////            String meanVectorFileName = IndexFileNames.segmentFileName(
////                    segmentWriteState.segmentInfo.name,
////                    segmentWriteState.segmentSuffix,
////                    "vmf"
////            );
////
////            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
////                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
////                output.writeInt(meanVector.length);
////                for (float value : meanVector) {
////                    output.writeInt(Float.floatToIntBits(value));
////                }
////
////                CodecUtil.writeFooter(output);
////                segmentWriteState.segmentInfo.addFile(meanVectorFileName);
////
////
////                System.out.println("Written file name: " + meanVectorFileName);
////                System.out.println("Written file length: " + output.getFilePointer());
////            }
////
////            try {
////                String[] files = segmentWriteState.directory.listAll();
////                System.out.println("Files in directory:");
////                for (String file : files) {
////                    System.out.println(" - " + file + " (size: " + segmentWriteState.directory.fileLength(file) + " bytes)");
////                }
////            } catch (IOException e) {
////                System.err.println("Error listing directory contents: " + e.getMessage());
////            }
////
////            // Add a file deletion listener
////            if (segmentWriteState.directory instanceof TrackingDirectoryWrapper) {
////                TrackingDirectoryWrapper trackingDir = (TrackingDirectoryWrapper) segmentWriteState.directory;
//////                trackingDir.setDeleteListener(new Directory.DeleteListener() {
//////                    @Override
//////                    public void deleteFile(String name) {
//////                        System.out.println("File being deleted: " + name);
//////                    }
//////                });
////            }
////        }
//
//
////        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
////            // Generate the filename
////            String meanVectorFileName = IndexFileNames.segmentFileName(
////                    segmentWriteState.segmentInfo.name,
////                    segmentWriteState.segmentSuffix,
////                    "vmf"
////            );
////
////            // Debug logging to verify the directory and filename
////            System.out.println("Directory: " + segmentWriteState.directory);
////            System.out.println("Attempting to write file: " + meanVectorFileName);
////
////            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
////                // Write header
////                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
////
////                // Write vector data
////                output.writeInt(meanVector.length);
////                for (float value : meanVector) {
////                    output.writeInt(Float.floatToIntBits(value));
////                }
////
////                // Write footer and flush
////                CodecUtil.writeFooter(output);
////
////                System.out.println("Mean vector stats written to: " + meanVectorFileName);
////            }
////
////            // Ensure proper syncing
////            Set<String> files = new HashSet<>();
////            files.add(meanVectorFileName);
////
////            try {
////                // Sync the specific file
////                segmentWriteState.directory.sync(files);
////
////                segmentWriteState.directory.syncMetaData();
////
////                System.out.println("Successfully synced file: " + meanVectorFileName);
////            } catch (IOException e) {
////                System.err.println("Error syncing file: " + e.getMessage());
////                throw e;
////            }
////        }
//
//    // Add this helper method to verify file existence
////    private static void verifyFileExists(SegmentWriteState state, String fileName) {
////        try {
////            if (state.directory.fileLength(fileName)) {
////                System.out.println("File verified: " + fileName);
////                System.out.println("File length: " + state.directory.fileLength(fileName));
////            } else {
////                System.out.println("File does not exist: " + fileName);
////            }
////        } catch (IOException e) {
////            System.err.println("Error verifying file: " + e.getMessage());
////        }
////    }
//
//
////        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
////            String meanVectorFileName = IndexFileNames.segmentFileName(
////                    segmentWriteState.segmentInfo.name,
////                    segmentWriteState.segmentSuffix,
////                    "vmf"
////            );
////            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
////                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
////                output.writeInt(meanVector.length);
////                for (float value : meanVector) {
////                    output.writeInt(Float.floatToIntBits(value));
////                }
////               // output.writeInt(meanVector.length);
////                CodecUtil.writeFooter(output);
////
////                System.out.println("Mean vector stats saved to: " + meanVectorFileName);
////
////                flushSegment(segmentWriteState, meanVectorFileName);
////            }
////        }
//
//        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
//            String meanVectorFileName = IndexFileNames.segmentFileName(
//                    segmentWriteState.segmentInfo.name,
//                    segmentWriteState.segmentSuffix,
//                    "txt"
//            );
//
//            //Path meanStatsFile = segmentWriteState.directory.toPath().resolve(meanVectorFileName);
//            Directory directory = segmentWriteState.directory;
//            while(directory instanceof FilterDirectory) {
//                directory = ((FilterDirectory) directory).getDelegate();
//            }
//
//            if(!(directory instanceof FSDirectory)) {
//                throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
//            }
//
//            Path directoryPath = ((FSDirectory) directory).getDirectory();
//            Path meanStatsFile = directoryPath.resolve(meanVectorFileName);
//
//            Files.createDirectories(meanStatsFile.getParent());
//
//            StringBuilder sb = new StringBuilder();
//            sb.append("Mean Vector Stats:\n");
//            for (float value : meanVector) {
//                sb.append(value).append(" ");
//            }
//            Files.write(meanStatsFile, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
//            System.out.println("Mean vector stats saved to" + meanStatsFile);
//            }
//
//
////        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
////            String meanVectorFileName = IndexFileNames.segmentFileName(
////                    segmentWriteState.segmentInfo.name,
////                    segmentWriteState.segmentSuffix,
////                    "json"
////            );
////
////            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
////                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
////
////                StringBuilder jsonOutput = new StringBuilder();
////                jsonOutput.append("{\n  \"mean_vector\": [");
////                for (int i = 0; i < meanVector.length; i++) {
////                    jsonOutput.append(meanVector[i]);
////                    if (i < meanVector.length - 1) {
////                        jsonOutput.append(", ");
////                    }
////                }
////
////                jsonOutput.append("]\n}");
////                byte[] jsonBytes = jsonOutput.toString().getBytes(StandardCharsets.UTF_8);
////                output.writeBytes(jsonBytes, jsonBytes.length);
////                // output.writeInt(meanVector.length);
////
////                CodecUtil.writeFooter(output);
////                System.out.println("Mean vector stats saved to: " + meanVectorFileName);
////
////            }
////
////                flushSegment(segmentWriteState, meanVectorFileName);
////
////        }
//
//        private static void flushSegment(SegmentWriteState segmentWriteState, String meanVectorFileName) {
//                try {
//                    segmentWriteState.directory.sync(Collections.singleton(meanVectorFileName));
//                    System.out.println("Segment sync completed: " + meanVectorFileName);
//                } catch (IOException e) {
//                    System.err.println("Failed to sync segment: " + meanVectorFileName + " - " + e.getMessage());
//                }
//        }
//
//
////        public static void printMeanVectorStats(float[] meanVector) {
////            System.out.println("Per-dimension mean: " + Arrays.toString(meanVector));
////        }
//
////        public static void saveMeanVectorStats(float[] meanVector) {
////            try {
////                Path indicesDir = Paths.get("build/testclusters/integTest-0/data/nodes/0/indices");
////                if (!Files.exists(indicesDir)) {
////                    throw new IOException("Indices directory not found: " + indicesDir.toString());
////                }
////
////                Optional<Path> latestIndexOptional = Files.list(indicesDir)
////                        .filter(Files::isDirectory)
////                        .max(Comparator.comparingLong(path -> path.toFile().lastModified()));
////
////                if (latestIndexOptional.isEmpty()) {
////                    throw new IOException("No directories found in indices directory: " + indicesDir.toString());
////                }
////
////                Path latestIndexDir = latestIndexOptional.get();
////                Path outputPath = latestIndexDir.resolve("0/index/mean_output.bin");
////                Files.createDirectories(outputPath.getParent());
////
////                try (FSDirectory directory = FSDirectory.open(outputPath.getParent());
////                     IndexOutput output = directory.createOutput(outputPath.getFileName().toString(), null)) {
////                    CodecUtil.writeHeader(output, "MEAN_VECTOR_STATS", 1);
////
////                    output.writeInt(meanVector.length);
////                    ;
////
////                    for (float value : meanVector) {
////                        output.writeInt(Float.floatToIntBits(value));
////                    }
////
////                    output.writeInt(-1);
////
////                    System.out.println("Mean vector stats saved to: " + outputPath.toString());
////                }
////
////            } catch (IOException e) {
////                e.printStackTrace();
////            }
////        }
//            //Path outputPath = Paths.get("build/testclusters/integTest-0/data/nodes/0/indices/0/index/mean_output.json");
////            try {
////                Files.createDirectories(outputPath.getParent());
////                try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
////                    out.writeUTF("VECTOR_STATS");
////                    out.writeInt(meanVector.length);
////                    for (float value : meanVector) {
////                        out.writeFloat(value);
////                    }
////                    out.writeInt(-1);
////                }
////            } catch (IOException e) {
////                e.printStackTrace();
////            }
//
//}

//public class VectorProfiler {
//    private static final ConcurrentHashMap<VectorProfiler, VectorProfiler> SEGMENT_CONTEXTS = new ConcurrentHashMap<>();
//
//    // SegmentKey fields
//    private final String segBaseName;
//    private final String segSuffix;
//
//    // SegmentContext fields
//    private final Path directoryPath;
//
//    // SegmentStats fields
//    private final float[] sumPerDimension;
//    private long count;
//
//    public VectorProfiler(String segBaseName, String segSuffix, Path directoryPath, int dimension) {
//        this.segBaseName = segBaseName;
//        this.segSuffix = segSuffix;
//        this.directoryPath = directoryPath;
//        this.sumPerDimension = new float[dimension];
//        this.count = 0;
//    }
//
//    public static void recordReadTimeVectors(String segBaseName,
//                                             String segSuffix,
//                                             Path directoryPath,
//                                             Collection<float[]> vectors) {
//        if (vectors == null || vectors.isEmpty()) {
//            return;
//        }
//
//        int dim = vectors.iterator().next().length;
//        VectorProfiler key = new VectorProfiler(segBaseName, segSuffix, directoryPath, dim);
//
//        VectorProfiler context = SEGMENT_CONTEXTS.computeIfAbsent(key,
//                k -> new VectorProfiler(segBaseName, segSuffix, directoryPath, dim));
//
//        context.addVectors(vectors);
//        writeReadTimeStats(context);
//    }
//
//    public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
//        String meanVectorFileName = IndexFileNames.segmentFileName(
//                segmentWriteState.segmentInfo.name,
//                segmentWriteState.segmentSuffix,
//                "txt"
//        );
//
//        Directory directory = segmentWriteState.directory;
//        while(directory instanceof FilterDirectory) {
//            directory = ((FilterDirectory) directory).getDelegate();
//        }
//
//        if(!(directory instanceof FSDirectory)) {
//            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
//        }
//
//        Path directoryPath = ((FSDirectory) directory).getDirectory();
//        Path statsFile = directoryPath.resolve(meanVectorFileName);
//
//        writeVectorStats(statsFile, meanVector, "Write-Time Stats");
//    }
//
//    public static float[] calculateMeanVector(Collection<float[]> vectors) {
//        if (vectors == null || vectors.isEmpty()) {
//            throw new IllegalArgumentException("Vectors collection cannot be null or empty");
//        }
//
//        float[] firstVector = vectors.iterator().next();
//        int dim = firstVector.length;
//        float[] meanVector = new float[dim];
//        int count = vectors.size();
//
//        Arrays.fill(meanVector, 0);
//
//        for (float[] vec : vectors) {
//            if (vec.length != dim) {
//                throw new IllegalArgumentException("All vectors must have same dimension");
//            }
//            for (int i = 0; i < dim; i++) {
//                meanVector[i] += vec[i];
//            }
//        }
//
//        for (int i = 0; i < dim; i++) {
//            meanVector[i] /= count;
//        }
//
//        return meanVector;
//    }
//
//    public static float[] getCurrentMeanVector(String segBaseName, String segSuffix) {
//        VectorProfiler key = new VectorProfiler(segBaseName, segSuffix, null, 0);
//        VectorProfiler context = SEGMENT_CONTEXTS.get(key);
//        return context != null ? context.getMeanVector() : null;
//    }
//
//    private static synchronized void writeReadTimeStats(VectorProfiler ctx) {
//        float[] meanVec = ctx.getMeanVector();
//        if (meanVec == null) {
//            return;
//        }
//
//        String fileName = IndexFileNames.segmentFileName(
//                ctx.segBaseName,
//                ctx.segSuffix,
//                "txt"
//        );
//        Path outputFile = ctx.directoryPath.resolve(fileName);
//
//        try {
//            writeVectorStats(outputFile, meanVec, "Read-Time Stats", ctx.count);
//        } catch (IOException ex) {
//            System.err.println("Failed to write read-time profiler file for segment "
//                    + ctx.segBaseName + ": " + ex.getMessage());
//        }
//    }
//
//    private static void writeVectorStats(Path outputFile, float[] meanVector, String header) throws IOException {
//        writeVectorStats(outputFile, meanVector, header, -1);
//    }
//
//    private static void writeVectorStats(Path outputFile, float[] meanVector, String header, long count) throws IOException {
//        Files.createDirectories(outputFile.getParent());
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("=== ").append(header).append(" @ ").append(System.currentTimeMillis()).append(" ===\n");
//        if (count >= 0) {
//            sb.append("count: ").append(count).append("\n");
//        }
//
//        sb.append("mean vector: [");
//        for (int i = 0; i < meanVector.length; i++) {
//            sb.append(meanVector[i]);
//            if (i < meanVector.length - 1) {
//                sb.append(", ");
//            }
//        }
//        sb.append("]\n\n");
//
//        Files.write(
//                outputFile,
//                sb.toString().getBytes(StandardCharsets.UTF_8),
//                StandardOpenOption.CREATE,
//                StandardOpenOption.APPEND
//        );
//    }
//
//    synchronized void addVectors(Collection<float[]> vectors) {
//        if (vectors == null || vectors.isEmpty()) {
//            return;
//        }
//        for (float[] vec : vectors) {
//            for (int i = 0; i < sumPerDimension.length; i++) {
//                sumPerDimension[i] += vec[i];
//            }
//            count++;
//        }
//    }
//
//    synchronized float[] getMeanVector() {
//        if (count == 0) {
//            return null;
//        }
//        float[] mean = new float[sumPerDimension.length];
//        for (int i = 0; i < sumPerDimension.length; i++) {
//            mean[i] = sumPerDimension[i] / count;
//        }
//        return mean;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (!(o instanceof VectorProfiler)) return false;
//        VectorProfiler that = (VectorProfiler) o;
//        return Objects.equals(segBaseName, that.segBaseName) &&
//                Objects.equals(segSuffix, that.segSuffix);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(segBaseName, segSuffix);
//    }
//}

//public class VectorProfiler<T extends VectorProfiler.VectorStatistic> {
//    private static final ConcurrentHashMap<SegmentKey, SegmentContext<T>> SEGMENT_CONTEXTS = new ConcurrentHashMap<>();
//
//    private final StatisticCalculator<T> calculator;
//
//    public VectorProfiler(StatisticCalculator<T> calculator) {
//        this.calculator = calculator;
//    }
//
//    // Interface for different statistical measures
//    public interface VectorStatistic {
//        float[] getValues();
//        String getDescription();
//    }
//
//    // Interface for calculating statistics
//    public interface StatisticCalculator<T extends VectorStatistic> {
//        T calculate(Collection<float[]> vectors);
//    }
//
//    /**
//     * Records vectors during read-time and writes statistics
//     */
//    public static void recordReadTimeVectors(String segBaseName,
//                                             String segSuffix,
//                                             Path directoryPath,
//                                             Collection<float[]> vectors) {
//        if (vectors == null || vectors.isEmpty()) {
//            return;
//        }
//
//        int dim = vectors.iterator().next().length;
//        SegmentKey key = new SegmentKey(segBaseName, segSuffix);
//
//        SegmentContext context = SEGMENT_CONTEXTS.computeIfAbsent(key,
//                k -> new SegmentContext(segBaseName, segSuffix, directoryPath, dim));
//
//        context.stats.addVectors(vectors);
//        writeReadTimeStats(context, key);
//    }
//
//    // Implementation for mean vector statistics
//    public static class MeanVectorStatistic implements VectorStatistic {
//        private final float[] meanVector;
//
//        public MeanVectorStatistic(float[] meanVector) {
//            this.meanVector = meanVector;
//        }
//
//        @Override
//        public float[] getValues() {
//            return meanVector;
//        }
//
//        @Override
//        public String getDescription() {
//            return "Mean Vector";
//        }
//    }
//
//    // Implementation for variance vector statistics
//    public static class VarianceVectorStatistic implements VectorStatistic {
//        private final float[] varianceVector;
//
//        public VarianceVectorStatistic(float[] varianceVector) {
//            this.varianceVector = varianceVector;
//        }
//
//        @Override
//        public float[] getValues() {
//            return varianceVector;
//        }
//
//        @Override
//        public String getDescription() {
//            return "Variance Vector";
//        }
//    }
//    private static synchronized void writeReadTimeStats(SegmentContext ctx, SegmentKey key) {
//        float[] meanVec = ctx.stats.getMeanVector();
//        if (meanVec == null) {
//            return;
//        }
//
//        String fileName = IndexFileNames.segmentFileName(
//                key.segBaseName,
//                key.segSuffix,
//                "txt"
//        );
//        Path outputFile = ctx.directoryPath.resolve(fileName);
//
//        try {
//            writeVectorStats(outputFile, meanVec, "Read-Time Stats", ctx.stats.count);
//        } catch (IOException ex) {
//            System.err.println("Failed to write read-time profiler file for segment "
//                    + key.segBaseName + ": " + ex.getMessage());
//        }
//    }
//
////    public void recordReadTimeVectors(String segBaseName,
////                                      String segSuffix,
////                                      Path directoryPath,
////                                      Collection<float[]> vectors) {
////        if (vectors == null || vectors.isEmpty()) {
////            return;
////        }
////
////        int dim = vectors.iterator().next().length;
////        SegmentKey key = new SegmentKey(segBaseName, segSuffix);
////
////        SegmentContext<T> context = SEGMENT_CONTEXTS.computeIfAbsent(key,
////                k -> new SegmentContext<>(segBaseName, segSuffix, directoryPath, dim, calculator));
////
////        context.addVectors(vectors);
////        writeReadTimeStats(context, key);
////    }
//
//    public void saveMeanVectorStats(SegmentWriteState segmentWriteState, Collection<float[]> vectors) throws IOException {
//        T statistic = calculator.calculate(vectors);
//
//        String statsFileName = IndexFileNames.segmentFileName(
//                segmentWriteState.segmentInfo.name,
//                segmentWriteState.segmentSuffix,
//                "txt"
//        );
//
//        Directory directory = segmentWriteState.directory;
//        while(directory instanceof FilterDirectory) {
//            directory = ((FilterDirectory) directory).getDelegate();
//        }
//
//        if(!(directory instanceof FSDirectory)) {
//            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
//        }
//
//        Path directoryPath = ((FSDirectory) directory).getDirectory();
//        Path statsFile = directoryPath.resolve(statsFileName);
//
//        writeVectorStats(statsFile, statistic, "Write-Time Stats");
//    }
//
//    private void writeVectorStats(Path outputFile, T statistic, String header) throws IOException {
//        Files.createDirectories(outputFile.getParent());
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("=== ").append(header).append(" @ ").append(System.currentTimeMillis()).append(" ===\n");
//        sb.append(statistic.getDescription()).append(": [");
//
//        float[] values = statistic.getValues();
//        for (int i = 0; i < values.length; i++) {
//            sb.append(values[i]);
//            if (i < values.length - 1) {
//                sb.append(", ");
//            }
//        }
//        sb.append("]\n\n");
//
//        Files.write(
//                outputFile,
//                sb.toString().getBytes(StandardCharsets.UTF_8),
//                StandardOpenOption.CREATE,
//                StandardOpenOption.APPEND
//        );
//    }
//
//    private static class SegmentContext<T extends VectorStatistic> {
//        final String segBaseName;
//        final String segSuffix;
//        final Path directoryPath;
//        final StatisticCalculator<T> calculator;
//        final List<float[]> vectors;
//
//        SegmentContext(String segBaseName, String segSuffix, Path directoryPath,
//                       int dimension, StatisticCalculator<T> calculator) {
//            this.segBaseName = segBaseName;
//            this.segSuffix = segSuffix;
//            this.directoryPath = directoryPath;
//            this.calculator = calculator;
//            this.vectors = new ArrayList<>();
//
//        }
//
//        synchronized void addVectors(Collection<float[]> newVectors) {
//            vectors.addAll(newVectors);
//        }
//
//        synchronized T getStatistic() {
//            return calculator.calculate(vectors);
//        }
//    }
//
//    // Example calculator implementations
//    public static class MeanVectorCalculator implements StatisticCalculator<MeanVectorStatistic> {
//        @Override
//        public MeanVectorStatistic calculate(Collection<float[]> vectors) {
//            // Implementation of mean calculation
//            float[] meanVector = calculateMean(vectors);
//            return new MeanVectorStatistic(meanVector);
//        }
//
//        private float[] calculateMean(Collection<float[]> vectors) {
//            // Your existing mean calculation logic here
//            // ...
//            return new float[0]; // Placeholder
//        }
//    }
//
//    public static class VarianceVectorCalculator implements StatisticCalculator<VarianceVectorStatistic> {
//        @Override
//        public VarianceVectorStatistic calculate(Collection<float[]> vectors) {
//            // Implementation of variance calculation
//            float[] varianceVector = calculateVariance(vectors);
//            return new VarianceVectorStatistic(varianceVector);
//        }
//
//        private float[] calculateVariance(Collection<float[]> vectors) {
//            // Your variance calculation logic here
//            // ...
//            return new float[0]; // Placeholder
//        }
//    }
//
//    private static class SegmentKey {
//        final String segBaseName;
//        final String segSuffix;
//
//        SegmentKey(String segBaseName, String segSuffix) {
//            this.segBaseName = segBaseName;
//            this.segSuffix = segSuffix;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (!(o instanceof SegmentKey)) return false;
//            SegmentKey that = (SegmentKey) o;
//            return Objects.equals(segBaseName, that.segBaseName) &&
//                    Objects.equals(segSuffix, that.segSuffix);
//        }
//
//        @Override
//        public int hashCode() {
//            return Objects.hash(segBaseName, segSuffix);
//        }
//    }
//
//    // Other supporting classes (SegmentKey, etc.) remain the same
//    // ...
//}

//public class VectorProfiler {
//    private static final ConcurrentHashMap<VectorProfiler, VectorProfiler> SEGMENT_CONTEXTS = new ConcurrentHashMap<>();
//
//    private final String segBaseName;
//    private final String segSuffix;
//    private final Path directoryPath;
//
//    // Extended statistics fields
//    private final float[] sumPerDimension;
//    private final float[] sumSquaredPerDimension; // For variance calculation
//    private final float[] minPerDimension;
//    private final float[] maxPerDimension;
//    private long count;
//
//    public static class VectorStats {
//        public final float[] mean;
//        public final float[] variance;
//        public final float[] standardDeviation;
//        public final float[] min;
//        public final float[] max;
//        public final float[] median;
//        public final float[] skewness;
//        public final float[] kurtosis;
//        public final long count;
//
//        public VectorStats(float[] mean, float[] variance, float[] standardDeviation,
//                           float[] min, float[] max, float[] median,
//                           float[] skewness, float[] kurtosis, long count) {
//            this.mean = mean;
//            this.variance = variance;
//            this.standardDeviation = standardDeviation;
//            this.min = min;
//            this.max = max;
//            this.median = median;
//            this.skewness = skewness;
//            this.kurtosis = kurtosis;
//            this.count = count;
//        }
//    }
//
//    public VectorProfiler(String segBaseName, String segSuffix, Path directoryPath, int dimension) {
//        this.segBaseName = segBaseName;
//        this.segSuffix = segSuffix;
//        this.directoryPath = directoryPath;
//        this.sumPerDimension = new float[dimension];
//        this.sumSquaredPerDimension = new float[dimension];
//        this.minPerDimension = new float[dimension];
//        this.maxPerDimension = new float[dimension];
//        Arrays.fill(this.minPerDimension, Float.POSITIVE_INFINITY);
//        Arrays.fill(this.maxPerDimension, Float.NEGATIVE_INFINITY);
//        this.count = 0;
//    }
//
//    synchronized void addVectors(Collection<float[]> vectors) {
//        if (vectors == null || vectors.isEmpty()) {
//            return;
//        }
//
//        for (float[] vec : vectors) {
//            for (int i = 0; i < sumPerDimension.length; i++) {
//                float value = vec[i];
//                sumPerDimension[i] += value;
//                sumSquaredPerDimension[i] += value * value;
//                minPerDimension[i] = Math.min(minPerDimension[i], value);
//                maxPerDimension[i] = Math.max(maxPerDimension[i], value);
//            }
//            count++;
//        }
//    }
//
//    synchronized VectorStats getVectorStats() {
//        if (count == 0) {
//            return null;
//        }
//
//        int dim = sumPerDimension.length;
//        float[] mean = new float[dim];
//        float[] variance = new float[dim];
//        float[] standardDeviation = new float[dim];
//        float[] median = new float[dim];
//        float[] skewness = new float[dim];
//        float[] kurtosis = new float[dim];
//
//        // Calculate mean and variance
//        for (int i = 0; i < dim; i++) {
//            mean[i] = sumPerDimension[i] / count;
//            variance[i] = (sumSquaredPerDimension[i] / count) - (mean[i] * mean[i]);
//            standardDeviation[i] = (float) Math.sqrt(variance[i]);
//        }
//
//        // For median, skewness, and kurtosis, we need to store all values temporarily
//        float[][] allValues = new float[dim][(int) count];
//        int[] currentIndex = new int[dim];
//
//        // Collect all values per dimension
//        for (float[] vec : getAllVectors()) {
//            for (int i = 0; i < dim; i++) {
//                allValues[i][currentIndex[i]++] = vec[i];
//            }
//        }
//
//        // Calculate median, skewness, and kurtosis
//        for (int i = 0; i < dim; i++) {
//            Arrays.sort(allValues[i]);
//            median[i] = calculateMedian(allValues[i]);
//            skewness[i] = calculateSkewness(allValues[i], mean[i], standardDeviation[i]);
//            kurtosis[i] = calculateKurtosis(allValues[i], mean[i], standardDeviation[i]);
//        }
//
//        return new VectorStats(mean, variance, standardDeviation,
//                minPerDimension, maxPerDimension, median,
//                skewness, kurtosis, count);
//    }
//
//    private static float calculateMedian(float[] sorted) {
//        if (sorted.length % 2 == 0) {
//            return (sorted[sorted.length/2 - 1] + sorted[sorted.length/2]) / 2.0f;
//        } else {
//            return sorted[sorted.length/2];
//        }
//    }
//
//    private static float calculateSkewness(float[] values, float mean, float stdDev) {
//        if (stdDev == 0) return 0;
//        float sum = 0;
//        for (float value : values) {
//            sum += Math.pow((value - mean) / stdDev, 3);
//        }
//        return sum / values.length;
//    }
//
//    private static float calculateKurtosis(float[] values, float mean, float stdDev) {
//        if (stdDev == 0) return 0;
//        float sum = 0;
//        for (float value : values) {
//            sum += Math.pow((value - mean) / stdDev, 4);
//        }
//        return sum / values.length - 3; // Excess kurtosis
//    }
//
//    private static void writeVectorStats(Path outputFile, VectorStats stats, String header) throws IOException {
//        Files.createDirectories(outputFile.getParent());
//
//        StringBuilder sb = new StringBuilder();
//        sb.append("=== ").append(header).append(" @ ").append(System.currentTimeMillis()).append(" ===\n");
//        sb.append("count: ").append(stats.count).append("\n");
//
//        appendVector(sb, "mean", stats.mean);
//        appendVector(sb, "variance", stats.variance);
//        appendVector(sb, "standard deviation", stats.standardDeviation);
//        appendVector(sb, "minimum", stats.min);
//        appendVector(sb, "maximum", stats.max);
//        appendVector(sb, "median", stats.median);
//        appendVector(sb, "skewness", stats.skewness);
//        appendVector(sb, "kurtosis", stats.kurtosis);
//        sb.append("\n");
//
//        Files.write(
//                outputFile,
//                sb.toString().getBytes(StandardCharsets.UTF_8),
//                StandardOpenOption.CREATE,
//                StandardOpenOption.APPEND
//        );
//    }
//
//    private static void appendVector(StringBuilder sb, String name, float[] vector) {
//        sb.append(name).append(": [");
//        for (int i = 0; i < vector.length; i++) {
//            sb.append(String.format("%.6f", vector[i]));
//            if (i < vector.length - 1) {
//                sb.append(", ");
//            }
//        }
//        sb.append("]\n");
//    }
//
//    // Modified write method to include all statistics
//    private static synchronized void writeReadTimeStats(VectorProfiler ctx) {
//        VectorStats stats = ctx.getVectorStats();
//        if (stats == null) {
//            return;
//        }
//
//        String fileName = IndexFileNames.segmentFileName(
//                ctx.segBaseName,
//                ctx.segSuffix,
//                "txt"
//        );
//        Path outputFile = ctx.directoryPath.resolve(fileName);
//
//        try {
//            writeVectorStats(outputFile, stats, "Read-Time Stats");
//        } catch (IOException ex) {
//            System.err.println("Failed to write read-time profiler file for segment "
//                    + ctx.segBaseName + ": " + ex.getMessage());
//        }
//    }
//
//    // Helper method to get all vectors (implement based on your storage mechanism)
//    private Collection<float[]> getAllVectors() {
//        // Implement this based on how you store the vectors
//        return new ArrayList<>();
//    }
//
//    // Rest of the class remains the same...
//}


public class VectorProfiler<T extends Computation> {
    private static final ConcurrentHashMap<VectorProfiler<?>, VectorProfiler<?>> SEGMENT_CONTEXTS = new ConcurrentHashMap<>();

    private final String segBaseName;
    private final String segSuffix;
    private final Path directoryPath;
    private final float[] sumPerDimension;
    private final T computation;
    private long count;

    public VectorProfiler(String segBaseName, String segSuffix, Path directoryPath, int dimension, T computation) {
        this.segBaseName = segBaseName;
        this.segSuffix = segSuffix;
        this.directoryPath = directoryPath;
        this.sumPerDimension = new float[dimension];
        this.computation = computation;
        this.count = 0;
    }

    public static <T extends Computation> void recordReadTimeVectors(String segBaseName,
                                                                     String segSuffix,
                                                                     Path directoryPath,
                                                                     Collection<float[]> vectors,
                                                                     T computation) {
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
            VectorProfiler<T> context = (VectorProfiler<T>) SEGMENT_CONTEXTS.computeIfAbsent(key,
                    k -> new VectorProfiler<>(segBaseName, segSuffix, directoryPath, dim, computation));

            System.out.println("Adding vectors to context");
            context.addVectors(vectors);

            System.out.println("Writing read-time stats");
            writeReadTimeStats(context);

        } catch (Exception e) {
            System.err.println("Error in recordReadTimeVectors: " + e.getMessage());
            e.printStackTrace();
        }
    }

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

    public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
        String meanVectorFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                "txt"
        );

        Directory directory = segmentWriteState.directory;
        while(directory instanceof FilterDirectory) {
            directory = ((FilterDirectory) directory).getDelegate();
        }

        if(!(directory instanceof FSDirectory)) {
            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
        }

        Path directoryPath = ((FSDirectory) directory).getDirectory();
        Path statsFile = directoryPath.resolve(meanVectorFileName);

        writeVectorStats(statsFile, meanVector, "Write-Time Stats");
    }

    public static void saveVectorStats(SegmentWriteState segmentWriteState, Collection<float[]> vectors) throws IOException {
        // Calculate all statistics
        float[] meanVector = calculateVector(vectors, StatisticalOperators.MEAN);
        float[] varianceVector = calculateVector(vectors, StatisticalOperators.VARIANCE);
        float[] stdDevVector = calculateVector(vectors, StatisticalOperators.STANDARD_DEVIATION);

        String statsFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                "txt"
        );

        Directory directory = segmentWriteState.directory;
        while(directory instanceof FilterDirectory) {
            directory = ((FilterDirectory) directory).getDelegate();
        }

        if(!(directory instanceof FSDirectory)) {
            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
        }

        Path directoryPath = ((FSDirectory) directory).getDirectory();
        Path statsFile = directoryPath.resolve(statsFileName);

        writeAllVectorStats(statsFile, meanVector, varianceVector, stdDevVector, "Write-Time Stats");
    }


    private static void writeVectorStats(Path outputFile, float[] meanVector, String header) throws IOException {
        writeVectorStats(outputFile, meanVector, header, -1);
    }

    private static void writeAllVectorStats(Path outputFile, float[] meanVector, float[] varianceVector,
                                            float[] stdDevVector, String header) throws IOException {
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

        Files.write(
                outputFile,
                sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    private static void writeAllVectorStats(Path outputFile, float[] meanVector, float[] varianceVector,
                                            float[] stdDevVector, String header, long count) throws IOException {
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

        Files.write(
                outputFile,
                sb.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
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

    private static synchronized  <T extends Computation> void writeReadTimeStats(VectorProfiler<T> ctx) {
        Collection<float[]> vectors = ctx.getAllVectors();
        if (vectors == null || vectors.isEmpty()) {
            System.out.println("No vectors available for statistics calculation");
            return;
        }

        String fileName = IndexFileNames.segmentFileName(
                ctx.segBaseName,
                ctx.segSuffix,
                "txt"
        );
        Path outputFile = ctx.directoryPath.resolve(fileName);

        try {
            float[] meanVector = calculateVector(vectors, StatisticalOperators.MEAN);
            float[] varianceVector = calculateVector(vectors, StatisticalOperators.VARIANCE);
            float[] stdDevVector = calculateVector(vectors, StatisticalOperators.STANDARD_DEVIATION);

            writeAllVectorStats(outputFile, meanVector, varianceVector, stdDevVector, "Read-Time Stats", ctx.count);
            //writeVectorStats(outputFile, vec, "Read-Time Stats", ctx.count);
        } catch (IOException ex) {
            System.err.println("Failed to write read-time profiler file for segment "
                    + ctx.segBaseName + ": " + ex.getMessage());
        }
    }

    private final List<float[]> allVectors = new ArrayList<>();

    // ... (rest of the static methods remain the same)

    synchronized void addVectors(Collection<float[]> vectors) {
        if (vectors == null || vectors.isEmpty()) {
            return;
        }
        for (float[] vec : vectors) {
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
        return Objects.equals(segBaseName, that.segBaseName) &&
                Objects.equals(segSuffix, that.segSuffix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segBaseName, segSuffix);
    }
}