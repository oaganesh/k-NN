/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.*;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.knn.index.query.KNNWeight;
import org.apache.lucene.index.LeafReaderContext;


import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
//import java.nio.file.Path;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.knn.index.query.KNNWeight;

import java.nio.file.Path;

import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.nio.file.Paths;

public class VectorProfiler {
        /**
         * Calculates and returns the mean vector for a collection of float vectors
         *
         * @param vectors Collection of float arrays to analyze
         * @return mean vector as float array
         */
        public static float[] calculateMeanVector(Collection<float[]> vectors) {
            if (vectors == null || vectors.isEmpty()) {
                throw new IllegalArgumentException("Vectors collection cannot be null or empty");
            }

            float[] firstVector = vectors.iterator().next();
            int dim = firstVector.length;
            float[] meanVector = new float[dim];
            int count = vectors.size();

            Arrays.fill(meanVector, 0);

            for (float[] vec : vectors) {
                if (vec.length != dim) {
                    throw new IllegalArgumentException("All vectors must have the same dimension");
                }
                for (int i = 0; i < dim; i++) {
                    meanVector[i] += vec[i];
                }
            }

            for (int i = 0; i < dim; i++) {
                meanVector[i] /= count;
            }

            return meanVector;
        }

        public static float[] computeMeanAndLogSearch(List<LeafReaderContext> segmentContexts, SegmentWriteState segmentWriteState) throws IOException {
            List<float[]> allVectors = new ArrayList<>();
            List<float[]> searchedVectors = new ArrayList<>();

            for(LeafReaderContext context : segmentContexts) {
                KNNWeight knnWeight = new KNNWeight(null, 1.0f);
                //KNNWeight.PerLeafResult result = knnWeight.searchLeaf(context, 10);
                Map<Integer, Float> docIdToScoreMap = (Map<Integer, Float>) knnWeight.searchLeaf(context, 10);
                //KNNWeight.PerLeafResult result = knnWeight.searchLeaf(context, 10);

                for (Integer docId : docIdToScoreMap.keySet()) {
                    searchedVectors.add(knnWeight.getVectorForDoc(context, docId));
                    allVectors.add(knnWeight.getVectorForDoc(context, docId));
                }
            }
            logSearchActivity("global_search", searchedVectors);
            saveMeanVectorStats(segmentWriteState, calculateMeanVector(allVectors));

            return calculateMeanVector(allVectors);
        }

        public static void logSearchActivity(String segmentName, List<float[]> searchedVectors) throws IOException {
            if (searchedVectors.isEmpty()) {
                return;
            }

            Path searchLogDir = Paths.get("search_logs");
            Files.createDirectories(searchLogDir);

            Path searchStatsFile = searchLogDir.resolve(segmentName + "_search_stats.txt");

            StringBuilder sb = new StringBuilder();
            sb.append("Timestamp: ").append(System.currentTimeMillis()).append("\n");
            sb.append("Search Vectors (sampled):\n");
            for (float[] vector : searchedVectors) {
                sb.append(Arrays.toString(vector)).append("\n");
            }

            Files.write(searchStatsFile, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            System.out.println("Logged search activity for segment: " + segmentName);
        }

        /**
         * Prints the mean vector statistics
         *
         * @param meanVector The calculated mean vector
         */

//        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
//            String meanVectorFileName = IndexFileNames.segmentFileName(
//                    segmentWriteState.segmentInfo.name,
//                    segmentWriteState.segmentSuffix,
//                    "vmf"
//            );
//
//            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
//                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
//                output.writeInt(meanVector.length);
//                for (float value : meanVector) {
//                    output.writeInt(Float.floatToIntBits(value));
//                }
//
//                CodecUtil.writeFooter(output);
//                segmentWriteState.segmentInfo.addFile(meanVectorFileName);
//
//
//                System.out.println("Written file name: " + meanVectorFileName);
//                System.out.println("Written file length: " + output.getFilePointer());
//            }
//
//            try {
//                String[] files = segmentWriteState.directory.listAll();
//                System.out.println("Files in directory:");
//                for (String file : files) {
//                    System.out.println(" - " + file + " (size: " + segmentWriteState.directory.fileLength(file) + " bytes)");
//                }
//            } catch (IOException e) {
//                System.err.println("Error listing directory contents: " + e.getMessage());
//            }
//
//            // Add a file deletion listener
//            if (segmentWriteState.directory instanceof TrackingDirectoryWrapper) {
//                TrackingDirectoryWrapper trackingDir = (TrackingDirectoryWrapper) segmentWriteState.directory;
////                trackingDir.setDeleteListener(new Directory.DeleteListener() {
////                    @Override
////                    public void deleteFile(String name) {
////                        System.out.println("File being deleted: " + name);
////                    }
////                });
//            }
//        }


//        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
//            // Generate the filename
//            String meanVectorFileName = IndexFileNames.segmentFileName(
//                    segmentWriteState.segmentInfo.name,
//                    segmentWriteState.segmentSuffix,
//                    "vmf"
//            );
//
//            // Debug logging to verify the directory and filename
//            System.out.println("Directory: " + segmentWriteState.directory);
//            System.out.println("Attempting to write file: " + meanVectorFileName);
//
//            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
//                // Write header
//                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
//
//                // Write vector data
//                output.writeInt(meanVector.length);
//                for (float value : meanVector) {
//                    output.writeInt(Float.floatToIntBits(value));
//                }
//
//                // Write footer and flush
//                CodecUtil.writeFooter(output);
//
//                System.out.println("Mean vector stats written to: " + meanVectorFileName);
//            }
//
//            // Ensure proper syncing
//            Set<String> files = new HashSet<>();
//            files.add(meanVectorFileName);
//
//            try {
//                // Sync the specific file
//                segmentWriteState.directory.sync(files);
//
//                segmentWriteState.directory.syncMetaData();
//
//                System.out.println("Successfully synced file: " + meanVectorFileName);
//            } catch (IOException e) {
//                System.err.println("Error syncing file: " + e.getMessage());
//                throw e;
//            }
//        }

    // Add this helper method to verify file existence
//    private static void verifyFileExists(SegmentWriteState state, String fileName) {
//        try {
//            if (state.directory.fileLength(fileName)) {
//                System.out.println("File verified: " + fileName);
//                System.out.println("File length: " + state.directory.fileLength(fileName));
//            } else {
//                System.out.println("File does not exist: " + fileName);
//            }
//        } catch (IOException e) {
//            System.err.println("Error verifying file: " + e.getMessage());
//        }
//    }


//        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
//            String meanVectorFileName = IndexFileNames.segmentFileName(
//                    segmentWriteState.segmentInfo.name,
//                    segmentWriteState.segmentSuffix,
//                    "vmf"
//            );
//            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
//                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
//                output.writeInt(meanVector.length);
//                for (float value : meanVector) {
//                    output.writeInt(Float.floatToIntBits(value));
//                }
//               // output.writeInt(meanVector.length);
//                CodecUtil.writeFooter(output);
//
//                System.out.println("Mean vector stats saved to: " + meanVectorFileName);
//
//                flushSegment(segmentWriteState, meanVectorFileName);
//            }
//        }

        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
            String meanVectorFileName = IndexFileNames.segmentFileName(
                    segmentWriteState.segmentInfo.name,
                    segmentWriteState.segmentSuffix,
                    "txt"
            );

            //Path meanStatsFile = segmentWriteState.directory.toPath().resolve(meanVectorFileName);
            Directory directory = segmentWriteState.directory;
            while(directory instanceof FilterDirectory) {
                directory = ((FilterDirectory) directory).getDelegate();
            }

            if(!(directory instanceof FSDirectory)) {
                throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
            }

            Path directoryPath = ((FSDirectory) directory).getDirectory();
            Path meanStatsFile = directoryPath.resolve(meanVectorFileName);

            Files.createDirectories(meanStatsFile.getParent());

            StringBuilder sb = new StringBuilder();
            sb.append("Mean Vector Stats:\n");
            for (float value : meanVector) {
                sb.append(value).append(" ");
            }
            Files.write(meanStatsFile, sb.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println("Mean vector stats saved to" + meanStatsFile);
            }


//        public static void saveMeanVectorStats(SegmentWriteState segmentWriteState, float[] meanVector) throws IOException {
//            String meanVectorFileName = IndexFileNames.segmentFileName(
//                    segmentWriteState.segmentInfo.name,
//                    segmentWriteState.segmentSuffix,
//                    "json"
//            );
//
//            try (IndexOutput output = segmentWriteState.directory.createOutput(meanVectorFileName, segmentWriteState.context)) {
//                CodecUtil.writeIndexHeader(output, "MEAN_VECTOR_STATS", 1, segmentWriteState.segmentInfo.getId(), segmentWriteState.segmentSuffix);
//
//                StringBuilder jsonOutput = new StringBuilder();
//                jsonOutput.append("{\n  \"mean_vector\": [");
//                for (int i = 0; i < meanVector.length; i++) {
//                    jsonOutput.append(meanVector[i]);
//                    if (i < meanVector.length - 1) {
//                        jsonOutput.append(", ");
//                    }
//                }
//
//                jsonOutput.append("]\n}");
//                byte[] jsonBytes = jsonOutput.toString().getBytes(StandardCharsets.UTF_8);
//                output.writeBytes(jsonBytes, jsonBytes.length);
//                // output.writeInt(meanVector.length);
//
//                CodecUtil.writeFooter(output);
//                System.out.println("Mean vector stats saved to: " + meanVectorFileName);
//
//            }
//
//                flushSegment(segmentWriteState, meanVectorFileName);
//
//        }

        private static void flushSegment(SegmentWriteState segmentWriteState, String meanVectorFileName) {
                try {
                    segmentWriteState.directory.sync(Collections.singleton(meanVectorFileName));
                    System.out.println("Segment sync completed: " + meanVectorFileName);
                } catch (IOException e) {
                    System.err.println("Failed to sync segment: " + meanVectorFileName + " - " + e.getMessage());
                }
        }


//        public static void printMeanVectorStats(float[] meanVector) {
//            System.out.println("Per-dimension mean: " + Arrays.toString(meanVector));
//        }

//        public static void saveMeanVectorStats(float[] meanVector) {
//            try {
//                Path indicesDir = Paths.get("build/testclusters/integTest-0/data/nodes/0/indices");
//                if (!Files.exists(indicesDir)) {
//                    throw new IOException("Indices directory not found: " + indicesDir.toString());
//                }
//
//                Optional<Path> latestIndexOptional = Files.list(indicesDir)
//                        .filter(Files::isDirectory)
//                        .max(Comparator.comparingLong(path -> path.toFile().lastModified()));
//
//                if (latestIndexOptional.isEmpty()) {
//                    throw new IOException("No directories found in indices directory: " + indicesDir.toString());
//                }
//
//                Path latestIndexDir = latestIndexOptional.get();
//                Path outputPath = latestIndexDir.resolve("0/index/mean_output.bin");
//                Files.createDirectories(outputPath.getParent());
//
//                try (FSDirectory directory = FSDirectory.open(outputPath.getParent());
//                     IndexOutput output = directory.createOutput(outputPath.getFileName().toString(), null)) {
//                    CodecUtil.writeHeader(output, "MEAN_VECTOR_STATS", 1);
//
//                    output.writeInt(meanVector.length);
//                    ;
//
//                    for (float value : meanVector) {
//                        output.writeInt(Float.floatToIntBits(value));
//                    }
//
//                    output.writeInt(-1);
//
//                    System.out.println("Mean vector stats saved to: " + outputPath.toString());
//                }
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
            //Path outputPath = Paths.get("build/testclusters/integTest-0/data/nodes/0/indices/0/index/mean_output.json");
//            try {
//                Files.createDirectories(outputPath.getParent());
//                try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(outputPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
//                    out.writeUTF("VECTOR_STATS");
//                    out.writeInt(meanVector.length);
//                    for (float value : meanVector) {
//                        out.writeFloat(value);
//                    }
//                    out.writeInt(-1);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

}
