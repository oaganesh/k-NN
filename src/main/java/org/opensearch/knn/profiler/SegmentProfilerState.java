/*
Copyright OpenSearch Contributors
SPDX-License-Identifier: Apache-2.0
*/

package org.opensearch.knn.profiler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * This class handles the profiling and statistical analysis of KNN vector segments
 * in OpenSearch. It provides functionality to collect, process, and store statistical
 * information about vector dimensions across different shards and segments.
 */
@Log4j2
public class SegmentProfilerState {

    public static final String VECTOR_STATS_EXTENSION = "stats";
    public static final String VECTOR_OUTPUT_FILE = "NativeEngines990KnnVectors";
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    // JSON field names
    private static final String FIELD_DOC_COUNT = "doc_count";
    private static final String FIELD_SIZE_IN_BYTES = "size_in_bytes";
    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_VECTOR_STATS = "vector_stats";
    private static final String FIELD_SAMPLE_SIZE = "sample_size";
    private static final String FIELD_SUMMARY_STATS = "summary_stats";
    private static final String FIELD_STATUS = "status";
    private static final String FIELD_DIMENSIONS = "dimensions";
    private static final String FIELD_DIMENSION = "dimension";
    private static final String FIELD_COUNT = "count";
    private static final String FIELD_MIN = "min";
    private static final String FIELD_MAX = "max";
    private static final String FIELD_SUM = "sum";
    private static final String FIELD_MEAN = "mean";
    private static final String FIELD_STD_DEV = "standardDeviation";
    private static final String FIELD_VARIANCE = "variance";
    private static final String FIELD_ERROR = "error";
    private static final String FIELD_MESSAGE = "message";

    // Directory structure constants
    private static final String DIR_NODES = "nodes";
    private static final String DIR_INDICES = "indices";
    private static final String DIR_INDEX = "index";
    private static final String NODE_ID = "0";

    private static final int MINIMUM_STATS_COUNT_0 = 0;
    private static final int MINIMUM_STATS_COUNT_1 = 1;
    private static final int MINIMUM_STATS_COUNT_2 = 2;

    @Getter
    private final List<SummaryStatistics> statistics;

    /**
     * Constructor initializing the statistics collection
     * @param statistics List of summary statistics for vector dimensions
     */
    public SegmentProfilerState(final List<SummaryStatistics> statistics) {
        this.statistics = statistics;
    }

    /**
     * Writes statistical data to a JSON file.
     * Stores raw statistics data to disk for later retrieval.
     * Called during vector indexing/processing.
     * @param outputFile Path to output file
     * @param statistics List of statistics to write
     * @param fieldName Name of the field being processed
     * @param vectorCount Total number of vectors
     */
    private static void writeStatsToFile(
        final Path outputFile,
        final List<SummaryStatistics> statistics,
        final String fieldName,
        final int vectorCount
    ) throws IOException {
        Directory directory = FSDirectory.open(outputFile.getParent());
        String fileName = outputFile.getFileName().toString();

        try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, VECTOR_OUTPUT_FILE, 0);

            output.writeString(ISO_FORMATTER.format(Instant.now()));
            output.writeString(fieldName);
            output.writeInt(vectorCount);
            output.writeInt(statistics.size());

            for (int i = 0; i < statistics.size(); i++) {
                SummaryStatistics stats = statistics.get(i);
                output.writeInt(i);
                output.writeLong(stats.getN());
                output.writeLong(Double.doubleToLongBits(stats.getMin()));
                output.writeLong(Double.doubleToLongBits(stats.getMax()));
                output.writeLong(Double.doubleToLongBits(stats.getSum()));
                output.writeLong(Double.doubleToLongBits(stats.getMean()));
                output.writeLong(Double.doubleToLongBits(Math.sqrt(stats.getVariance())));
                output.writeLong(Double.doubleToLongBits(stats.getVariance()));
            }

            CodecUtil.writeFooter(output);
        } finally {
            directory.close();
        }
    }

    /**
     * Reads statistics from a file
     * @param path Path to the statistics file
     * @return List of summary statistics
     */
    public static List<SummaryStatistics> readStatsFromFile(Path path) throws IOException {
        Directory directory = FSDirectory.open(path.getParent());
        String fileName = path.getFileName().toString();

        try (
            IndexInput indexInput = directory.openInput(fileName, IOContext.DEFAULT);
            BufferedChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput)
        ) {

            CodecUtil.checkHeader(input, VECTOR_OUTPUT_FILE, 0, 0);

            String timestamp = input.readString();
            String fieldName = input.readString();
            int vectorCount = input.readInt();
            int dimensionCount = input.readInt();

            if (dimensionCount <= 0 || dimensionCount > 10000) {
                throw new CorruptIndexException("Invalid dimension count: " + dimensionCount, input);
            }

            List<SummaryStatistics> statistics = new ArrayList<>(dimensionCount);
            for (int i = 0; i < dimensionCount; i++) {
                SummaryStatistics stats = new SummaryStatistics();

                int dimension = input.readInt();
                if (dimension != i) {
                    throw new CorruptIndexException("Dimension mismatch: expected " + i + ", got " + dimension, input);
                }

                long count = input.readLong();
                if (count < 0 || count > vectorCount) {
                    throw new CorruptIndexException("Invalid count: " + count, input);
                }

                double min = Double.longBitsToDouble(input.readLong());
                double max = Double.longBitsToDouble(input.readLong());
                double sum = Double.longBitsToDouble(input.readLong());
                double mean = Double.longBitsToDouble(input.readLong());
                double stdDev = Double.longBitsToDouble(input.readLong());
                double variance = Double.longBitsToDouble(input.readLong());

                if (!Double.isFinite(min)
                    || !Double.isFinite(max)
                    || !Double.isFinite(sum)
                    || !Double.isFinite(mean)
                    || !Double.isFinite(stdDev)
                    || !Double.isFinite(variance)) {
                    throw new CorruptIndexException("Invalid statistics values", input);
                }

                if (count > 0) {
                    stats.addValue(min);
                    if (count > 1) {
                        stats.addValue(max);
                    }
                    if (count > 2) {
                        double remainingMean = (sum - min - max) / (count - 2);
                        for (int j = 0; j < count - 2; j++) {
                            stats.addValue(remainingMean);
                        }
                    }
                }

                statistics.add(stats);
            }

            CodecUtil.checkFooter(input);

            return statistics;
        } catch (IOException e) {
            throw new IOException("Failed to read stats file: " + fileName, e);
        } finally {
            directory.close();
        }
    }

    /**
     * Profiles vectors in a segment and collects statistical information
     * @param knnVectorValuesSupplier Supplier for vector values
     * @param segmentWriteState State of the segment being written
     * @param fieldName Name of the field being processed
     * @return SegmentProfilerState containing collected statistics
     */
    public static SegmentProfilerState profileVectors(
        final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier,
        final SegmentWriteState segmentWriteState,
        final String fieldName
    ) throws IOException {
        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();
        if (vectorValues == null) {
            return new SegmentProfilerState(new ArrayList<>());
        }
        SegmentProfilerState profilerState = new SegmentProfilerState(new ArrayList<>());
        KNNCodecUtil.initializeVectorValues(vectorValues);
        int dimension = vectorValues.dimension();
        int vectorCount = 0;

        for (int i = 0; i < dimension; i++) {
            profilerState.statistics.add(new SummaryStatistics());
        }

        while (vectorValues.docId() != NO_MORE_DOCS) {
            vectorCount++;
            Object vector = vectorValues.getVector();
            processVector(vector, profilerState.statistics);
            vectorValues.nextDoc();
        }

        String statsFileName = IndexFileNames.segmentFileName(
            segmentWriteState.segmentInfo.name,
            segmentWriteState.segmentSuffix,
            VECTOR_STATS_EXTENSION
        );

        Directory directory = getUnderlyingDirectory(segmentWriteState.directory);
        Path statsFile = ((FSDirectory) directory).getDirectory().resolve(statsFileName);
        writeStatsToFile(statsFile, profilerState.statistics, fieldName, vectorCount);

        return profilerState;
    }

    /**
     * Determines the path to a shard's index directory
     * @param shard Shard statistics
     * @param environment OpenSearch environment
     * @return Path to the shard's index directory
     */
    public static Path getShardIndexPath(ShardStats shard, Environment environment) {
        int shardId = shard.getShardRouting().shardId().getId();
        String indexUUID = shard.getShardRouting().shardId().getIndex().getUUID();
        return environment.dataFiles()[0].resolve(DIR_NODES)
            .resolve(NODE_ID)
            .resolve(DIR_INDICES)
            .resolve(indexUUID)
            .resolve(String.valueOf(shardId))
            .resolve(DIR_INDEX);
    }

    /**
     * Processes a vector and updates statistics
     * @param vector Vector to process (float[] or byte[])
     * @param statistics List of statistics to update
     */
    public static <T> void processVector(T vector, List<SummaryStatistics> statistics) {
        if (vector instanceof float[]) {
            float[] floatVector = (float[]) vector;
            for (int j = 0; j < floatVector.length; j++) {
                statistics.get(j).addValue(floatVector[j]);
            }
        } else if (vector instanceof byte[]) {
            byte[] byteVector = (byte[]) vector;
            for (int j = 0; j < byteVector.length; j++) {
                statistics.get(j).addValue(byteVector[j] & 0xFF);
            }
        }
    }

    /**
     * Gets the underlying FSDirectory from a potentially wrapped Directory
     * @param directory Input directory
     * @return Underlying FSDirectory
     */
    public static Directory getUnderlyingDirectory(Directory directory) throws IOException {
        while (directory instanceof FilterDirectory) {
            directory = ((FilterDirectory) directory).getDelegate();
        }
        if (!(directory instanceof FSDirectory)) {
            throw new IOException("Expected FSDirectory but found " + directory.getClass().getSimpleName());
        }
        return directory;
    }

    /**
     * Formats a double value according to the specified decimal format
     * @param value Double value to format
     * @return Formatted double value
     */
    public static double formatDouble(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }

    /**
     * Generates index-level statistics and writes them to the XContentBuilder.
     */
    public static void getIndexStats(IndexStats indexStats, XContentBuilder builder, Environment environment) throws IOException {
        try {
            builder.startObject("index_summary")
                .field(FIELD_DOC_COUNT, indexStats.getTotal().getDocs().getCount())
                .field(FIELD_SIZE_IN_BYTES, indexStats.getTotal().getStore().getSizeInBytes())
                .field(FIELD_TIMESTAMP, ISO_FORMATTER.format(Instant.now()))
                .endObject();

            builder.startObject(FIELD_VECTOR_STATS).field(FIELD_SAMPLE_SIZE, indexStats.getTotal().getDocs().getCount());

            builder.startObject(FIELD_SUMMARY_STATS);

            Map<String, List<SummaryStatistics>> fieldStats = getFieldStatistics(indexStats, environment);

            if (!fieldStats.isEmpty()) {
                for (Map.Entry<String, List<SummaryStatistics>> fieldEntry : fieldStats.entrySet()) {
                    String fieldName = fieldEntry.getKey();
                    List<SummaryStatistics> dimensionStats = fieldEntry.getValue();

                    builder.startObject(fieldName);
                    builder.startArray(FIELD_DIMENSIONS);

                    for (int i = 0; i < dimensionStats.size(); i++) {
                        SummaryStatistics dimStats = dimensionStats.get(i);
                        builder.startObject()
                            .field(FIELD_DIMENSION, i)
                            .field(FIELD_COUNT, dimStats.getN())
                            .field(FIELD_MIN, formatDouble(dimStats.getMin()))
                            .field(FIELD_MAX, formatDouble(dimStats.getMax()))
                            .field(FIELD_SUM, formatDouble(dimStats.getSum()))
                            .field(FIELD_MEAN, formatDouble(dimStats.getMean()))
                            .field(FIELD_STD_DEV, formatDouble(dimStats.getStandardDeviation()))
                            .field(FIELD_VARIANCE, formatDouble(dimStats.getVariance()))
                            .endObject();
                    }

                    builder.endArray();
                    builder.endObject();
                }
            } else {
                builder.field(FIELD_STATUS, "No statistics available");
            }

            builder.endObject().endObject();

        } catch (Exception e) {
            builder.startObject(FIELD_ERROR).field(FIELD_MESSAGE, "Failed to get statistics: " + e.getMessage()).endObject();
        }
    }

    /**
     * Collects and aggregates statistics for all fields across all shards
     */
    private static Map<String, List<SummaryStatistics>> getFieldStatistics(IndexStats indexStats, Environment environment)
        throws IOException {

        Map<String, List<SummaryStatistics>> fieldStats = new HashMap<>();
        ShardStats[] shardStats = indexStats.getShards();

        if (shardStats != null) {
            for (ShardStats shard : shardStats) {
                try {
                    Path indexPath = getShardIndexPath(shard, environment);
                    if (Files.exists(indexPath)) {
                        processShardStatistics(indexPath, fieldStats);
                    }
                } catch (Exception e) {
                    log.error("Error processing shard stats", e);
                }
            }
        }
        return fieldStats;
    }

    /**
     * Process statistics files in a shard directory and aggregate them
     */
    private static void processShardStatistics(Path indexPath, Map<String, List<SummaryStatistics>> fieldStats) throws IOException {

        Files.list(indexPath).filter(path -> path.getFileName().toString().contains(VECTOR_OUTPUT_FILE)).forEach(path -> {
            try {
                List<SummaryStatistics> fileStats = readStatsFromFile(path);

                String fieldName = readFieldNameFromStatsFile(path);

                if (fieldName != null && !fileStats.isEmpty()) {
                    List<SummaryStatistics> fieldDimensionStats = fieldStats.computeIfAbsent(
                        fieldName,
                        k -> initializeDimensionStats(fileStats.size())
                    );

                    for (int i = 0; i < fileStats.size(); i++) {
                        SummaryStatistics source = fileStats.get(i);
                        SummaryStatistics target = fieldDimensionStats.get(i);

                        if (source.getN() > MINIMUM_STATS_COUNT_0) {
                            target.addValue(source.getMin());
                            if (source.getN() > MINIMUM_STATS_COUNT_1) {
                                target.addValue(source.getMax());
                            }
                            if (source.getN() > MINIMUM_STATS_COUNT_2) {
                                double remainingMean = (source.getSum() - source.getMin() - source.getMax()) / (source.getN() - 2);
                                for (int j = 0; j < source.getN() - 2; j++) {
                                    target.addValue(remainingMean);
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Failed to process stats file: " + path, e);
            }
        });
    }

    /**
     * Reads just the field name from a statistics file
     */
    private static String readFieldNameFromStatsFile(Path path) throws IOException {
        Directory directory = FSDirectory.open(path.getParent());
        String fileName = path.getFileName().toString();

        try (
            IndexInput indexInput = directory.openInput(fileName, IOContext.DEFAULT);
            BufferedChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput)
        ) {

            CodecUtil.checkHeader(input, VECTOR_OUTPUT_FILE, 0, 0);
            input.readString();

            return input.readString();
        } catch (IOException e) {
            log.error("Failed to read field name from stats file: " + fileName, e);
            return null;
        } finally {
            directory.close();
        }
    }

    /**
     * Initialize statistics objects for each dimension
     */
    private static List<SummaryStatistics> initializeDimensionStats(int dimensionCount) {
        List<SummaryStatistics> stats = new ArrayList<>(dimensionCount);
        for (int i = 0; i < dimensionCount; i++) {
            stats.add(new SummaryStatistics());
        }
        return stats;
    }
}
