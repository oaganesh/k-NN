/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.knn.common.FieldInfoExtractor;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.index.codec.KNN990Codec.KNN990QuantizationStateReader;
import org.opensearch.knn.index.codec.util.NativeMemoryCacheKeyHelper;
import org.opensearch.knn.index.engine.qframe.QuantizationConfig;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.index.memory.NativeMemoryAllocation;
import org.opensearch.knn.index.memory.NativeMemoryCacheManager;
import org.opensearch.knn.index.memory.NativeMemoryEntryContext;
import org.opensearch.knn.index.memory.NativeMemoryLoadStrategy;
import org.opensearch.knn.index.engine.KNNEngine;
import org.opensearch.knn.index.query.SegmentLevelQuantizationUtil;
import org.opensearch.knn.profiler.ProfilerStateCollector;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationStateReadConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.opensearch.knn.common.KNNConstants.MODEL_ID;
import static org.opensearch.knn.common.KNNConstants.SPACE_TYPE;
import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;

import static org.opensearch.knn.index.util.IndexUtil.getParametersAtLoading;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.buildEngineFilePrefix;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.buildEngineFileSuffix;

/**
 * KNNIndexShard wraps IndexShard and adds methods to perform k-NN related operations against the shard
 */
@Log4j2
public class KNNIndexShard {
    private final IndexShard indexShard;
    private final NativeMemoryCacheManager nativeMemoryCacheManager;
    private static final String INDEX_SHARD_CLEAR_CACHE_SEARCHER = "knn-clear-cache";

    /**
     * Constructor to generate KNNIndexShard. We do not perform validation that the index the shard is from
     * is in fact a k-NN Index (index.knn = true). This may make sense to add later, but for now the operations for
     * KNNIndexShards that are not from a k-NN index should be no-ops.
     *
     * @param indexShard IndexShard to be wrapped.
     */
    public KNNIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
        this.nativeMemoryCacheManager = NativeMemoryCacheManager.getInstance();
    }

    /**
     * Return the underlying IndexShard
     *
     * @return IndexShard
     */
    public IndexShard getIndexShard() {
        return indexShard;
    }

    /**
     * Return the name of the shards index
     *
     * @return Name of shard's index
     */
    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

    /**
     * Load all of the k-NN segments for this shard into the cache.
     *
     * @throws IOException Thrown when getting the HNSW Paths to be loaded in
     */
    public void warmup() throws IOException {
        log.info("[KNN] Warming up index: [{}]", getIndexName());
        final Directory directory = indexShard.store().directory();

        try (Engine.Searcher searcher = indexShard.acquireSearcher("knn-warmup")) {
            // Get all KNN fields and their statistics
            Map<String, List<List<SummaryStatistics>>> fieldSegmentStats = new HashMap<>();

            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                SegmentReader segmentReader = Lucene.segmentReader(leafContext.reader());
                String segmentName = segmentReader.getSegmentName();

                // Find all fields that are KNN vector fields
                for (FieldInfo fieldInfo : segmentReader.getFieldInfos()) {
                    if (fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
                        String fieldName = fieldInfo.name;
                        int fieldNumber = fieldInfo.number;
                        log.info("[KNN Stats] Processing field: {} (number: {}) in segment: {}", fieldName, fieldNumber, segmentName);

                        // Look for quantization state file
                        String[] files = segmentReader.directory().listAll();
                        log.debug("[KNN Stats] Files in segment {}: {}", segmentName, String.join(", ", files));

                        for (String file : files) {
                            if (file.startsWith(segmentName) && file.endsWith(KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX)) {
                                log.info("[KNN Stats] Found quantization state file: {} for field: {}", file, fieldName);

                                try {
                                    // Extract the suffix correctly
                                    String suffix = file.substring(
                                        file.indexOf("NativeEngines990KnnVectorsFormat"),
                                        file.length() - KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX.length()
                                    );

                                    SegmentReadState segmentReadState = new SegmentReadState(
                                        segmentReader.directory(),
                                        segmentReader.getSegmentInfo().info,
                                        segmentReader.getFieldInfos(),
                                        IOContext.DEFAULT,
                                        suffix
                                    );

                                    QuantizationStateReadConfig readConfig = new QuantizationStateReadConfig(
                                        segmentReadState,
                                        null,
                                        fieldName,
                                        fieldName
                                    );

                                    SegmentProfilerState profilerState = KNN990QuantizationStateReader.readProfilerState(readConfig);

                                    if (profilerState != null) {
                                        List<SummaryStatistics> segmentStats = profilerState.getStatistics();
                                        fieldSegmentStats.computeIfAbsent(fieldName, k -> new ArrayList<>()).add(segmentStats);

                                        log.info(
                                            "[KNN Stats] Successfully read statistics for field {} from segment {}",
                                            fieldName,
                                            segmentName
                                        );
                                    } else {
                                        log.warn("[KNN Stats] No profiler state found for field {} in segment {}", fieldName, segmentName);
                                    }
                                } catch (Exception e) {
                                    log.error(
                                        "[KNN Stats] Error reading statistics from file {} for field {}: {}",
                                        file,
                                        fieldName,
                                        e.getMessage(),
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }

            // Aggregate and log statistics for each field
            for (Map.Entry<String, List<List<SummaryStatistics>>> entry : fieldSegmentStats.entrySet()) {
                String fieldName = entry.getKey();
                List<List<SummaryStatistics>> segmentStats = entry.getValue();

                if (!segmentStats.isEmpty()) {
                    List<SummaryStatistics> aggregatedStats = SegmentLevelQuantizationUtil.aggregateStatistics(segmentStats);

                    log.info("[KNN Stats] ===== Aggregated statistics for field: {} =====", fieldName);
                    log.info("[KNN Stats] Number of segments aggregated: {}", segmentStats.size());
                    log.info("[KNN Stats] Total dimensions: {}", aggregatedStats.size());

                    for (int i = 0; i < aggregatedStats.size(); i++) {
                        SummaryStatistics dim = aggregatedStats.get(i);
                        log.info(
                            "[KNN Stats] Dimension {}: count={}, min={}, max={}, mean={}, stddev={}",
                            i,
                            dim.getN(),
                            SegmentProfilerState.formatDouble(dim.getMin()),
                            SegmentProfilerState.formatDouble(dim.getMax()),
                            SegmentProfilerState.formatDouble(dim.getMean()),
                            SegmentProfilerState.formatDouble(dim.getStandardDeviation())
                        );
                    }
                } else {
                    log.warn("[KNN Stats] No statistics found for field: {}", fieldName);
                }
            }

            getAllEngineFileContexts(searcher.getIndexReader()).forEach((engineFileContext) -> {
                try {
                    final String cacheKey = NativeMemoryCacheKeyHelper.constructCacheKey(
                        engineFileContext.vectorFileName,
                        engineFileContext.segmentInfo
                    );
                    nativeMemoryCacheManager.get(
                        new NativeMemoryEntryContext.IndexEntryContext(
                            directory,
                            cacheKey,
                            NativeMemoryLoadStrategy.IndexLoadStrategy.getInstance(),
                            getParametersAtLoading(
                                engineFileContext.getSpaceType(),
                                KNNEngine.getEngineNameFromPath(engineFileContext.getVectorFileName()),
                                getIndexName(),
                                engineFileContext.getVectorDataType()
                            ),
                            getIndexName(),
                            engineFileContext.getModelId()
                        ),
                        true
                    );
                } catch (ExecutionException ex) {
                    throw new RuntimeException(ex);
                }
            });
            log.info("[KNN] Collecting statistics for index [{}]", getIndexName());
            Map<String, List<SummaryStatistics>> shardStats = collectShardStatistics();
            log.info("[KNN] Collected statistics for {} fields in index [{}]", shardStats.size(), getIndexName());
        }
    }

    /**
     * Removes all the k-NN segments for this shard from the cache.
     * Adding write lock onto the NativeMemoryAllocation of the index that needs to be evicted from cache.
     * Write lock will be unlocked after the index is evicted. This locking mechanism is used to avoid
     * conflicts with queries fired on this index when the index is being evicted from cache.
     */
    public void clearCache() {
        String indexName = getIndexName();
        Optional<NativeMemoryAllocation> indexAllocationOptional;
        NativeMemoryAllocation indexAllocation;
        indexAllocationOptional = nativeMemoryCacheManager.getIndexMemoryAllocation(indexName);
        if (indexAllocationOptional.isPresent()) {
            indexAllocation = indexAllocationOptional.get();
            indexAllocation.writeLock();
            log.info("[KNN] Evicting index from cache: [{}]", indexName);
            try (Engine.Searcher searcher = indexShard.acquireSearcher(INDEX_SHARD_CLEAR_CACHE_SEARCHER)) {
                getAllEngineFileContexts(searcher.getIndexReader()).forEach((engineFileContext) -> {
                    final String cacheKey = NativeMemoryCacheKeyHelper.constructCacheKey(
                        engineFileContext.vectorFileName,
                        engineFileContext.segmentInfo
                    );
                    nativeMemoryCacheManager.invalidate(cacheKey);
                });
            } catch (IOException ex) {
                log.error("[KNN] Failed to evict index from cache: [{}]", indexName, ex);
                throw new RuntimeException(ex);
            } finally {
                indexAllocation.writeUnlock();
            }
        }
    }

    /**
     * For the given shard, get all of its engine file context objects
     *
     * @param indexReader IndexReader to read the information for each segment in the shard
     * @return List of engine contexts
     * @throws IOException Thrown when the SegmentReader is attempting to read the segments files
     */
    @VisibleForTesting
    List<EngineFileContext> getAllEngineFileContexts(IndexReader indexReader) throws IOException {
        List<EngineFileContext> engineFiles = new ArrayList<>();
        for (KNNEngine knnEngine : KNNEngine.getEnginesThatCreateCustomSegmentFiles()) {
            engineFiles.addAll(getEngineFileContexts(indexReader, knnEngine));
        }
        return engineFiles;
    }

    List<EngineFileContext> getEngineFileContexts(IndexReader indexReader, KNNEngine knnEngine) throws IOException {
        List<EngineFileContext> engineFiles = new ArrayList<>();

        for (LeafReaderContext leafReaderContext : indexReader.leaves()) {
            SegmentReader reader = Lucene.segmentReader(leafReaderContext.reader());
            String fileExtension = reader.getSegmentInfo().info.getUseCompoundFile()
                ? knnEngine.getCompoundExtension()
                : knnEngine.getExtension();

            for (FieldInfo fieldInfo : reader.getFieldInfos()) {
                if (fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
                    // Space Type will not be present on ES versions 7.1 and 7.4 because the only available space type
                    // was L2. So, if Space Type is not present, just fall back to L2
                    String spaceTypeName = fieldInfo.attributes().getOrDefault(SPACE_TYPE, SpaceType.L2.getValue());
                    SpaceType spaceType = SpaceType.getSpace(spaceTypeName);
                    String modelId = fieldInfo.attributes().getOrDefault(MODEL_ID, null);
                    engineFiles.addAll(
                        getEngineFileContexts(
                            reader.getSegmentInfo(),
                            fieldInfo.name,
                            fileExtension,
                            spaceType,
                            modelId,
                            FieldInfoExtractor.extractQuantizationConfig(fieldInfo) == QuantizationConfig.EMPTY
                                ? VectorDataType.get(
                                    fieldInfo.attributes().getOrDefault(VECTOR_DATA_TYPE_FIELD, VectorDataType.FLOAT.getValue())
                                )
                                : VectorDataType.BINARY
                        )
                    );
                }
            }
        }
        return engineFiles;
    }

    @VisibleForTesting
    List<EngineFileContext> getEngineFileContexts(
        SegmentCommitInfo segmentCommitInfo,
        String fieldName,
        String fileExtension,
        SpaceType spaceType,
        String modelId,
        VectorDataType vectorDataType
    ) throws IOException {
        // Ex: 0_
        final String prefix = buildEngineFilePrefix(segmentCommitInfo.info.name);
        // Ex: _my_field.faiss
        final String suffix = buildEngineFileSuffix(fieldName, fileExtension);
        return segmentCommitInfo.files()
            .stream()
            .filter(fileName -> fileName.startsWith(prefix))
            .filter(fileName -> fileName.endsWith(suffix))
            .map(vectorFileName -> new EngineFileContext(spaceType, modelId, vectorFileName, vectorDataType, segmentCommitInfo.info))
            .collect(Collectors.toList());
    }

    @AllArgsConstructor
    @Getter
    @VisibleForTesting
    static class EngineFileContext {
        private final SpaceType spaceType;
        private final String modelId;
        private final String vectorFileName;
        private final VectorDataType vectorDataType;
        private final SegmentInfo segmentInfo;
    }

    /**
     * Collects statistics for each field in the shard
     * @return
     * @throws IOException
     */
//    public Map<String, List<SummaryStatistics>> collectShardStatistics() throws IOException {
//        Map<String, List<List<SummaryStatistics>>> fieldSegmentStats = new HashMap<>();
//        log.info("[KNN Debug] Starting statistics collection in shard {}", indexShard.shardId());
//
//        try (Engine.Searcher searcher = indexShard.acquireSearcher("knn-stats-collection")) {
//            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
//                SegmentReader segmentReader = Lucene.segmentReader(leafContext.reader());
//                String segmentName = segmentReader.getSegmentName();
//                log.info("[KNN Debug] Processing segment: {}", segmentName);
//
//                // Get directory info for this segment
//                Directory directory = segmentReader.directory();
//                if (directory instanceof FSDirectory) {
//                    Path dirPath = ((FSDirectory) directory).getDirectory();
//                    log.info("[KNN Debug] Directory path for segment {}: {}", segmentName, dirPath);
//
//                    // List all files in the directory
//                    log.info("[KNN Debug] Files in directory: {}", String.join(", ", directory.listAll()));
//                }
//
//                // List all KNN fields
//                Map<Integer, String> fieldNumberToName = new HashMap<>();
//                for (FieldInfo fieldInfo : segmentReader.getFieldInfos()) {
//                    if (fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
//                        fieldNumberToName.put(fieldInfo.number, fieldInfo.name);
//                        log.info("[KNN Debug] Found KNN field: {} (number: {})", fieldInfo.name, fieldInfo.number);
//                    }
//                }
//
//                // Log all files in directory
//                String[] files = segmentReader.directory().listAll();
//                log.info("[KNN Debug] Files in segment {}: {}", segmentName, String.join(", ", files));
//
//                // Look specifically for quantization state files
//                for (String file : files) {
//                    if (file.endsWith(KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX)) {
//                        log.info("[KNN Debug] Found quantization state file: {}", file);
//
//                        // Try to read the file directly for diagnostics
//                        try (IndexInput input = segmentReader.directory().openInput(file, IOContext.DEFAULT)) {
//                            // Check header
//                            try {
//                                // Use the string constant directly to avoid reference issues
//                                CodecUtil.checkHeader(input, "NativeEngines990KnnVectorsFormatQSData", 0, 0);
//                                log.info("[KNN Debug] Successfully verified header for file: {}", file);
//                            } catch (Exception e) {
//                                log.error("[KNN Debug] Invalid header in file {}: {}", file, e.getMessage());
//                                continue;
//                            }
//
//                            // Read footer to find index position
//                            long fileLength = input.length();
//                            long footerStart = fileLength - CodecUtil.footerLength();
//                            input.seek(footerStart - Integer.BYTES - Long.BYTES);
//                            long indexStartPosition = input.readLong();
//                            int marker = input.readInt();
//                            log.info(
//                                "[KNN Debug] File: {}, length: {}, index position: {}, marker: {}",
//                                file,
//                                fileLength,
//                                indexStartPosition,
//                                marker
//                            );
//
//                            // Read index
//                            input.seek(indexStartPosition);
//                            int numQuantizationStates = input.readInt();
//                            log.info("[KNN Debug] Quantization states: {}", numQuantizationStates);
//
//                            // Skip quantization states
//                            for (int i = 0; i < numQuantizationStates; i++) {
//                                input.readInt();    // field number
//                                input.readInt();    // length
//                                input.readVLong();  // position
//                            }
//
//                            // Read profiler states
//                            int numProfilerStates = input.readInt();
//                            log.info("[KNN Debug] Profiler states: {}", numProfilerStates);
//
//                            List<Integer> foundFieldNumbers = new ArrayList<>();
//                            for (int i = 0; i < numProfilerStates; i++) {
//                                int fieldNumber = input.readInt();
//                                int length = input.readInt();
//                                long position = input.readVLong();
//                                foundFieldNumbers.add(fieldNumber);
//
//                                String fieldName = fieldNumberToName.get(fieldNumber);
//                                log.info(
//                                    "[KNN Debug] Found profiler state: field={} ({}), length={}, position={}",
//                                    fieldNumber,
//                                    fieldName,
//                                    length,
//                                    position
//                                );
//
//                                if (fieldName != null) {
//                                    // Try to read this profiler state
//                                    long currentPosition = input.getFilePointer();
//                                    input.seek(position);
//                                    byte[] stateBytes = new byte[length];
//                                    input.readBytes(stateBytes, 0, length);
//
//                                    try {
//                                        SegmentProfilerState profilerState = SegmentProfilerState.fromByteArray(stateBytes);
//                                        log.info(
//                                            "[KNN Debug] Successfully read profiler state for field {} with {} dimensions",
//                                            fieldName,
//                                            profilerState.getStatistics().size()
//                                        );
//
//                                        // Store the statistics
//                                        fieldSegmentStats.computeIfAbsent(fieldName, k -> new ArrayList<>())
//                                            .add(profilerState.getStatistics());
//                                    } catch (Exception e) {
//                                        log.error("[KNN Debug] Failed to deserialize profiler state: {}", e.getMessage());
//                                    }
//
//                                    // Return to the index position
//                                    input.seek(currentPosition);
//                                } else {
//                                    log.warn("[KNN Debug] Found profiler state for unknown field number: {}", fieldNumber);
//                                }
//                            }
//
//                            // Compare found field numbers with what we expected
//                            Set<Integer> missingFields = new HashSet<>(fieldNumberToName.keySet());
//                            missingFields.removeAll(foundFieldNumbers);
//                            if (!missingFields.isEmpty()) {
//                                log.warn("[KNN Debug] Some KNN fields have no profiler states: {}", missingFields);
//                            }
//                        } catch (Exception e) {
//                            log.error("[KNN Debug] Error reading file {}: {}", file, e.getMessage(), e);
//                        }
//                    }
//                }
//            }
//
//            // Aggregate statistics for each field
//            Map<String, List<SummaryStatistics>> aggregatedFieldStats = new HashMap<>();
//            for (Map.Entry<String, List<List<SummaryStatistics>>> entry : fieldSegmentStats.entrySet()) {
//                String fieldName = entry.getKey();
//                List<List<SummaryStatistics>> segmentStats = entry.getValue();
//
//                if (!segmentStats.isEmpty()) {
//                    List<SummaryStatistics> aggregatedStats = SegmentLevelQuantizationUtil.aggregateStatistics(segmentStats);
//                    aggregatedFieldStats.put(fieldName, aggregatedStats);
//
//                    // Log aggregated statistics
//                    log.info("[KNN Stats] ===== Aggregated statistics for field: {} =====", fieldName);
//                    log.info("[KNN Stats] Number of segments aggregated: {}", segmentStats.size());
//                    log.info("[KNN Stats] Total dimensions: {}", aggregatedStats.size());
//
//                    for (int i = 0; i < aggregatedStats.size(); i++) {
//                        SummaryStatistics dim = aggregatedStats.get(i);
//                        log.info(
//                            "[KNN Stats] Dimension {}: count={}, min={}, max={}, mean={}, stddev={}",
//                            i,
//                            dim.getN(),
//                            SegmentProfilerState.formatDouble(dim.getMin()),
//                            SegmentProfilerState.formatDouble(dim.getMax()),
//                            SegmentProfilerState.formatDouble(dim.getMean()),
//                            SegmentProfilerState.formatDouble(dim.getStandardDeviation())
//                        );
//                    }
//                }
//            }
//
//            return aggregatedFieldStats;
//        }
//    }

    public Map<String, List<SummaryStatistics>> collectShardStatistics() throws IOException {
        Map<String, List<List<SummaryStatistics>>> fieldSegmentStats = new HashMap<>();

        try (Engine.Searcher searcher = indexShard.acquireSearcher("knn-stats-collection")) {
            for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                for (FieldInfo fieldInfo : Lucene.segmentReader(leafContext.reader()).getFieldInfos()) {
                    if (fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD)) {
                        String fieldName = fieldInfo.name;

                        ProfilerStateCollector collector = new ProfilerStateCollector();
                        leafContext.reader().searchNearestVectors(fieldName, new float[0], collector, null);

                        if (collector.getProfilerState() != null) {
                            fieldSegmentStats.computeIfAbsent(fieldName, k -> new ArrayList<>())
                                    .add(collector.getProfilerState().getStatistics());
                        }
                    }
                }
            }
        }

        // Aggregate statistics correctly
        Map<String, List<SummaryStatistics>> result = new HashMap<>();
        for (Map.Entry<String, List<List<SummaryStatistics>>> entry : fieldSegmentStats.entrySet()) {
            result.put(entry.getKey(), SegmentLevelQuantizationUtil.aggregateStatistics(entry.getValue()));
        }

        return result;
    }
}
