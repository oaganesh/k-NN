/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.plugin.KNNPlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest handler for serving KNN vector sampling statistics
 */
@Log4j2
public class RestKNNSamplingStatsHandler extends BaseRestHandler {
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Environment environment;
    private final IndicesService indicesService;

    public RestKNNSamplingStatsHandler(
            Settings settings,
            ClusterService clusterService,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Environment environment,
            IndicesService indicesService
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.environment = environment;
        this.indicesService = indicesService;
    }

    @Override
    public String getName() {
        return "knn_sampling_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, KNNPlugin.KNN_BASE_URI + "/sampling/{index}/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexName = request.param("index");
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException("Index name is required");
        }

        log.info("Received stats request for index: {}", indexName);

        return channel -> {
            // For direct segment access, try to get the index service first
            try {
                // This will throw an exception if the index doesn't exist
                IndexService indexService = indicesService.indexServiceSafe(
                        clusterService.state().metadata().index(indexName).getIndex()
                );

                // If we have the index service, we can process the index directly
                log.info("Using direct segment access for index: {}", indexName);
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();

                // Generate index stats using direct segment access
                try {
                    // Get basic index stats
                    long docCount = indexService.getShard(0).docStats().getCount();
                    builder.startObject("index_summary");
                    builder.field("doc_count", docCount);
                    builder.field("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));
                    builder.endObject();

                    // Process KNN fields in all shards
                    builder.startObject("vector_stats");
                    builder.field("sample_size", docCount);
                    builder.startObject("summary_stats");

                    // Get KNN fields and process them through all shards
                    Set<String> knnFields = getKNNFields(indexService);

                    if (!knnFields.isEmpty()) {
                        for (String fieldName : knnFields) {
                            List<SummaryStatistics> stats = new ArrayList<>();

                            // Process each shard
                            for (IndexShard shard : indexService) {
                                try (Engine.Searcher searcher = shard.acquireSearcher("knn-stats")) {
                                    // Process each segment
                                    for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
                                        try {
                                            SegmentProfilerState segmentState = SegmentProfilerState.profileVectorsFromLeafReader(
                                                    leafContext.reader(), fieldName
                                            );

                                            if (segmentState.getStatistics().size() > 0) {
                                                if (stats.isEmpty()) {
                                                    stats.addAll(segmentState.getStatistics());
                                                } else {
                                                    SegmentProfilerState.mergeStatistics(stats, segmentState.getStatistics());
                                                }
                                            }
                                        } catch (Exception e) {
                                            log.error("Error processing field {} in leaf reader: {}", fieldName, e.getMessage(), e);
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("Error processing shard {}: {}", shard, e.getMessage(), e);
                                }
                            }

                            // If we found stats, add them to the response
                            if (!stats.isEmpty()) {
                                builder.startArray("dimensions");
                                for (int i = 0; i < stats.size(); i++) {
                                    SummaryStatistics dimStats = stats.get(i);
                                    builder.startObject();
                                    builder.field("dimension", i);
                                    builder.field("min", SegmentProfilerState.formatDouble(dimStats.getMin()));
                                    builder.field("max", SegmentProfilerState.formatDouble(dimStats.getMax()));
                                    builder.field("mean", SegmentProfilerState.formatDouble(dimStats.getMean()));
                                    builder.field("std_dev", SegmentProfilerState.formatDouble(Math.sqrt(dimStats.getVariance())));
                                    builder.field("variance", SegmentProfilerState.formatDouble(dimStats.getVariance()));
                                    builder.field("count", dimStats.getN());
                                    builder.field("sum", SegmentProfilerState.formatDouble(dimStats.getSum()));
                                    builder.endObject();
                                }
                                builder.endArray();

                                // Add derived metrics
                                builder.field("sparsity", SegmentProfilerState.calculateSparsity(stats));
                                builder.field("clustering_coefficient", SegmentProfilerState.calculateClusteringCoefficient(stats));
                            } else {
                                builder.field("status", "No statistics available for field " + fieldName);
                            }
                        }
                    } else {
                        builder.field("status", "No KNN vector fields found in index");
                    }

                    builder.endObject(); // end summary_stats
                    builder.endObject(); // end vector_stats
                } catch (Exception e) {
                    log.error("Error processing index {}: {}", indexName, e.getMessage(), e);
                    builder.field("error", "Failed to process index: " + e.getMessage());
                }

                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));

            } catch (Exception e) {
                // Fallback to regular stats if we can't get the index service
                log.info("Falling back to regular stats for index: {}", indexName);

                client.admin()
                        .indices()
                        .prepareStats(indexName)
                        .clear()
                        .setDocs(true)
                        .setStore(true)
                        .execute(new ActionListener<>() {
                            @Override
                            public void onResponse(IndicesStatsResponse indicesStatsResponse) {
                                try {
                                    log.info("Processing stats response for index: {}", indexName);
                                    XContentBuilder builder = channel.newBuilder();
                                    builder.startObject();

                                    IndexStats indexStats = indicesStatsResponse.getIndex(indexName);
                                    if (indexStats != null) {
                                        SegmentProfilerState.getIndexStats(indexStats, builder, environment);
                                    } else {
                                        log.warn("No stats found for index: {}", indexName);
                                        builder.field("error", "No stats found for index: " + indexName);
                                    }

                                    builder.endObject();
                                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                                } catch (Exception e) {
                                    log.error("Error processing stats response", e);
                                    onFailure(e);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                log.error("Failed to get stats", e);
                                try {
                                    XContentBuilder builder = channel.newBuilder();
                                    builder.startObject();
                                    builder.field("error", "Failed to get stats: " + e.getMessage());
                                    builder.endObject();
                                    channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
                                } catch (IOException ex) {
                                    log.error("Failed to send error response", ex);
                                }
                            }
                        });
            }
        };
    }

    /**
     * Get a list of all KNN fields in an index
     */
    private static Set<String> getKNNFields(IndexService indexService) {
        Set<String> knnFields = new HashSet<>();
        MapperService mapperService = indexService.mapperService();

        // Convert Iterable to Map
        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        mapperService.fieldTypes().forEach(fieldType -> fieldTypes.put(fieldType.name(), fieldType));

        // Filter for KNN vector fields
        for (Map.Entry<String, MappedFieldType> entry : fieldTypes.entrySet()) {
            // Check if field is a KNN vector field using the content type
            if (KNNVectorFieldMapper.CONTENT_TYPE.equals(entry.getValue().typeName())) {
                knnFields.add(entry.getKey());
            }
        }

        return knnFields;
    }
}