/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.plugin.rest;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.env.Environment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.knn.index.KNNIndexShard;
import org.opensearch.knn.plugin.KNNPlugin;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.GET;

@Log4j2
@AllArgsConstructor
/**
 * Rest handler for sampling stats endpoint
 */
public class RestKNNSamplingStatsHandler extends BaseRestHandler {
    // Constants for request parameters
    private static final String PARAM_INDEX = "index";
    private static final String PARAM_FIELD = "field";

    // Error messages
    private static final String ERROR_INDEX_REQUIRED = "Index name is required";
    private static final String ERROR_NO_STATS = "No stats found for index: ";
    private static final String ERROR_STATS_PROCESSING = "Failed to get stats: ";
    private static final String FIELD_INDEX_SUMMARY = "index_summary";
    private static final String FIELD_VECTOR_STATS = "vector_stats";
    private static final String FIELD_FIELD_STATS = "field_stats";
    private static final String FIELD_DIMENSIONS = "dimensions";
    private static final String FIELD_DIMENSION = "dimension";
    private static final String FIELD_COUNT = "count";
    private static final String FIELD_MIN = "min";
    private static final String FIELD_MAX = "max";
    private static final String FIELD_MEAN = "mean";
    private static final String FIELD_STD_DEV = "std_dev";
    private static final String FIELD_VARIANCE = "variance";

    // Service dependencies
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Environment environment;
    //private final IndicesService indicesService;  // Add this field


    /**
     * @return The name of this handler for internal reference
     */
    @Override
    public String getName() {
        return "knn_sampling_stats_action";
    }

    /**
     * Defines the REST endpoints this handler supports
     * @return List of supported routes
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, KNNPlugin.KNN_BASE_URI + "/sampling/{" + PARAM_INDEX + "}/stats"));
    }

    /**
     * Prepares and processes the REST request
     * @param request The incoming REST request
     * @param client Node client for executing requests
     * @return RestChannelConsumer to handle the response
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String indexName = request.param(PARAM_INDEX);
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException(ERROR_INDEX_REQUIRED);
        }
        final String fieldName = request.param(PARAM_FIELD);

        log.info("Received stats request for index: {}, field: {}", indexName, fieldName);

        return channel -> client.admin()
            .indices()
            .prepareStats(indexName)
            .clear()
            .setDocs(true)
            .setStore(true)
            .execute(new ActionListener<>() {
                @Override
                public void onResponse(IndicesStatsResponse response) {
                    onStatsResponse(response, indexName, fieldName, channel);
                }

                @Override
                public void onFailure(Exception e) {
                    onStatsFailure(e, channel);
                }
            });
    }

    /**
     * Processes the stats response
     * @param response The stats response
     * @param indexName The name of the index
     * @param fieldName Optional field name to filter by
     * @param channel The REST channel to send the response
     */
    private void onStatsResponse(IndicesStatsResponse response, String indexName, String fieldName, RestChannel channel) {
        try {
            log.info("Processing stats response for index: {}, field: {}", indexName, fieldName);
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();

            IndexStats indexStats = response.getIndex(indexName);
            if (indexStats != null) {
                buildStatsResponse(indexStats, fieldName, builder);
            } else {
                log.warn("No stats found for index: {}", indexName);
                builder.field("error", ERROR_NO_STATS + indexName);
            }

            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (Exception e) {
            log.error("Error processing stats response", e);
            onStatsFailure(e, channel);
        }
    }

    /**
     * Builds the statistics response using the new profiler state
     */
    private void buildStatsResponse(IndexStats indexStats, String fieldName, XContentBuilder builder) throws IOException {
        // Build index summary section
        builder.startObject(FIELD_INDEX_SUMMARY);
        builder.field("doc_count", indexStats.getTotal().getDocs().getCount());
        builder.field("size_in_bytes", indexStats.getTotal().getStore().getSizeInBytes());
        builder.endObject();

        // Start vector stats section
        builder.startObject(FIELD_VECTOR_STATS);

        // Get stats for each shard
        for (ShardStats shard : indexStats.getShards()) {
            try {
                IndexShard indexShard = indicesService.indexServiceSafe(shard.getShardRouting().shardId().getIndex())
                        .getShard(shard.getShardRouting().shardId().id());

                KNNIndexShard knnIndexShard = new KNNIndexShard(indexShard);

                // Get aggregate statistics for the shard
                Map<String, List<SummaryStatistics>> fieldStatistics = knnIndexShard.getAggregateStatistics();

                // If a specific field was requested, filter the results
                if (!Strings.isNullOrEmpty(fieldName)) {
                    if (fieldStatistics.containsKey(fieldName)) {
                        buildFieldStats(builder, fieldName, fieldStatistics.get(fieldName));
                    }
                } else {
                    // Build stats for all fields
                    builder.startObject(FIELD_FIELD_STATS);
                    for (Map.Entry<String, List<SummaryStatistics>> entry : fieldStatistics.entrySet()) {
                        buildFieldStats(builder, entry.getKey(), entry.getValue());
                    }
                    builder.endObject();
                }
            } catch (Exception e) {
                log.error("Error processing shard stats", e);
            }
        }

        builder.endObject(); // end vector_stats
    }

    /**
     * Builds statistics for a specific field
     */
    private void buildFieldStats(XContentBuilder builder, String fieldName, List<SummaryStatistics> stats) throws IOException {
        builder.startObject(fieldName);
        builder.startArray(FIELD_DIMENSIONS);

        for (int i = 0; i < stats.size(); i++) {
            SummaryStatistics stat = stats.get(i);
            builder.startObject()
                    .field(FIELD_DIMENSION, i)
                    .field(FIELD_COUNT, stat.getN())
                    .field(FIELD_MIN, SegmentProfilerState.formatDouble(stat.getMin()))
                    .field(FIELD_MAX, SegmentProfilerState.formatDouble(stat.getMax()))
                    .field(FIELD_MEAN, SegmentProfilerState.formatDouble(stat.getMean()))
                    .field(FIELD_STD_DEV, SegmentProfilerState.formatDouble(stat.getStandardDeviation()))
                    .field(FIELD_VARIANCE, SegmentProfilerState.formatDouble(stat.getVariance()))
                    .endObject();
        }

        builder.endArray();
        builder.endObject();
    }


    /**
     * Handles failures in processing the stats response
     * @param e The exception that occurred
     * @param channel The REST channel to send the error response
     */
    private void onStatsFailure(Exception e, RestChannel channel) {
        log.error("Failed to get stats", e);
        try {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field("error", ERROR_STATS_PROCESSING + e.getMessage());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
        } catch (IOException ex) {
            log.error("Failed to send error response", ex);
        }
    }
}
