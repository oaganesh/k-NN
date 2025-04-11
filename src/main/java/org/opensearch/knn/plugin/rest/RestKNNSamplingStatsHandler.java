/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.plugin.rest;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
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
import org.opensearch.knn.plugin.KNNPlugin;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

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

    // Service dependencies
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Environment environment;

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
        return List.of(
                new Route(GET, KNNPlugin.KNN_BASE_URI + "/sampling/{" + PARAM_INDEX + "}/stats")
        );
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

        // Optional field parameter to filter by specific vector field
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
    private void onStatsResponse(
            IndicesStatsResponse response,
            String indexName,
            String fieldName,
            RestChannel channel
    ) {
        try {
            log.info("Received stats response for index: {}, field: {}", indexName, fieldName);
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();

            IndexStats indexStats = response.getIndex(indexName);
            if (indexStats != null) {
                log.info("Processing index stats for: {}", indexName);
                SegmentProfilerState.getIndexStats(indexStats, builder, environment);
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
