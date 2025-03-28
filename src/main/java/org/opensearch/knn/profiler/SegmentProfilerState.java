/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.opensearch.knn.index.codec.util.KNNCodecUtil;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.opensearch.knn.index.codec.util.KNNCodecUtil.initializeVectorValues;

/**
 * Class to profile segment vectors and list summary statistics for each dimension of the vectors
 */
@Log4j2
public class SegmentProfilerState {
    @Getter
    private final List<SummaryStatistics> statistics;

    /**
     * Constructor to initialize the SegmentProfilerState
     * @param statistics
     */
    public SegmentProfilerState(final List<SummaryStatistics> statistics) {
        this.statistics = statistics;
    }

    /**
     * Profiles vectors in a segment by analyzing their statistical values
     * @param knnVectorValuesSupplier
     * @return
     * @throws IOException
     */
    public static SegmentProfilerState profileVectors(final Supplier<KNNVectorValues<?>> knnVectorValuesSupplier) throws IOException {
        KNNVectorValues<?> vectorValues = knnVectorValuesSupplier.get();

        if (vectorValues == null) {
            log.info("No vector values available");
            return new SegmentProfilerState(new ArrayList<>());
        }

        KNNCodecUtil.initializeVectorValues(vectorValues);
        List<SummaryStatistics> statistics = new ArrayList<>();

        // Get first vector to determine dimension
        int doc = vectorValues.nextDoc();
        if (doc == NO_MORE_DOCS) {
            log.info("No vectors to profile");
            return new SegmentProfilerState(statistics);
        }

        float[] firstVector = (float[]) vectorValues.getVector();
        int dimension = firstVector.length;
        log.info("Starting vector profiling with dimension: {}", dimension);

        // Initialize statistics based on first vector's dimension
        for (int i = 0; i < dimension; i++) {
            statistics.add(new SummaryStatistics());
        }

        // Process first vector
        for (int j = 0; j < firstVector.length; j++) {
            statistics.get(j).addValue(firstVector[j]);
        }

        // Process remaining vectors
        int vectorCount = 1;
        while (vectorValues.nextDoc() != NO_MORE_DOCS) {
            vectorCount++;
            float[] vector = (float[]) vectorValues.getVector();
            for (int j = 0; j < vector.length; j++) {
                statistics.get(j).addValue(vector[j]);
            }
        }

        // Log detailed statistics
        log.info("Vector profiling completed - processed {} vectors with {} dimensions", vectorCount, dimension);
        for (int i = 0; i < dimension; i++) {
            SummaryStatistics stats = statistics.get(i);
            log.info("Dimension {} stats: mean={}, std={}, min={}, max={}",
                    i,
                    stats.getMean(),
                    stats.getStandardDeviation(),
                    stats.getMin(),
                    stats.getMax()
            );
        }

        return new SegmentProfilerState(statistics);
    }
}