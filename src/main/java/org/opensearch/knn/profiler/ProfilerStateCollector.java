/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;

public class ProfilerStateCollector implements KnnCollector {
    private SegmentProfilerState profilerState;

    public SegmentProfilerState getProfilerState() {
        return profilerState;
    }

    public void setProfilerState(SegmentProfilerState state) {
        this.profilerState = state;
    }

    /**
     * @return
     */
    @Override
    public boolean earlyTerminated() {
        return false;
    }

    /**
     * @param i
     */
    @Override
    public void incVisitedCount(int i) {

    }

    /**
     * @return
     */
    @Override
    public long visitedCount() {
        return 0;
    }

    /**
     * @return
     */
    @Override
    public long visitLimit() {
        return 0;
    }

    /**
     * @return
     */
    @Override
    public int k() {
        return 0;
    }

    @Override
    public boolean collect(int doc, float score) {
        // Implementation not needed for this use case
        return false;
    }

    /**
     * @return
     */
    @Override
    public float minCompetitiveSimilarity() {
        return 0;
    }

    /**
     * @return
     */
    @Override
    public TopDocs topDocs() {
        return null;
    }

    // Other required KnnCollector methods...
}