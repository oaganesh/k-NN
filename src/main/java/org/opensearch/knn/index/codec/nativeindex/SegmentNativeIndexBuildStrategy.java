/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.nativeindex;

import org.opensearch.knn.profiler.SegmentBuildIndexParams;

import java.io.IOException;

public interface SegmentNativeIndexBuildStrategy {
    void buildAndWriteSegmentIndex(SegmentBuildIndexParams indexInfo) throws IOException;
}
