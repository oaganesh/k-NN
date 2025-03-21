/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

public interface Computation {
    float[] apply(float a, float b);

    float[] apply(float[] sum, long count);
}
