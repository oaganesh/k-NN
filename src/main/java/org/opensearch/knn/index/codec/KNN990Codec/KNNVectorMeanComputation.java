/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

public class KNNVectorMeanComputation implements Computation{

    @Override
    public float[] apply(float a, float b) {
        return new float[]{a + b};
    }

    @Override
    public float[] apply(float[] sum, long count) {
        float[] result = new float[sum.length];
        for (int i = 0; i < sum.length; i++) {
            result[i] = sum[i] / count;
        }
        return result;
    }
}



