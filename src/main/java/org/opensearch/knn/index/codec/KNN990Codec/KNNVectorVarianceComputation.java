/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

public class KNNVectorVarianceComputation implements Computation {
    private final float[] meanVector;

    public KNNVectorVarianceComputation(float[] meanVector) {
        this.meanVector = meanVector;
    }

    @Override
    public float[] apply(float a, float b) {
        float diff = b - meanVector[0];
        return new float[]{a + (diff * diff)};
    }

    @Override
    public float[] apply(float[] sum, long count) {
        float[] result = new float[sum.length];
        for (int i = 0; i < sum.length; i++) {
            result[i] = sum[i] / (count - 1);
        }
        return result;
    }
}

