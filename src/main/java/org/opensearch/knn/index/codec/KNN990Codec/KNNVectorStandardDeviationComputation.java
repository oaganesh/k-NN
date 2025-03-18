/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

public class KNNVectorStandardDeviationComputation implements Computation {
    private final KNNVectorVarianceComputation varianceComputation;

    public KNNVectorStandardDeviationComputation(float[] meanVector) {
        this.varianceComputation = new KNNVectorVarianceComputation(meanVector);
    }

    @Override
    public float[] apply(float a, float b) {
        return varianceComputation.apply(a, b);
    }

    @Override
    public float[] apply(float[] sum, long count) {
        float[] variance = varianceComputation.apply(sum, count);
        float[] result = new float[variance.length];
        for (int i = 0; i < variance.length; i++) {
            result[i] = (float) Math.sqrt(variance[i]);
        }
        return result;
    }
}
