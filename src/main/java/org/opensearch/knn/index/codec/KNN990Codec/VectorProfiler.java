/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import java.util.Arrays;
import java.util.Collection;

public class VectorProfiler {
        /**
         * Calculates and returns the mean vector for a collection of float vectors
         *
         * @param vectors Collection of float arrays to analyze
         * @return mean vector as float array
         */
        public static float[] calculateMeanVector(Collection<float[]> vectors) {
            if (vectors == null || vectors.isEmpty()) {
                throw new IllegalArgumentException("Vectors collection cannot be null or empty");
            }

            float[] firstVector = vectors.iterator().next();
            int dim = firstVector.length;
            float[] meanVector = new float[dim];
            int count = vectors.size();

            Arrays.fill(meanVector, 0);

            for (float[] vec : vectors) {
                if (vec.length != dim) {
                    throw new IllegalArgumentException("All vectors must have the same dimension");
                }
                for (int i = 0; i < dim; i++) {
                    meanVector[i] += vec[i];
                }
            }

            for (int i = 0; i < dim; i++) {
                meanVector[i] /= count;
            }

            return meanVector;
        }

        /**
         * Prints the mean vector statistics
         *
         * @param meanVector The calculated mean vector
         */
        public static void printMeanVectorStats(float[] meanVector) {
            System.out.println("Per-dimension mean: " + Arrays.toString(meanVector));
        }
}
