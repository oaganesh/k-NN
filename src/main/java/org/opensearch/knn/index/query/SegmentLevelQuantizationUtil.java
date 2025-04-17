/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.query;

import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.math3.stat.descriptive.AggregateSummaryStatistics;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.index.LeafReader;
import org.opensearch.knn.index.codec.KNN990Codec.QuantizationConfigKNNCollector;
import org.opensearch.knn.index.quantizationservice.QuantizationService;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * A utility class for doing Quantization related operation at a segment level. We can move this utility in {@link SegmentLevelQuantizationInfo}
 * but I am keeping it thinking that {@link SegmentLevelQuantizationInfo} free from these utility functions to reduce
 * the responsibilities of {@link SegmentLevelQuantizationInfo} class.
 */
@UtilityClass
@Log4j2
public class SegmentLevelQuantizationUtil {

    /**
     * A simple function to convert a vector to a quantized vector for a segment.
     * @param vector array of float
     * @return array of byte
     */
    @SuppressWarnings("unchecked")
    public static byte[] quantizeVector(final float[] vector, final SegmentLevelQuantizationInfo segmentLevelQuantizationInfo) {
        if (segmentLevelQuantizationInfo == null) {
            return null;
        }
        final QuantizationService quantizationService = QuantizationService.getInstance();
        // TODO: We are converting the output of Quantize to byte array for now. But this needs to be fixed when
        // other types of quantized outputs are returned like float[].
        return (byte[]) quantizationService.quantize(
            segmentLevelQuantizationInfo.getQuantizationState(),
            vector,
            quantizationService.createQuantizationOutput(segmentLevelQuantizationInfo.getQuantizationParams())
        );
    }

    /**
     * A utility function to get {@link QuantizationState} for a given segment and field.
     * @param leafReader {@link LeafReader}
     * @param fieldName {@link String}
     * @return {@link QuantizationState}
     * @throws IOException exception during reading the {@link QuantizationState}
     */
    static QuantizationState getQuantizationState(final LeafReader leafReader, String fieldName) throws IOException {
        final QuantizationConfigKNNCollector tempCollector = new QuantizationConfigKNNCollector();
        leafReader.searchNearestVectors(fieldName, new float[0], tempCollector, null);
        if (tempCollector.getQuantizationState() == null) {
            throw new IllegalStateException(String.format(Locale.ROOT, "No quantization state found for field %s", fieldName));
        }
        return tempCollector.getQuantizationState();
    }

//    public static List<SummaryStatistics> aggregateStatistics(List<List<SummaryStatistics>> segmentStats) {
//        if (segmentStats == null || segmentStats.isEmpty()) {
//            return new ArrayList<>();
//        }
//
//        int dimensions = segmentStats.get(0).size();
//        List<SummaryStatistics> result = new ArrayList<>(dimensions);
//
//        for (int dim = 0; dim < dimensions; dim++) {
//            Collection<StatisticalSummary> dimStats = new ArrayList<>();
//            for (List<SummaryStatistics> segmentStat : segmentStats) {
//                dimStats.add(segmentStat.get(dim));
//            }
//
//            AggregateSummaryStatistics aggregator = new AggregateSummaryStatistics();
//            StatisticalSummary summary = aggregator.aggregate(dimStats);
//
//            SummaryStatistics aggregatedStats = new SummaryStatistics();
//            if (summary.getN() > 0) {
//                aggregatedStats.addValue(summary.getMin());
//                if (summary.getN() > 1) {
//                    aggregatedStats.addValue(summary.getMax());
//                }
//                if (summary.getN() > 2) {
//                    double remainingMean = (summary.getSum() - summary.getMin() - summary.getMax()) / (summary.getN() - 2);
//                    for (long j = 0; j < summary.getN() - 2; j++) {
//                        aggregatedStats.addValue(remainingMean);
//                    }
//                }
//            }
//
//            result.add(aggregatedStats);
//        }
//
//        return result;
//    }

//    public static List<SummaryStatistics> aggregateStatistics(List<List<SummaryStatistics>> segmentStats) {
//        if (segmentStats == null || segmentStats.isEmpty()) {
//            return new ArrayList<>();
//        }
//
//        int dimensions = segmentStats.get(0).size();
//        List<SummaryStatistics> result = new ArrayList<>(dimensions);
//
//        for (int dim = 0; dim < dimensions; dim++) {
//            SummaryStatistics aggregated = new SummaryStatistics();
//
//            long totalN = 0;
//            double weightedMean = 0;
//            double weightedVarianceSum = 0;
//            double min = Double.MAX_VALUE;
//            double max = Double.MIN_VALUE;
//
//            for (List<SummaryStatistics> segmentStat : segmentStats) {
//                SummaryStatistics dimStat = segmentStat.get(dim);
//                if (dimStat.getN() > 0) {
//                    min = Math.min(min, dimStat.getMin());
//                    max = Math.max(max, dimStat.getMax());
//                    weightedMean += dimStat.getMean() * dimStat.getN();
//                    totalN += dimStat.getN();
//                }
//            }
//
//            if (totalN > 0) {
//                weightedMean /= totalN;
//
//                for (List<SummaryStatistics> segmentStat : segmentStats) {
//                    SummaryStatistics dimStat = segmentStat.get(dim);
//                    if (dimStat.getN() > 0) {
//                        double meanDiff = dimStat.getMean() - weightedMean;
//                        weightedVarianceSum += (dimStat.getN() - 1) * dimStat.getVariance() +
//                                dimStat.getN() * meanDiff * meanDiff;
//                    }
//                }
//
//                aggregated.addValue(min);
//                if (totalN > 1) {
//                    double stdDev = Math.sqrt(weightedVarianceSum / (totalN - 1));
//
//                    aggregated.addValue(max);
//
//                    if (totalN > 2) {
//                        for (int i = 0; i < totalN - 2; i++) {
//                            aggregated.addValue(weightedMean);
//                        }
//                    }
//                }
//            }
//
//            result.add(aggregated);
//        }
//
//        return result;
//    }

    public static List<SummaryStatistics> aggregateStatistics(List<List<SummaryStatistics>> segmentStats) {
        if (segmentStats == null || segmentStats.isEmpty()) {
            return new ArrayList<>();
        }

        int dimensions = segmentStats.get(0).size();
        List<SummaryStatistics> result = new ArrayList<>(dimensions);

        for (int dim = 0; dim < dimensions; dim++) {
            // Use AggregateSummaryStatistics to properly combine statistical moments
            AggregateSummaryStatistics aggregator = new AggregateSummaryStatistics();
            Collection<StatisticalSummary> dimStats = new ArrayList<>();

            for (List<SummaryStatistics> segmentStat : segmentStats) {
                if (dim < segmentStat.size()) {
                    dimStats.add(segmentStat.get(dim));
                }
            }

            StatisticalSummary summary = aggregator.aggregate(dimStats);

            SummaryStatistics aggregatedStats = new SummaryStatistics();
            long n = summary.getN();

            if (n > 0) {
                aggregatedStats.addValue(summary.getMin());

                if (n > 1) {
                    aggregatedStats.addValue(summary.getMax());

                    if (n > 2) {
                        // Calculate the values needed to match the original sum
                        double remainingSum = summary.getSum() - summary.getMin() - summary.getMax();
                        double remainingMean = remainingSum / (n - 2);

                        for (long i = 0; i < n - 2; i++) {
                            aggregatedStats.addValue(remainingMean);
                        }
                    }
                }
            }

            result.add(aggregatedStats);
        }

        return result;
    }
}
