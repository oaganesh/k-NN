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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.knn.index.codec.KNN990Codec.QuantizationConfigKNNCollector;
import org.opensearch.knn.index.quantizationservice.QuantizationService;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    /**
     * Gets aggregate statistics for a field across segments
     * @param indexReader The index reader containing all segments
     * @param fieldName Name of the field to get statistics for
     * @return Aggregated statistics for the field
     */
//    public static List<SummaryStatistics> getAggregateStatistics(IndexReader indexReader, String fieldName) throws IOException {
//        List<SummaryStatistics> allStats = new ArrayList<>();
//
//        for (LeafReaderContext leafContext : indexReader.leaves()) {
//            SegmentReader segmentReader = Lucene.segmentReader(leafContext.reader());
//
//            List<SummaryStatistics> segmentStats = collectStatisticsForSegment(segmentReader, fieldName);
//            if (segmentStats != null && !segmentStats.isEmpty()) {
//                allStats.addAll(segmentStats);
//            }
//        }
//        return aggregateStatistics(allStats);
//    }

    /**
     *  Helper method to collect statistics for a segment
     * @param reader
     * @param fieldName
     * @return
     * @throws IOException
     */
//    private static List<SummaryStatistics> collectStatisticsForSegment(SegmentReader reader, String fieldName) throws IOException {
//
//        SegmentInfo segmentInfo = reader.getSegmentInfo().info;
//        String segmentName = segmentInfo.name;
//
//        Directory dir = reader.directory();
//        String[] files = dir.listAll();
//
//        for (String file : files) {
//            if (file.startsWith(segmentName) && file.endsWith(SegmentProfilerState.VECTOR_STATS_EXTENSION)) {
//                Directory underlyingDir = SegmentProfilerState.getUnderlyingDirectory(dir);
//                if (underlyingDir instanceof FSDirectory) {
//                    Path dirPath = ((FSDirectory) underlyingDir).getDirectory();
//                    Path statsPath = dirPath.resolve(file);
//                    return SegmentProfilerState.readStatsFromFile(statsPath);
//                }
//            }
//        }
//
//        return new ArrayList<>();
//    }

    /**
     * Aggregates a list of summary statistics
     * @param statistics List of summary statistics
     * @return Aggregated summary statistics
     */
    public static List<SummaryStatistics> aggregateStatistics(List<SummaryStatistics> statistics) {
        if (statistics == null || statistics.isEmpty()) {
            return new ArrayList<>();
        }

        List<SummaryStatistics> result = new ArrayList<>(statistics.size());

        for (int i = 0; i < statistics.size(); i++) {
            Collection<StatisticalSummary> statCollection = Collections.singletonList(statistics.get(i));
            AggregateSummaryStatistics aggregator = new AggregateSummaryStatistics();
            aggregator.aggregate(statCollection);

            StatisticalSummary summary = aggregator.getSummary();
            SummaryStatistics aggregatedStats = new SummaryStatistics();

            if (summary.getN() > 0) {
                aggregatedStats.addValue(summary.getMin());
                if (summary.getN() > 1) {
                    aggregatedStats.addValue(summary.getMax());
                }
                if (summary.getN() > 2) {
                    double remainingMean = (summary.getSum() - summary.getMin() - summary.getMax()) / (summary.getN() - 2);
                    for (long j = 0; j < summary.getN() - 2; j++) {
                        aggregatedStats.addValue(remainingMean);
                    }
                }
            }

            result.add(aggregatedStats);
        }

        return result;
    }
}
