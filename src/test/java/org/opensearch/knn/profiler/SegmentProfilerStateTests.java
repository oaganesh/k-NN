///*
// * Copyright OpenSearch Contributors
// * SPDX-License-Identifier: Apache-2.0
// */
//
//package org.opensearch.knn.profiler;
//
//import org.apache.lucene.search.DocIdSetIterator;
//import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
//import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
//import org.opensearch.test.OpenSearchTestCase;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.function.Supplier;
//
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//public class SegmentProfilerStateTests extends OpenSearchTestCase {
//
//    public void testProfileVectorsWithNoVectors() throws IOException {
//        KNNVectorValues<?> mockVectorValues = mock(KNNVectorValues.class);
//        when(mockVectorValues.nextDoc()).thenReturn(DocIdSetIterator.NO_MORE_DOCS);
//
//        Supplier<KNNVectorValues<?>> supplier = () -> mockVectorValues;
//        SegmentProfilerState profilerState = SegmentProfilerState.profileVectors(supplier);
//
//        assertTrue("Statistics list should be empty for no vectors", profilerState.getStatistics().isEmpty());
//    }
//
//    public void testProfileVectorsWithSingleVector() throws IOException {
//        KNNVectorValues<float[]> mockVectorValues = (KNNVectorValues<float[]>) mock(KNNVectorValues.class);
//        when(mockVectorValues.nextDoc())
//                .thenReturn(0)
//                .thenReturn(DocIdSetIterator.NO_MORE_DOCS);
//        when(mockVectorValues.getVector())
//                .thenReturn(new float[]{1.0f, 2.0f, 3.0f});
//
//        Supplier<KNNVectorValues<?>> supplier = () -> mockVectorValues;
//        SegmentProfilerState profilerState = SegmentProfilerState.profileVectors(supplier);
//
//        List<SummaryStatistics> statistics = profilerState.getStatistics();
//        assertEquals("Should have statistics for each dimension", 3, statistics.size());
//        assertEquals("Dimension 0 should have mean 1.0", 1.0, statistics.get(0).getMean(), 0.001);
//        assertEquals("Dimension 1 should have mean 2.0", 2.0, statistics.get(1).getMean(), 0.001);
//        assertEquals("Dimension 2 should have mean 3.0", 3.0, statistics.get(2).getMean(), 0.001);
//    }
//
//    public void testProfileVectorsWithMultipleVectors() throws IOException {
//        KNNVectorValues<float[]> mockVectorValues = (KNNVectorValues<float[]>) mock(KNNVectorValues.class);
//        when(mockVectorValues.nextDoc())
//                .thenReturn(0)
//                .thenReturn(1)
//                .thenReturn(2)
//                .thenReturn(DocIdSetIterator.NO_MORE_DOCS);
//        when(mockVectorValues.getVector())
//                .thenReturn(new float[]{1.0f, 2.0f})
//                .thenReturn(new float[]{3.0f, 4.0f})
//                .thenReturn(new float[]{5.0f, 6.0f});
//
//        Supplier<KNNVectorValues<?>> supplier = () -> mockVectorValues;
//        SegmentProfilerState profilerState = SegmentProfilerState.profileVectors(supplier);
//
//        List<SummaryStatistics> statistics = profilerState.getStatistics();
//        assertEquals("Should have statistics for each dimension", 2, statistics.size());
//        assertEquals("Dimension 0 should have mean 3.0", 3.0, statistics.get(0).getMean(), 0.001);
//        assertEquals("Dimension 1 should have mean 4.0", 4.0, statistics.get(1).getMean(), 0.001);
//        assertEquals("Dimension 0 should have min 1.0", 1.0, statistics.get(0).getMin(), 0.001);
//        assertEquals("Dimension 1 should have max 6.0", 6.0, statistics.get(1).getMax(), 0.001);
//    }
//
////    public void testProfileVectorsWithInvalidDimensions() throws IOException {
////        KNNVectorValues<float[]> mockVectorValues = (KNNVectorValues<float[]>) mock(KNNVectorValues.class);
////        when(mockVectorValues.nextDoc())
////                .thenReturn(0)
////                .thenReturn(1)
////                .thenReturn(DocIdSetIterator.NO_MORE_DOCS);
////        when(mockVectorValues.getVector())
////                .thenReturn(new float[]{1.0f, 2.0f})
////                .thenReturn(new float[]{3.0f}); // Invalid dimension
////
////        Supplier<KNNVectorValues<?>> supplier = () -> mockVectorValues;
////
////        expectThrows(ArrayIndexOutOfBoundsException.class, () -> {
////            SegmentProfilerState.profileVectors(supplier);
////        });
////    }
//
//    public void testProfileVectorsStatisticalAccuracy() throws IOException {
//        KNNVectorValues<float[]> mockVectorValues = (KNNVectorValues<float[]>) mock(KNNVectorValues.class);
//        when(mockVectorValues.nextDoc())
//                .thenReturn(0)
//                .thenReturn(1)
//                .thenReturn(2)
//                .thenReturn(DocIdSetIterator.NO_MORE_DOCS);
//        when(mockVectorValues.getVector())
//                .thenReturn(new float[]{1.0f, 1.0f})
//                .thenReturn(new float[]{2.0f, 2.0f})
//                .thenReturn(new float[]{3.0f, 3.0f});
//
//        Supplier<KNNVectorValues<?>> supplier = () -> mockVectorValues;
//        SegmentProfilerState profilerState = SegmentProfilerState.profileVectors(supplier);
//
//        List<SummaryStatistics> statistics = profilerState.getStatistics();
//        for (int i = 0; i < 2; i++) {
//            SummaryStatistics stats = statistics.get(i);
//            assertEquals("Mean should be 2.0", 2.0, stats.getMean(), 0.001);
//            assertEquals("Standard deviation should be 1.0", 1.0, stats.getStandardDeviation(), 0.001);
//            assertEquals("Min should be 1.0", 1.0, stats.getMin(), 0.001);
//            assertEquals("Max should be 3.0", 3.0, stats.getMax(), 0.001);
//            assertEquals("Count should be 3", 3, stats.getN());
//        }
//    }
//}