/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.knn.index.vectorvalues.KNNVectorValues;
import org.opensearch.test.OpenSearchTestCase;
import lombok.SneakyThrows;
import org.apache.lucene.search.Sort;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentProfilerStateTests extends OpenSearchTestCase {

    @Test
    public void testProcessVectorWithFloatArray() {
        List<SummaryStatistics> stats = new ArrayList<>();
        stats.add(new SummaryStatistics());
        stats.add(new SummaryStatistics());

        float[] vector = { 1.5f, 2.8f };
        SegmentProfilerState.processVector(vector, stats);

        assertEquals(1.5, stats.get(0).getMin(), 0.001);
        assertEquals(2.8, stats.get(1).getMin(), 0.001);
        assertEquals(1, stats.get(0).getN());
    }

    @Test
    public void testProcessVectorWithByteArray() {
        List<SummaryStatistics> stats = new ArrayList<>();
        stats.add(new SummaryStatistics());
        stats.add(new SummaryStatistics());

        byte[] vector = { (byte) 100, (byte) 200 };
        SegmentProfilerState.processVector(vector, stats);

        assertEquals(100.0, stats.get(0).getMin(), 0.001);
        assertEquals(200.0, stats.get(1).getMax(), 0.001);
    }

    @Test
    @SneakyThrows
    public void testProfileVectorsWritesStatsFile() {
        KNNVectorValues<Object> mockVectorValues = mock(KNNVectorValues.class);
        when(mockVectorValues.dimension()).thenReturn(2);
        when(mockVectorValues.docId()).thenReturn(0, NO_MORE_DOCS);
        when(mockVectorValues.getVector()).thenReturn(new float[] { 1.0f, 2.0f });
        Supplier<KNNVectorValues<?>> supplier = () -> mockVectorValues;

        Path tempDir = createTempDir();
        try (FSDirectory directory = FSDirectory.open(tempDir)) {
            SegmentInfo segmentInfo = new SegmentInfo(
                    directory,
                    Version.LATEST,
                    Version.LATEST,
                    "test_segment",
                    1,
                    false,
                    false,
                    Mockito.mock(Codec.class),
                    Map.of(),
                    new byte[16],
                    Map.of(),
                    Sort.RELEVANCE
            );

            SegmentWriteState state = new SegmentWriteState(
                    null,
                    directory,
                    segmentInfo,
                    new FieldInfos(new FieldInfo[0]),
                    null,
                    IOContext.DEFAULT
            );

            SegmentProfilerState profilerState = SegmentProfilerState.profileVectors(supplier, state, "test_field");

            String statsFileName = IndexFileNames.segmentFileName(
                    "test_segment",
                    state.segmentSuffix,
                    SegmentProfilerState.VECTOR_STATS_EXTENSION
            );

            assertTrue("Stats file should exist", Files.exists(tempDir.resolve(statsFileName)));
        }
    }

    @Test
    public void testGetUnderlyingDirectory() throws IOException {
        FSDirectory fsDir = mock(FSDirectory.class);
        FilterDirectory filterDir = new FilterDirectory(fsDir) {
        };
        Directory result = SegmentProfilerState.getUnderlyingDirectory(filterDir);
        assertEquals(fsDir, result);
    }

    @Test(expected = IOException.class)
    public void testGetUnderlyingDirectoryThrowsForNonFSDirectory() throws IOException {
        Directory dir = mock(Directory.class);
        SegmentProfilerState.getUnderlyingDirectory(dir);
    }
}
