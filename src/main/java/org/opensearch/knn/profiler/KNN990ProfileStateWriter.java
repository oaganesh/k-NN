/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.profiler;

import lombok.AllArgsConstructor;
import lombok.Setter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes segment states to off heap memory
 */
public final class KNN990ProfileStateWriter {

    private final IndexOutput output;
    private List<FieldSegmentState> fieldSegmentStates = new ArrayList<>();
    static final String NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_SS_DATA = "NativeEngines990KnnVectorsFormatSSData";

    /**
     * Constructor
     * Overall file format for writer:
     * Header
     * QS1 state bytes
     * QS2 state bytes
     * Number of segment states
     * QS1 field number
     * QS1 state bytes length
     * QS1 position of state bytes
     * QS2 field number
     * QS2 state bytes length
     * QS2 position of state bytes
     * Position of index section (where QS1 field name is located)
     * -1 (marker)
     * Footer
     * @param segmentWriteState segment write state containing segment information
     * @throws IOException exception could be thrown while creating the output
     */
    public KNN990ProfileStateWriter(SegmentWriteState segmentWriteState) throws IOException {
        String segmentStateFileName = IndexFileNames.segmentFileName(
                segmentWriteState.segmentInfo.name,
                segmentWriteState.segmentSuffix,
                KNNConstants.SEGMENT_PROFILE_STATE_FILE_SUFFIX
        );

        output = segmentWriteState.directory.createOutput(segmentStateFileName, segmentWriteState.context);
    }

    /**
     * Writes an index header
     * @param segmentWriteState state containing segment information
     * @throws IOException exception could be thrown while writing header
     */
    public void writeHeader(SegmentWriteState segmentWriteState) throws IOException {
        CodecUtil.writeIndexHeader(
                output,
                NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_SS_DATA,
                0,
                segmentWriteState.segmentInfo.getId(),
                segmentWriteState.segmentSuffix
        );
    }

    /**
     * Writes a segment profile state as bytes
     *
     * @param fieldNumber field number
     * @param segmentProfilerState segment profiler state
     * @throws IOException could be thrown while writing
     */
    public void writeState(int fieldNumber, SegmentProfilerState segmentProfilerState) throws IOException {
        byte[] stateBytes = segmentProfilerState.toByteArray();
        long position = output.getFilePointer();
        output.writeBytes(stateBytes, stateBytes.length);
        fieldSegmentStates.add(new FieldSegmentState(fieldNumber, stateBytes, position));
    }

    /**
     * Writes index footer and other index information for parsing later
     * @throws IOException could be thrown while writing
     */
    public void writeFooter() throws IOException {
        long indexStartPosition = output.getFilePointer();
        output.writeInt(fieldSegmentStates.size());
        for (FieldSegmentState fieldSegmentState : fieldSegmentStates) {
            output.writeInt(fieldSegmentState.fieldNumber);
            output.writeInt(fieldSegmentState.stateBytes.length);
            output.writeVLong(fieldSegmentState.position);
        }
        output.writeLong(indexStartPosition);
        output.writeInt(-1);
        CodecUtil.writeFooter(output);
    }

    @AllArgsConstructor
    private static class FieldSegmentState {
        final int fieldNumber;
        final byte[] stateBytes;
        @Setter
        Long position;
    }

    public void closeOutput() throws IOException {
        output.close();
    }
}
