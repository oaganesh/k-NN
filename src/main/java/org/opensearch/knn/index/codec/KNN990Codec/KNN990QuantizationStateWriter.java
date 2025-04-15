/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import lombok.Setter;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes quantization states to off heap memory
 */
public final class KNN990QuantizationStateWriter {

    private final IndexOutput output;
    private List<FieldQuantizationState> fieldQuantizationStates = new ArrayList<>();
    private List<FieldProfilerState> fieldProfilerStates = new ArrayList<>();
    static final String NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_QS_DATA = "NativeEngines990KnnVectorsFormatQSData";

    /**
     * Constructor
     * Overall file format for writer:
     * Header
     * QS1 state bytes
     * QS2 state bytes
     * Number of quantization states
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
    public KNN990QuantizationStateWriter(SegmentWriteState segmentWriteState) throws IOException {
        String quantizationStateFileName = IndexFileNames.segmentFileName(
            segmentWriteState.segmentInfo.name,
            segmentWriteState.segmentSuffix,
            KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX
        );

        output = segmentWriteState.directory.createOutput(quantizationStateFileName, segmentWriteState.context);
    }

    /**
     * Writes an index header
     * @param segmentWriteState state containing segment information
     * @throws IOException exception could be thrown while writing header
     */
    public void writeHeader(SegmentWriteState segmentWriteState) throws IOException {
        CodecUtil.writeIndexHeader(
            output,
            NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_QS_DATA,
            0,
            segmentWriteState.segmentInfo.getId(),
            segmentWriteState.segmentSuffix
        );
    }

    /**
     * Writes a quantization state as bytes
     *
     * @param fieldNumber field number
     * @param quantizationState quantization state
     * @throws IOException could be thrown while writing
     */
    public void writeState(int fieldNumber, QuantizationState quantizationState) throws IOException {
        byte[] stateBytes = quantizationState.toByteArray();
        long position = output.getFilePointer();
        output.writeBytes(stateBytes, stateBytes.length);
        fieldQuantizationStates.add(new FieldQuantizationState(fieldNumber, stateBytes, position));
    }

    /**
     * Writes index footer and other index information for parsing later
     * @throws IOException could be thrown while writing
     */
//    public void writeFooter() throws IOException {
//        long indexStartPosition = output.getFilePointer();
//        output.writeInt(fieldQuantizationStates.size());
//        for (FieldQuantizationState fieldQuantizationState : fieldQuantizationStates) {
//            output.writeInt(fieldQuantizationState.fieldNumber);
//            output.writeInt(fieldQuantizationState.stateBytes.length);
//            output.writeVLong(fieldQuantizationState.position);
//        }
//        output.writeLong(indexStartPosition);
//        output.writeInt(-1);
//        CodecUtil.writeFooter(output);
//    }

    @AllArgsConstructor
    private static class FieldQuantizationState {
        final int fieldNumber;
        final byte[] stateBytes;
        @Setter
        Long position;
    }

    public void closeOutput() throws IOException {
        output.close();
    }

    @AllArgsConstructor
    private static class FieldProfilerState {
        final int fieldNumber;
        final byte[] stateBytes;
        @Setter
        Long position;
    }

    public void writeProfilerState(int fieldNumber, SegmentProfilerState profilerState) throws IOException {
        byte[] stateBytes = serializeProfilerState(profilerState);
        long position = output.getFilePointer();
        output.writeBytes(stateBytes, stateBytes.length);
        fieldProfilerStates.add(new FieldProfilerState(fieldNumber, stateBytes, position));
    }

    private byte[] serializeProfilerState(SegmentProfilerState profilerState) throws IOException {
        // Serialize statistics list to bytes
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            List<SummaryStatistics> stats = profilerState.getStatistics();
            dos.writeInt(stats.size());

            for (SummaryStatistics stat : stats) {
                dos.writeLong(stat.getN());
                dos.writeDouble(stat.getMin());
                dos.writeDouble(stat.getMax());
                dos.writeDouble(stat.getSum());
                dos.writeDouble(stat.getMean());
                dos.writeDouble(stat.getStandardDeviation());
                dos.writeDouble(stat.getVariance());
            }

            return baos.toByteArray();
        }
    }

    public void writeFooter() throws IOException {
        long indexStartPosition = output.getFilePointer();

        // Write quantization states
        output.writeInt(fieldQuantizationStates.size());
        for (FieldQuantizationState state : fieldQuantizationStates) {
            output.writeInt(state.fieldNumber);
            output.writeInt(state.stateBytes.length);
            output.writeVLong(state.position);
        }

        // Write profiler states
        output.writeInt(fieldProfilerStates.size());
        for (FieldProfilerState state : fieldProfilerStates) {
            output.writeInt(state.fieldNumber);
            output.writeInt(state.stateBytes.length);
            output.writeVLong(state.position);
        }

        output.writeLong(indexStartPosition);
        output.writeInt(-1);
        CodecUtil.writeFooter(output);
    }
}
