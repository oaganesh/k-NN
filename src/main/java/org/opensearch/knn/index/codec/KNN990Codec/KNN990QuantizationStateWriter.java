/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
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
 * Writes quantization states to off heap memory
 */
@Log4j2
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

    public void writeProfilerState(int fieldNumber, SegmentProfilerState profilerState) throws IOException {
        log.info("[KNN Stats] Writing profiler state for field number: {}", fieldNumber);
        try {
            byte[] stateBytes = profilerState.toByteArray();
            long position = output.getFilePointer();
            output.writeBytes(stateBytes, 0, stateBytes.length);
            fieldProfilerStates.add(new FieldProfilerState(fieldNumber, stateBytes, position));
            log.info("[KNN Stats] Successfully wrote profiler state of {} bytes at position {}", stateBytes.length, position);
        } catch (Exception e) {
            log.error("[KNN Stats] Error writing profiler state: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * Writes index footer and other index information for parsing later
     * @throws IOException could be thrown while writing
     */

    public void writeFooter() throws IOException {
        long indexStartPosition = output.getFilePointer();
        log.info("[KNN Stats] Writing footer starting at position: {}", indexStartPosition);

        output.writeInt(fieldQuantizationStates.size());
        log.info("[KNN Stats] Writing {} quantization states", fieldQuantizationStates.size());
        for (FieldQuantizationState state : fieldQuantizationStates) {
            output.writeInt(state.fieldNumber);
            output.writeInt(state.stateBytes.length);
            output.writeVLong(state.position);
        }

        output.writeInt(fieldProfilerStates.size());
        log.info("[KNN Stats] Writing {} profiler states", fieldProfilerStates.size());
        for (FieldProfilerState state : fieldProfilerStates) {
            output.writeInt(state.fieldNumber);
            output.writeInt(state.stateBytes.length);
            output.writeVLong(state.position);
            log.info(
                "[KNN Stats] Writing index for profiler state: field={}, length={}, position={}",
                state.fieldNumber,
                state.stateBytes.length,
                state.position
            );
        }

        output.writeLong(indexStartPosition);
        output.writeInt(-1);
        log.info("[KNN Stats] Wrote footer index position: {} and marker: -1", indexStartPosition);
        CodecUtil.writeFooter(output);
    }

    @AllArgsConstructor
    private static class FieldQuantizationState {
        final int fieldNumber;
        final byte[] stateBytes;
        @Setter
        Long position;
    }

    @AllArgsConstructor
    private static class FieldProfilerState {
        final int fieldNumber;
        final byte[] stateBytes;
        @Setter
        Long position;
    }

    public void closeOutput() throws IOException {
        output.close();
    }
}
