/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.profiler.SegmentProfileStateReadConfig;
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.knn.quantization.enums.ScalarQuantizationType;
import org.opensearch.knn.quantization.models.quantizationParams.ScalarQuantizationParams;
import org.opensearch.knn.quantization.models.quantizationState.MultiBitScalarQuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.OneBitScalarQuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationStateReadConfig;

import java.io.IOException;

import static org.opensearch.knn.index.codec.KNN990Codec.KNN990QuantizationStateWriter.NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_QS_DATA;

/**
 * Reads quantization states and segment profiler states
 */
@Log4j2
public final class KNN990QuantizationStateReader {

    private static final byte QUANTIZATION_STATE_MARKER = 0;
    private static final byte SEGMENT_PROFILER_STATE_MARKER = 1;

    /**
     * Reads a quantization state for a given field
     *
     * @param readConfig config for reading the quantization state
     * @return QuantizationState object
     */
    public static QuantizationState readQuantizationState(QuantizationStateReadConfig readConfig) throws IOException {
        SegmentReadState segmentReadState = readConfig.getSegmentReadState();
        String field = readConfig.getField();
        String stateFileName = getStateFileName(segmentReadState);
        int fieldNumber = segmentReadState.fieldInfos.fieldInfo(field).getFieldNumber();

        try (IndexInput input = segmentReadState.directory.openInput(stateFileName, IOContext.READONCE)) {
            CodecUtil.retrieveChecksum(input);
            int numFields = getNumFields(input);

            long position = -1;
            int length = 0;

            for (int i = 0; i < numFields; i++) {
                int tempFieldNumber = input.readInt();
                byte stateType = input.readByte();
                int tempLength = input.readInt();
                long tempPosition = input.readVLong();
                if (tempFieldNumber == fieldNumber && stateType == QUANTIZATION_STATE_MARKER) {
                    position = tempPosition;
                    length = tempLength;
                    break;
                }
            }

            if (position == -1 || length == 0) {
                throw new IllegalArgumentException(String.format("Quantization state for field %s not found", field));
            }

            byte[] stateBytes = readStateBytes(input, position, length);
            return deserializeQuantizationState(stateBytes, readConfig);
        }
    }

    /**
     * Reads a segment profiler state for a given field
     *
     * @param readConfig config for reading the profiler state
     * @return SegmentProfilerState object
     */
    public static SegmentProfilerState readProfilerState(SegmentProfileStateReadConfig readConfig) throws IOException {
        SegmentReadState segmentReadState = readConfig.getSegmentReadState();
        String field = readConfig.getField();
        String stateFileName = getStateFileName(segmentReadState);
        int fieldNumber = segmentReadState.fieldInfos.fieldInfo(field).getFieldNumber();

        try (IndexInput input = segmentReadState.directory.openInput(stateFileName, IOContext.DEFAULT)) {
            CodecUtil.checkHeader(input, NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_QS_DATA, 0, 0);

            long footerStart = input.length() - CodecUtil.footerLength();
            long markerAndIndexPosition = footerStart - Integer.BYTES - Long.BYTES;
            input.seek(markerAndIndexPosition);
            long indexStartPosition = input.readLong();
            input.seek(indexStartPosition);

            int numFields = input.readInt();
            log.debug("Number of fields in state file: {}", numFields);

            long position = -1;
            int length = 0;
            byte stateType = -1;

            for (int i = 0; i < numFields; i++) {
                int tempFieldNumber = input.readInt();
                byte tempStateType = input.readByte();
                int tempLength = input.readInt();
                long tempPosition = input.readVLong();

                log.debug("Field: {}, StateType: {}, Length: {}, Position: {}",
                        tempFieldNumber, tempStateType, tempLength, tempPosition);

                if (tempFieldNumber == fieldNumber) {
                    position = tempPosition;
                    length = tempLength;
                    stateType = tempStateType;
                    break;
                }
            }

            if (position == -1 || length == 0) {
                throw new IllegalArgumentException(String.format("Profile state for field %s not found", field));
            }

            log.debug("Found state for field {}: Position: {}, Length: {}, StateType: {}",
                    field, position, length, stateType);

            input.seek(position);
            byte marker = input.readByte();
            log.debug("Read marker byte: {}, Expected: {}", marker, SEGMENT_PROFILER_STATE_MARKER);

            if (marker != SEGMENT_PROFILER_STATE_MARKER) {
                throw new IllegalStateException("Invalid state marker: expected " +
                        SEGMENT_PROFILER_STATE_MARKER + ", but got " + marker);
            }

            byte[] stateBytes = new byte[length - 1];
            input.readBytes(stateBytes, 0, length - 1);

            log.debug("Read {} bytes for state", stateBytes.length);

            return SegmentProfilerState.fromBytes(stateBytes);
        }
    }


    private static QuantizationState deserializeQuantizationState(byte[] stateBytes, QuantizationStateReadConfig readConfig) throws IOException {
        ScalarQuantizationType scalarQuantizationType = ((ScalarQuantizationParams) readConfig.getQuantizationParams()).getSqType();
        switch (scalarQuantizationType) {
            case ONE_BIT:
                return OneBitScalarQuantizationState.fromByteArray(stateBytes);
            case TWO_BIT:
            case FOUR_BIT:
                return MultiBitScalarQuantizationState.fromByteArray(stateBytes);
            default:
                throw new IllegalArgumentException(String.format("Unexpected scalar quantization type: %s", scalarQuantizationType));
        }
    }

    @VisibleForTesting
    static int getNumFields(IndexInput input) throws IOException {
        long footerStart = input.length() - CodecUtil.footerLength();
        long markerAndIndexPosition = footerStart - Integer.BYTES - Long.BYTES;
        input.seek(markerAndIndexPosition);
        long indexStartPosition = input.readLong();
        input.seek(indexStartPosition);
        return input.readInt();
    }

    @VisibleForTesting
    static byte[] readStateBytes(IndexInput input, long position, int length) throws IOException {
        input.seek(position);
        byte stateType = input.readByte(); // Read and discard the state type marker
        byte[] stateBytes = new byte[length - 1]; // Subtract 1 for the state type marker
        input.readBytes(stateBytes, 0, length - 1);
        return stateBytes;
    }

    @VisibleForTesting
    static String getStateFileName(SegmentReadState state) {
        return IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX);
    }
}
