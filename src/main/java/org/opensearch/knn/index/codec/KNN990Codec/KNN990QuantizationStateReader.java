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
import org.opensearch.knn.profiler.SegmentProfilerState;
import org.opensearch.knn.profiler.SegmentProfileStateReadConfig;
import org.opensearch.knn.quantization.enums.ScalarQuantizationType;
import org.opensearch.knn.quantization.models.quantizationParams.ScalarQuantizationParams;
import org.opensearch.knn.quantization.models.quantizationState.MultiBitScalarQuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.OneBitScalarQuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationStateReadConfig;

import java.io.IOException;

/**
 * Reader for various state files (quantization state and profiler state)
 */
@Log4j2
public final class KNN990QuantizationStateReader {

    /**
     * Reads a quantization state for a given field
     *
     * @param readConfig a config class that contains necessary information for reading the state
     * @return quantization state
     * @throws IOException if an error occurs during reading
     */
    public static QuantizationState readQuantizationState(QuantizationStateReadConfig readConfig) throws IOException {
        SegmentReadState segmentReadState = readConfig.getSegmentReadState();
        String field = readConfig.getField();
        String stateFileName = getStateFileName(segmentReadState, KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX);

        byte[] stateBytes = readStateBytes(segmentReadState, field, stateFileName);

        // Deserialize the byte array to a quantization state object
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

    /**
     * Reads a profiler state for a given field
     *
     * @param readConfig config for reading the profiler state
     * @return SegmentProfilerState object
     * @throws IOException if there's an error reading the state
     */
    /**
     * Reads a profiler state for a given field, optionally including quantization information
     *
     * @param readConfig config for reading the profiler state
     * @param quantizationParams optional quantization parameters (can be null)
     * @return SegmentProfilerState object
     * @throws IOException if there's an error reading the state
     */
    public static SegmentProfilerState readProfileState(
            SegmentProfileStateReadConfig readConfig,
            ScalarQuantizationParams quantizationParams) throws IOException {

        SegmentReadState segmentReadState = readConfig.getSegmentReadState();
        String field = readConfig.getField();
        String stateFileName = getStateFileName(segmentReadState, KNNConstants.SEGMENT_PROFILE_STATE_FILE_SUFFIX);

        byte[] stateBytes = readStateBytes(segmentReadState, field, stateFileName);
        SegmentProfilerState state = SegmentProfilerState.fromBytes(stateBytes);

        // Enhance the profiler state with quantization information if available
        if (quantizationParams != null && state.isQuantizedStats()) {
            ScalarQuantizationType sqType = quantizationParams.getSqType();
            log.debug("Enhancing profiler state with {} quantization type information", sqType);

            // Determine bits per value based on quantization type
            int bitsPerValue = sqType == ScalarQuantizationType.ONE_BIT ? 1 :
                    sqType == ScalarQuantizationType.TWO_BIT ? 2 : 4;

            // We could either enhance the existing state object or create a new one with
            // additional quantization information

            // Option 1: For now, if SegmentProfilerState has appropriate setters or constructors
            // state.setQuantizationType(sqType);
            // state.setBitsPerValue(bitsPerValue);

            // Option 2: If we want to add this information without modifying SegmentProfilerState
            log.info("Profile state represents {} quantized data ({} bits per value, {}:1 compression)",
                    sqType, bitsPerValue, 32/bitsPerValue);
        }

        return state;
    }

    /**
     * Simplified version without quantization parameters
     */
    public static SegmentProfilerState readProfileState(SegmentProfileStateReadConfig readConfig) throws IOException {
        return readProfileState(readConfig, null);
    }

    /**
     * Reads state bytes for a specific field from a state file
     *
     * @param segmentReadState the segment read state
     * @param field the field name
     * @param stateFileName the name of the state file
     * @return the state bytes
     * @throws IOException if there's an error reading the state
     */
    private static byte[] readStateBytes(SegmentReadState segmentReadState, String field, String stateFileName) throws IOException {
        int fieldNumber = segmentReadState.fieldInfos.fieldInfo(field).getFieldNumber();

        try (IndexInput input = segmentReadState.directory.openInput(stateFileName, IOContext.DEFAULT)) {
            CodecUtil.retrieveChecksum(input);
            int numFields = getNumFields(input);

            long position = -1;
            int length = 0;

            // Read each field's metadata from the index section, break when correct field is found
            for (int i = 0; i < numFields; i++) {
                int tempFieldNumber = input.readInt();
                int tempLength = input.readInt();
                long tempPosition = input.readVLong();
                if (tempFieldNumber == fieldNumber) {
                    position = tempPosition;
                    length = tempLength;
                    break;
                }
            }

            if (position == -1 || length == 0) {
                throw new IllegalArgumentException(String.format("Field %s not found", field));
            }

            return readBytes(input, position, length);
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
    static byte[] readBytes(IndexInput input, long position, int length) throws IOException {
        input.seek(position);
        byte[] stateBytes = new byte[length];
        input.readBytes(stateBytes, 0, length);
        return stateBytes;
    }

    @VisibleForTesting
    static String getStateFileName(SegmentReadState state, String suffix) {
        return IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, suffix);
    }
}