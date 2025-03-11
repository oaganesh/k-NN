/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.knn.index.codec.KNN990Codec;

import lombok.AllArgsConstructor;
import lombok.Setter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.index.Index;
import org.opensearch.knn.common.KNNConstants;
import org.opensearch.knn.quantization.models.quantizationState.QuantizationState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

/**
 * Writes quantization states to off heap memory
 */
public final class KNN990QuantizationStateWriter {

    private final IndexOutput output;
    private List<FieldQuantizationState> fieldQuantizationStates = new ArrayList<>();
    static final String NATIVE_ENGINES_990_KNN_VECTORS_FORMAT_QS_DATA = "NativeEngines990KnnVectorsFormatQSData";

    private static final int DEFAULT_VECTOR_DIMENSIONS = 4;

    private double[] dimensionSums;
    private double[] dimensionSquaresSums;
    private int sampledCount = 0;
    private final double samplingRate = 0.15;
    private final Random random = new Random();

    public KNN990QuantizationStateWriter(SegmentWriteState segmentWriteState) throws IOException {
        this(segmentWriteState, DEFAULT_VECTOR_DIMENSIONS);
    }

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
    public KNN990QuantizationStateWriter(SegmentWriteState segmentWriteState, int vectorDimension) throws IOException {
        String quantizationStateFileName = IndexFileNames.segmentFileName(
            segmentWriteState.segmentInfo.name,
            segmentWriteState.segmentSuffix,
            KNNConstants.QUANTIZATION_STATE_FILE_SUFFIX
        );

        output = segmentWriteState.directory.createOutput(quantizationStateFileName, segmentWriteState.context);

        this.dimensionSums = new double[vectorDimension];
        this.dimensionSquaresSums = new double[vectorDimension];
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

    public void processVector(float[] vector) {
        if(random.nextDouble() < samplingRate) {
            for(int i = 0; i < vector.length; i++) {
                dimensionSums[i] += vector[i];
                dimensionSquaresSums[i] += vector[i] * vector[i];
            }
            sampledCount++;
        }
    }

    public void outputVectorStatsToConsole() {
        if(sampledCount == 0) {
            System.out.println("No vectors sampled.");
            return;
        }
        double[] means = new double[dimensionSums.length];
        double[] variances = new double[dimensionSums.length];
        for(int i = 0; i < dimensionSums.length; i++) {
            means[i] = dimensionSums[i] / sampledCount;
            double averageSquare = dimensionSquaresSums[i] / sampledCount;
            variances[i] = averageSquare - (means[i] * means[i]);
        }
        System.out.println("Processed " + sampledCount + " vectors (15% sample rate).");
        System.out.println("Per-dimension means: " + Arrays.toString(means));
        System.out.println("Per-dimension variances: " + Arrays.toString(variances));
    }

    /**
     * Writes index footer and other index information for parsing later
     * @throws IOException could be thrown while writing
     */
    public void writeFooter() throws IOException {
        long indexStartPosition = output.getFilePointer();
        output.writeInt(fieldQuantizationStates.size());
        for (FieldQuantizationState fieldQuantizationState : fieldQuantizationStates) {
            output.writeInt(fieldQuantizationState.fieldNumber);
            output.writeInt(fieldQuantizationState.stateBytes.length);
            output.writeVLong(fieldQuantizationState.position);
        }
        output.writeLong(indexStartPosition);
        output.writeInt(-1);
        CodecUtil.writeFooter(output);

        outputVectorStatsToConsole();
    }

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
}
