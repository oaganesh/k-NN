///*
// * Copyright OpenSearch Contributors
// * SPDX-License-Identifier: Apache-2.0
// */
//
//package org.opensearch.knn.profiler;
//
//import org.apache.lucene.codecs.CodecUtil;
//import org.apache.lucene.index.IndexFileNames;
//import org.apache.lucene.index.SegmentReadState;
//import org.apache.lucene.index.SegmentWriteState;
//import org.apache.lucene.store.IOContext;
//import org.apache.lucene.store.IndexInput;
//import org.apache.lucene.store.IndexOutput;
//
//import java.io.IOException;
//
//import static com.github.luben.zstd.Zstd.compress;
//
//public class KNN990ProfileDecompresser {
//    // During quantization/indexing:
//    public void writeProfileState(SegmentWriteState state, int fieldNumber, SegmentProfilerState profileState) throws IOException {
//        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "knnprofile");
//        try (IndexOutput output = state.directory.createOutput(fileName, IOContext.DEFAULT)) {
//            // Write header
//            CodecUtil.writeHeader(output, "KNNProfile", 1);
//
//            // Write the number of fields
//            output.writeInt(1);
//
//            // Write field number
//            output.writeInt(fieldNumber);
//
//            // Get profile state bytes
//            byte[] profileBytes = profileState.toByteArray();
//
//            // Compress the bytes
//            byte[] compressedBytes = compress(profileBytes);
//
//            // Write compressed data length
//            output.writeInt(compressedBytes.length);
//
//            // Write uncompressed length (for allocation)
//            output.writeInt(profileBytes.length);
//
//            // Write compressed bytes
//            output.writeBytes(compressedBytes, 0, compressedBytes.length);
//
//            // Write footer with checksum
//            CodecUtil.writeFooter(output);
//        }
//    }
//
//    // During reading/profiling:
//    public static SegmentProfilerState readProfileState(SegmentReadState state, String field) throws IOException {
//        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "knnprofile");
//        if (!state.directory.fileExists(fileName)) {
//            return null;
//        }
//
//        try (IndexInput input = state.directory.openInput(fileName, IOContext.DEFAULT)) {
//            CodecUtil.checkHeader(input, "KNNProfile", 1, 1);
//
//            int fieldCount = input.readInt();
//            int fieldNumber = state.fieldInfos.fieldInfo(field).getFieldNumber();
//
//            for (int i = 0; i < fieldCount; i++) {
//                int currentField = input.readInt();
//                int compressedLength = input.readInt();
//                int uncompressedLength = input.readInt();
//
//                if (currentField == fieldNumber) {
//                    byte[] compressedData = new byte[compressedLength];
//                    input.readBytes(compressedData, 0, compressedLength);
//                    byte[] uncompressedData = decompress(compressedData);
//                    return SegmentProfilerState.fromBytes(uncompressedData);
//                } else {
//                    // Skip this field's data
//                    input.skipBytes(compressedLength);
//                }
//            }
//
//            return null;
//        }
//    }
//
//}
