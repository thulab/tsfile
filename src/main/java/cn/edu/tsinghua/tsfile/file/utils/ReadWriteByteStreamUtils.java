package cn.edu.tsinghua.tsfile.file.utils;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.*;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by lta on 2018/5/31.
 */
public class ReadWriteByteStreamUtils {

    public static void writeRowGroupBlockMetadata(TsDeltaObjectMetadata tsDeltaObjectMetadata, ITsRandomAccessFileWriter out) throws IOException {
//        List<RowGroupMetaData> rowGroupMetadataList = tsDeltaObjectMetadata.getRowGroups();
//        for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
//            writeRowGroupMetadata(rowGroupMetaData, out);
//        }
//        tsDeltaObjectMetadata.setOffset(out.getPos());
//        for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
//            out.getOutputStream().write(BytesUtils.longToBytes(rowGroupMetaData.getMetadataOffset()));
//            out.getOutputStream().write(BytesUtils.intToBytes(rowGroupMetaData.getMetadataSize()));
//        }
//        tsDeltaObjectMetadata.setMetadataBlockSize((int) (out.getPos() - tsDeltaObjectMetadata.getOffset()));
        tsDeltaObjectMetadata.serializeTo(out.getOutputStream());
    }

    public static void writeRowGroupMetadata(RowGroupMetaData rowGroupMetaData, ITsRandomAccessFileWriter out) throws IOException {
//        List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
//        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
//            writeTimeSeriesChunkMetadata(timeSeriesChunkMetaData, out);
//        }
//        rowGroupMetaData.setMetadataOffset(out.getPos());
//        for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
//            out.getOutputStream().write(BytesUtils.longToBytes(timeSeriesChunkMetaData.getTsDigestOffset()));
//        }
//        rowGroupMetaData.setMetadataSize((int) (out.getPos() - rowGroupMetaData.getMetadataOffset()));
        rowGroupMetaData.serializeTo(out.getOutputStream());
    }

    public static void writeTimeSeriesChunkMetadata(TimeSeriesChunkMetaData timeSeriesChunkMetaData, ITsRandomAccessFileWriter out) throws IOException {
//        TsDigest valuesStatistics = timeSeriesChunkMetaData.getDigest();
//        long offsetOfDigest = writeDigest(valuesStatistics, out);
//        timeSeriesChunkMetaData.setTsDigestOffset(out.getPos());
//        int digestSize = (int) (out.getPos() - offsetOfDigest);
//        out.getOutputStream().write(BytesUtils.longToBytes(Long.valueOf(timeSeriesChunkMetaData.getMeasurementUID())));
//        out.getOutputStream().write(BytesUtils.longToBytes(timeSeriesChunkMetaData.getFileOffset()));
//        out.getOutputStream().write(BytesUtils.intToBytes(timeSeriesChunkMetaData.getCompression().ordinal()));
//        out.getOutputStream().write(BytesUtils.longToBytes(timeSeriesChunkMetaData.getNumOfPoints()));
//        out.getOutputStream().write(BytesUtils.longToBytes(timeSeriesChunkMetaData.getTotalByteSize()));
//        out.getOutputStream().write(BytesUtils.longToBytes(timeSeriesChunkMetaData.getStartTime()));
//        out.getOutputStream().write(BytesUtils.longToBytes(timeSeriesChunkMetaData.getEndTime()));
//        out.getOutputStream().write(BytesUtils.intToBytes(timeSeriesChunkMetaData.getDataType().ordinal()));
//        out.getOutputStream().write(BytesUtils.longToBytes(offsetOfDigest));
//        out.getOutputStream().write(BytesUtils.intToBytes(digestSize));
        timeSeriesChunkMetaData.serializeTo(out.getOutputStream());
    }

    public static long writeDigest(TsDigest tsDigest, ITsRandomAccessFileWriter out) throws IOException {
//        Map<String, Long> offsetMap = new HashMap<>();
//        Map<String, ByteBuffer> statistics = tsDigest.getStatistics();
//        Iterator<Map.Entry<String, ByteBuffer>> iterator = statistics.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, ByteBuffer> entry = iterator.next();
//            ByteBuffer byteBuffer = entry.getValue();
//            offsetMap.put(entry.getKey(), out.getPos());
//            byte[] data = new byte[byteBuffer.remaining()];
//            byteBuffer.get(data, 0, data.length);
//            out.getOutputStream().write(data);
//        }
//        iterator = statistics.entrySet().iterator();
//        long offsetOfDigest = out.getPos();
//        while (iterator.hasNext()) {
//            Map.Entry<String, ByteBuffer> entry = iterator.next();
//            long offset = offsetMap.get(entry.getKey());
//            ByteBuffer byteBuffer = entry.getValue();
//            out.getOutputStream().write(BytesUtils.longToBytes(Long.valueOf(entry.getKey())));
//            out.getOutputStream().write(BytesUtils.longToBytes(offset));
//            out.getOutputStream().write(BytesUtils.intToBytes(byteBuffer.remaining()));
//        }
//        return offsetOfDigest;
        return tsDigest.serializeTo(out.getOutputStream());
    }

    public static int writeFileMetaData(TsFileMetaData tsFileMetadata, ITsRandomAccessFileWriter out) throws IOException {
//        List<TimeSeriesMetadata> timeSeriesList = tsFileMetadata.getTimeSeriesList();
//        for (int i = 0; i < timeSeriesList.size(); i++) {
//            if (i == 0) {
//                tsFileMetadata.setFirstTimeSeriesMetadataOffset(out.getPos());
//            }
//            if (i == timeSeriesList.size() - 1) {
//                tsFileMetadata.setLastTimeSeriesMetadataOffset(out.getPos());
//            }
//            out.getOutputStream().write(BytesUtils.longToBytes(Long.valueOf(timeSeriesList.get(i).getMeasurementUID())));
//            out.getOutputStream().write(BytesUtils.intToBytes(timeSeriesList.get(i).getType().ordinal()));
//        }
//        tsFileMetadata.setFirstTsDeltaObjectMetadataOffset(out.getPos());
//        Map<String, TsDeltaObjectMetadata> deltaObjectMap = tsFileMetadata.getDeltaObjectMap();
//        Iterator<Map.Entry<String, TsDeltaObjectMetadata>> iterator = deltaObjectMap.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<String, TsDeltaObjectMetadata> entry = iterator.next();
//            TsDeltaObjectMetadata currentTsDeltaObjectMetadata = entry.getValue();
//            out.getOutputStream().write(BytesUtils.longToBytes(Long.valueOf(entry.getKey())));
//            out.getOutputStream().write(BytesUtils.longToBytes(currentTsDeltaObjectMetadata.getOffset()));
//            out.getOutputStream().write(BytesUtils.intToBytes(currentTsDeltaObjectMetadata.getMetadataBlockSize()));
//            out.getOutputStream().write(BytesUtils.longToBytes(currentTsDeltaObjectMetadata.getStartTime()));
//            out.getOutputStream().write(BytesUtils.longToBytes(currentTsDeltaObjectMetadata.getEndTime()));
//        }
//        tsFileMetadata.setLastTsDeltaObjectMetadataOffset(out.getPos() - 36); //TsDeltaObjectMetadata has fixed length: 36 bytes.
//        out.getOutputStream().write(BytesUtils.StringToBytes(tsFileMetadata.getCreatedBy()));
        return tsFileMetadata.serializeTo(out.getOutputStream());
    }


    /**
     * read file metadata(thrift format) from stream
     *
     * @param from
     *            InputStream
     * @return metadata of TsFile
     * @throws IOException
     *             cannot read file metadata from OutputStream
     */
    public static TsFileMetaData readFileMetaData(InputStream from) throws IOException {
        return TsFileMetaData.deserializeFrom(from);
    }



}
