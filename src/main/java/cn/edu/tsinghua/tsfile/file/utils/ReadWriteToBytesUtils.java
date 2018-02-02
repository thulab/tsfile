package cn.edu.tsinghua.tsfile.file.utils;

import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.header.DataPageHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.header.PageType;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ConverterUtils is a utility class. It provide conversion between normal datatype and byte array.
 *
 * @author East
 */
public class ReadWriteToBytesUtils {

    public static int INT_LEN = 4;
    public static int LONG_LEN = 8;

    /**
     * convert Integer to byte array
     *
     * @param a
     * @return
     */
    public static byte[] intToByteArray(int a) {
        return new byte[] {
                (byte) ((a >> 24) & 0xFF),
                (byte) ((a >> 16) & 0xFF),
                (byte) ((a >> 8) & 0xFF),
                (byte) (a & 0xFF)
        };
    }

    public static int byteArrayToInt(byte[] b) {
        return   b[3] & 0xFF |
                (b[2] & 0xFF) << 8 |
                (b[1] & 0xFF) << 16 |
                (b[0] & 0xFF) << 24;
    }

    public static byte[] longToByteArray(long s) {
        byte[] targets = new byte[8];
        for (int i = 0; i < 8; i++) {
            int offset = (targets.length - 1 - i) * 8;
            targets[i] = (byte) ((s >>> offset) & 0xff);
        }
        return targets;
    }

    public static long byteArrayToLong(byte[] b){
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (b[ix] & 0xff);
        }
        return num;
    }

    public static int write(Boolean flag, OutputStream outputStream) throws IOException {
        if(flag)outputStream.write(1);
        else outputStream.write(0);
        return 1;
    }

    public static boolean readBool(InputStream inputStream) throws IOException {
        int flag = inputStream.read();
        return flag == 1;
    }

    public static int writeIsNull(Object object, OutputStream outputStream) throws IOException {
        return write(object != null, outputStream);
    }

    public static boolean readIsNull(InputStream inputStream) throws IOException {
        return readBool(inputStream);
    }

    public static int write(int n, OutputStream outputStream) throws IOException {
        byte[] bytes = intToByteArray(n);
        outputStream.write(bytes);
        return bytes.length;
    }

    public static int readInt(InputStream inputStream) throws IOException {
        byte[] bytes = new byte[INT_LEN];
        inputStream.read(bytes);
        return byteArrayToInt(bytes);
    }

    public static int write(long n, OutputStream outputStream) throws IOException {
        byte[] bytes = longToByteArray(n);
        outputStream.write(bytes);
        return bytes.length;
    }

    public static long readLong(InputStream inputStream) throws IOException {
        byte[] bytes = new byte[LONG_LEN];
        inputStream.read(bytes);
        return byteArrayToLong(bytes);
    }

    public static int write(String s, OutputStream outputStream) throws IOException {
        int len = 0;
        len += write(s.length(), outputStream);
        byte[] bytes = s.getBytes();
        outputStream.write(bytes);
        len += bytes.length;
        return len;
    }

    public static String readString(InputStream inputStream) throws IOException {
        int sLength = readInt(inputStream);
        byte[] bytes = new byte[sLength];
        inputStream.read(bytes, 0, sLength);
        return new String(bytes, 0, sLength);
    }

    public static int write(ByteBuffer byteBuffer, OutputStream outputStream) throws IOException {
        int len = 0;
        len += write(byteBuffer.capacity(), outputStream);
        byte[] bytes = byteBuffer.array();
        outputStream.write(bytes);
        len += bytes.length;
        return len;
    }

    public static ByteBuffer readByteBuffer(InputStream inputStream) throws IOException {
        int byteLength = readInt(inputStream);
        byte[] bytes = new byte[byteLength];
        inputStream.read(bytes);
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteLength);
        byteBuffer.put(bytes);

        return byteBuffer;
    }

    public static int write(List list, TSDataType dataType, OutputStream outputStream) throws IOException {
        int len = 0;

        len += write(list.size(), outputStream);
        for(Object one : list){
            switch (dataType){
                case INT32:
                    len += write((int)one, outputStream);
                    break;
                case TEXT:
                    len += write((String)one, outputStream);
                    break;
                default:
                    throw new IOException(String.format("Unsupported data type for {}", dataType.toString()));
            }
        }

        return len;
    }

    public static List<Integer> readIntegerList(InputStream inputStream) throws IOException {
        List<Integer> list = new ArrayList<>();
        int size = readInt(inputStream);

        for(int i = 0;i < size;i++)
            list.add(readInt(inputStream));

        return list;
    }

    public static List<String> readStringList(InputStream inputStream) throws IOException {
        List<String> list = new ArrayList<>();
        int size = readInt(inputStream);

        for(int i = 0;i < size;i++)
            list.add(readString(inputStream));

        return list;
    }

    public static int write(TsDigest digest, OutputStream outputStream) throws IOException {
        return digest.write(outputStream);
    }

    public static TsDigest readDigest(InputStream inputStream) throws IOException {
        TsDigest tsDigest = new TsDigest();
        tsDigest.read(inputStream);
        return tsDigest;
    }

    public static int write(VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData, OutputStream outputStream) throws IOException {
        return vInTimeSeriesChunkMetaData.write(outputStream);
    }

    public static VInTimeSeriesChunkMetaData readVInTimeSeriesChunkMetaData(InputStream inputStream) throws IOException {
        VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData = new VInTimeSeriesChunkMetaData();
        vInTimeSeriesChunkMetaData.read(inputStream);
        return vInTimeSeriesChunkMetaData;
    }

    public static int write(TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData, OutputStream outputStream) throws IOException {
        return tInTimeSeriesChunkMetaData.write(outputStream);
    }

    public static TInTimeSeriesChunkMetaData readTInTimeSeriesChunkMetaData(InputStream inputStream) throws IOException {
        TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = new TInTimeSeriesChunkMetaData();
        tInTimeSeriesChunkMetaData.read(inputStream);
        return tInTimeSeriesChunkMetaData;
    }

    public static int write(TimeSeriesMetadata timeSeriesMetadata, OutputStream outputStream) throws IOException {
        return timeSeriesMetadata.write(outputStream);
    }

    public static TimeSeriesMetadata readTimeSeriesMetadata(InputStream inputStream) throws IOException {
        TimeSeriesMetadata timeSeriesMetadata = new TimeSeriesMetadata();
        timeSeriesMetadata.read(inputStream);
        return timeSeriesMetadata;
    }

    public static int write(TimeSeriesChunkProperties timeSeriesChunkProperties, OutputStream outputStream) throws IOException {
        return timeSeriesChunkProperties.write(outputStream);
    }

    public static TimeSeriesChunkProperties readTimeSeriesChunkProperties(InputStream inputStream) throws IOException {
        TimeSeriesChunkProperties timeSeriesChunkProperties = new TimeSeriesChunkProperties();
        timeSeriesChunkProperties.read(inputStream);
        return timeSeriesChunkProperties;
    }

    public static int write(TimeSeriesChunkMetaData timeSeriesChunkMetaData, OutputStream outputStream) throws IOException {
        return timeSeriesChunkMetaData.write(outputStream);
    }

    public static TimeSeriesChunkMetaData readTimeSeriesChunkMetaData(InputStream inputStream) throws IOException {
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();
        timeSeriesChunkMetaData.read(inputStream);
        return timeSeriesChunkMetaData;
    }

    public static int write(RowGroupMetaData rowGroupMetaData, OutputStream outputStream) throws IOException {
        return rowGroupMetaData.write(outputStream);
    }

    public static RowGroupMetaData readRowGroupMetaData(InputStream inputStream) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();
        rowGroupMetaData.read(inputStream);
        return rowGroupMetaData;
    }

    public static int write(TsRowGroupBlockMetaData rowGroupBlockMetaData, OutputStream outputStream) throws IOException {
        return rowGroupBlockMetaData.write(outputStream);
    }

    public static TsRowGroupBlockMetaData readTsRowGroupBlockMetaData(InputStream inputStream) throws IOException {
        TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
        rowGroupBlockMetaData.read(inputStream);
        return rowGroupBlockMetaData;
    }

    public static TsRowGroupBlockMetaData readTsRowGroupBlockMetaData(ITsRandomAccessFileReader reader, long offset,
                                                                  int size) throws IOException {
        reader.seek(offset);
        byte[] buf = new byte[size];
        reader.read(buf, 0, buf.length);
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);

        return readTsRowGroupBlockMetaData(bais);
    }

    public static int write(TsDeltaObject tsDeltaObject, OutputStream outputStream) throws IOException {
        return tsDeltaObject.write(outputStream);
    }

    public static TsDeltaObject readTsDeltaObject(InputStream inputStream) throws IOException {
        TsDeltaObject tsDeltaObject = new TsDeltaObject();
        tsDeltaObject.read(inputStream);
        return tsDeltaObject;
    }

    public static int write(TsFileMetaData tsFileMetaData, OutputStream outputStream) throws IOException {
        return tsFileMetaData.write(outputStream);
    }

    public static TsFileMetaData readTsFileMetaData(InputStream inputStream) throws IOException {
        TsFileMetaData tsFileMetaData = new TsFileMetaData();
        tsFileMetaData.read(inputStream);
        return tsFileMetaData;
    }

    public static int write(PageHeader pageHeader, OutputStream outputStream) throws IOException {
        return pageHeader.write(outputStream);
    }

    public static PageHeader readPageHeader(InputStream inputStream) throws IOException {
        PageHeader pageHeader = new PageHeader();
        pageHeader.read(inputStream);
        return pageHeader;
    }

    public static int writeDataPageHeader(int uncompressedSize, int compressedSize, int numValues,
                                           Statistics<?> statistics, int numRows, TSEncoding encoding,
                                           long max_timestamp, long min_timestamp, OutputStream outputStream) throws IOException {
        PageHeader pageHeader = new PageHeader(PageType.DATA_PAGE, uncompressedSize, compressedSize); // TODO: pageHeader crc uncomplete
        pageHeader.setData_page_header(new DataPageHeader(numValues, numRows, TSEncoding.valueOf(encoding.toString()),
                max_timestamp, min_timestamp));
        if (!statistics.isEmpty()) {
            TsDigest digest = new TsDigest();
            Map<String, ByteBuffer> statisticsMap = new HashMap<>();
            // TODO add your statistics
            statisticsMap.put(StatisticConstant.MAX_VALUE, ByteBuffer.wrap(statistics.getMaxBytes()));
            statisticsMap.put(StatisticConstant.MIN_VALUE, ByteBuffer.wrap(statistics.getMinBytes()));
            statisticsMap.put(StatisticConstant.FIRST, ByteBuffer.wrap(statistics.getFirstBytes()));
            statisticsMap.put(StatisticConstant.SUM, ByteBuffer.wrap(statistics.getSumBytes()));
            statisticsMap.put(StatisticConstant.LAST, ByteBuffer.wrap(statistics.getLastBytes()));
            digest.setStatistics(statisticsMap);

            pageHeader.data_page_header.setDigest(digest);
        }
        return write(pageHeader, outputStream);
    }
}
