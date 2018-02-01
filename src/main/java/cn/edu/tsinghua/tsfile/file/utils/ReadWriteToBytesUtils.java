package cn.edu.tsinghua.tsfile.file.utils;

import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * ConverterUtils is a utility class. It provide conversion between normal datatype and byte array.
 *
 * @author East
 */
public class ReadWriteToBytesUtils {

    private static byte[] intBytes = new byte[4];
    private static byte[] longBytes = new byte[8];
    private static byte[] stringBytes = new byte[100];

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

    public static void writeIsNull(Object object, OutputStream outputStream) throws IOException {
        if(object == null)write(0, outputStream);
        else write(1, outputStream);
    }

    public static boolean readIsNull(InputStream inputStream) throws IOException {
        int flag = readInt(inputStream);
        if(flag == 1)return true;
        else return false;
    }

    public static void write(int n, OutputStream outputStream) throws IOException {
        outputStream.write(intToByteArray(n));
    }

    public static int readInt(InputStream inputStream) throws IOException {
        inputStream.read(intBytes);
        return byteArrayToInt(intBytes);
    }

    public static void write(long n, OutputStream outputStream) throws IOException {
        outputStream.write(longToByteArray(n));
    }

    public static long readLong(InputStream inputStream) throws IOException {
        inputStream.read(longBytes);
        return byteArrayToLong(longBytes);
    }

    public static void write(String s, OutputStream outputStream) throws IOException {
        write(s.length(), outputStream);
        outputStream.write(s.getBytes());
    }

    public static String readString(InputStream inputStream) throws IOException {
        int sLength = readInt(inputStream);
        inputStream.read(stringBytes, 0, sLength);

        return new String(stringBytes, 0, sLength);
    }

    public static void write(ByteBuffer byteBuffer, OutputStream outputStream) throws IOException {
        write(byteBuffer.position(), outputStream);
        outputStream.write(byteBuffer.array());
    }

    public static ByteBuffer readByteBuffer(InputStream inputStream) throws IOException {
        int byteLength = readInt(inputStream);
        byte[] bytes = new byte[byteLength];
        inputStream.read(bytes);
        ByteBuffer byteBuffer = ByteBuffer.allocate(byteLength);
        byteBuffer.put(bytes);

        return byteBuffer;
    }

    public static void write(List list, TSDataType dataType, OutputStream outputStream) throws IOException {
        write(list.size(), outputStream);

        for(Object one : list){
            switch (dataType){
                case INT32:
                    write((int)one, outputStream);
                    break;
                case TEXT:
                    write((String)one, outputStream);
                    break;
                default:
                    throw new IOException(String.format("Unsupported data type for {}", dataType.toString()));
            }
        }
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

    public static void write(TsDigest digest, OutputStream outputStream) throws IOException {
        digest.write(outputStream);
    }

    public static TsDigest readDigest(InputStream inputStream) throws IOException {
        TsDigest tsDigest = new TsDigest();
        tsDigest.read(inputStream);
        return tsDigest;
    }

    public static void write(VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData, OutputStream outputStream) throws IOException {
        vInTimeSeriesChunkMetaData.write(outputStream);
    }

    public static VInTimeSeriesChunkMetaData readVInTimeSeriesChunkMetaData(InputStream inputStream) throws IOException {
        VInTimeSeriesChunkMetaData vInTimeSeriesChunkMetaData = new VInTimeSeriesChunkMetaData();
        vInTimeSeriesChunkMetaData.read(inputStream);
        return vInTimeSeriesChunkMetaData;
    }

    public static void write(TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData, OutputStream outputStream) throws IOException {
        tInTimeSeriesChunkMetaData.write(outputStream);
    }

    public static TInTimeSeriesChunkMetaData readTInTimeSeriesChunkMetaData(InputStream inputStream) throws IOException {
        TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = new TInTimeSeriesChunkMetaData();
        tInTimeSeriesChunkMetaData.read(inputStream);
        return tInTimeSeriesChunkMetaData;
    }

    public static void write(TimeSeriesMetadata timeSeriesMetadata, OutputStream outputStream) throws IOException {
        timeSeriesMetadata.write(outputStream);
    }

    public static TimeSeriesMetadata readTimeSeriesMetadata(InputStream inputStream) throws IOException {
        TimeSeriesMetadata timeSeriesMetadata = new TimeSeriesMetadata();
        timeSeriesMetadata.read(inputStream);
        return timeSeriesMetadata;
    }

    public static void write(TimeSeriesChunkProperties timeSeriesChunkProperties, OutputStream outputStream) throws IOException {
        timeSeriesChunkProperties.write(outputStream);
    }

    public static TimeSeriesChunkProperties readTimeSeriesChunkProperties(InputStream inputStream) throws IOException {
        TimeSeriesChunkProperties timeSeriesChunkProperties = new TimeSeriesChunkProperties();
        timeSeriesChunkProperties.read(inputStream);
        return timeSeriesChunkProperties;
    }

    public static void write(TimeSeriesChunkMetaData timeSeriesChunkMetaData, OutputStream outputStream) throws IOException {
        timeSeriesChunkMetaData.write(outputStream);
    }

    public static TimeSeriesChunkMetaData readTimeSeriesChunkMetaData(InputStream inputStream) throws IOException {
        TimeSeriesChunkMetaData timeSeriesChunkMetaData = new TimeSeriesChunkMetaData();
        timeSeriesChunkMetaData.read(inputStream);
        return timeSeriesChunkMetaData;
    }

    public static void write(RowGroupMetaData rowGroupMetaData, OutputStream outputStream) throws IOException {
        rowGroupMetaData.write(outputStream);
    }

    public static RowGroupMetaData readRowGroupMetaData(InputStream inputStream) throws IOException {
        RowGroupMetaData rowGroupMetaData = new RowGroupMetaData();
        rowGroupMetaData.read(inputStream);
        return rowGroupMetaData;
    }

    public static void write(TsRowGroupBlockMetaData rowGroupBlockMetaData, OutputStream outputStream) throws IOException {
        rowGroupBlockMetaData.write(outputStream);
    }

    public static TsRowGroupBlockMetaData readTsRowGroupBlockMetaData(InputStream inputStream) throws IOException {
        TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
        rowGroupBlockMetaData.read(inputStream);
        return rowGroupBlockMetaData;
    }

    public static void write(TsDeltaObject tsDeltaObject, OutputStream outputStream) throws IOException {
        tsDeltaObject.write(outputStream);
    }

    public static TsDeltaObject readTsDeltaObject(InputStream inputStream) throws IOException {
        TsDeltaObject tsDeltaObject = new TsDeltaObject();
        tsDeltaObject.read(inputStream);
        return tsDeltaObject;
    }

    public static void write(TsFileMetaData tsFileMetaData, OutputStream outputStream) throws IOException {
        tsFileMetaData.write(outputStream);
    }

    public static TsFileMetaData readTsFileMetaData(InputStream inputStream) throws IOException {
        TsFileMetaData tsFileMetaData = new TsFileMetaData();
        tsFileMetaData.read(inputStream);
        return tsFileMetaData;
    }

    private static class Test{
        public Test(){}

        private List<Integer> list;
        private int a;

        public void display(){
            System.out.println(a);
        }
    }

    public static void main(String[] args) throws IOException {
//        String path = "1.txt";
//
//        ByteBuffer byteBuffer = ByteBuffer.allocate(1000);
//        byteBuffer.put("abcbfucbevwufbvuwe".getBytes());
//
//        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(path));
//        write(byteBuffer.position(), outputStream);
//        outputStream.write(byteBuffer.array());
//        outputStream.close();
//
//        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(path));
//        int size = readInt(inputStream);
//        byte[] bytes = new byte[size];
//        inputStream.read(bytes);
//        String s = new String(bytes);
//        inputStream.close();
//
//        System.out.println(s);

        Test test = new Test();
        test.display();
    }
}
