package cn.edu.tsinghua.tsfile.file;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PageHeader {

    int uncompressedSize;
    int compressedSize;
    int numOfValues;
    Statistics<?> statistics;
    long max_timestamp;
    long min_timestamp;

    public PageHeader(int uncompressedSize, int compressedSize, int numOfValues, Statistics<?> statistics, long max_timestamp, long min_timestamp) {
        this.uncompressedSize = uncompressedSize;
        this.compressedSize = compressedSize;
        this.numOfValues = numOfValues;
        this.statistics = statistics;
        this.max_timestamp = max_timestamp;
        this.min_timestamp = min_timestamp;
    }

    private PageHeader(){}

    public int getUncompressedSize() {
        return uncompressedSize;
    }

    public void setUncompressedSize(int uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }

    public int getCompressedSize() {
        return compressedSize;
    }

    public void setCompressedSize(int compressedSize) {
        this.compressedSize = compressedSize;
    }

    public int getNumOfValues() {
        return numOfValues;
    }

    public void setNumOfValues(int numOfValues) {
        this.numOfValues = numOfValues;
    }

    public Statistics<?> getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics<?> statistics) {
        this.statistics = statistics;
    }

    public long getMax_timestamp() {
        return max_timestamp;
    }

    public void setMax_timestamp(long max_timestamp) {
        this.max_timestamp = max_timestamp;
    }

    public long getMin_timestamp() {
        return min_timestamp;
    }

    public void setMin_timestamp(long min_timestamp) {
        this.min_timestamp = min_timestamp;
    }


    public int serializeTo(OutputStream outputStream) throws IOException {
        int length=0;
        length+=ReadWriteToBytesUtils.write(uncompressedSize,outputStream);
        length+=ReadWriteToBytesUtils.write(compressedSize,outputStream);
        length+=ReadWriteToBytesUtils.write(numOfValues,outputStream);
        length+=ReadWriteToBytesUtils.write(max_timestamp, outputStream);
        length+=ReadWriteToBytesUtils.write(min_timestamp,outputStream);
        length+=statistics.serialize(outputStream);
        return length;
    }

    public static PageHeader deserializeFrom(InputStream inputStream, TSDataType dataType) throws IOException {
        PageHeader header=new PageHeader();
        header.uncompressedSize = ReadWriteToBytesUtils.readInt(inputStream);
        header.compressedSize = ReadWriteToBytesUtils.readInt(inputStream);
        header.numOfValues = ReadWriteToBytesUtils.readInt(inputStream);
        header.max_timestamp = ReadWriteToBytesUtils.readInt(inputStream);
        header.min_timestamp = ReadWriteToBytesUtils.readInt(inputStream);
        header.statistics = Statistics.deserialize(inputStream, dataType);
        return header;
    }

}

