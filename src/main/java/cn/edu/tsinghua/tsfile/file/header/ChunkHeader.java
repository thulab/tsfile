package cn.edu.tsinghua.tsfile.file.header;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ChunkHeader {
    private String measurementID;
    private int dataSize;
    private TSDataType dataType;
    private CompressionType compressionType;
    private TSEncoding encodingType;
    private int numOfPages;


    private int serializedSize ;//this filed does not need to be sieralized.

    public int getSerializedSize() {
        return serializedSize;
    }

    private ChunkHeader(){

    }

    public ChunkHeader(String measurementID, int dataSize, TSDataType dataType, CompressionType compressionType, TSEncoding encoding, int numOfPages) {
        this.measurementID = measurementID;
        this.dataSize = dataSize;
        this.dataType = dataType;
        this.compressionType = compressionType;
        this.numOfPages = numOfPages;
        this.encodingType=encoding;
        this.serializedSize = getSerializedSize(measurementID);
    }

    public String getMeasurementID() {
        return measurementID;
    }

    public int getDataSize() {
        return dataSize;
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int length=0;
        length+=ReadWriteIOUtils.write(measurementID,outputStream);
        length+=ReadWriteIOUtils.write(dataSize,outputStream);
        length+=ReadWriteIOUtils.write(dataType,outputStream);
        length+=ReadWriteIOUtils.write(numOfPages,outputStream);
        length+=ReadWriteIOUtils.write(compressionType, outputStream);
        length+=ReadWriteIOUtils.write(encodingType, outputStream);
        assert length == getSerializedSize();
        return length;
    }
    public int serializeTo(ByteBuffer buffer) throws IOException {
        int length=0;
        length+=ReadWriteIOUtils.write(measurementID,buffer);
        length+=ReadWriteIOUtils.write(dataSize,buffer);
        length+=ReadWriteIOUtils.write(dataType,buffer);
        length+=ReadWriteIOUtils.write(numOfPages,buffer);
        length+=ReadWriteIOUtils.write(compressionType, buffer);
        length+=ReadWriteIOUtils.write(encodingType, buffer);
        assert length == getSerializedSize();
        return length;
    }


    public static ChunkHeader deserializeFrom(InputStream inputStream) throws IOException {
        String measurementID=ReadWriteIOUtils.readString(inputStream);
        int dataSize=ReadWriteIOUtils.readInt(inputStream);
        TSDataType dataType=TSDataType.deserialize(ReadWriteIOUtils.readShort(inputStream));
        int numOfPages=ReadWriteIOUtils.readInt(inputStream);
        CompressionType type=ReadWriteIOUtils.readCompressionType(inputStream);
        TSEncoding encoding=ReadWriteIOUtils.readEncoding(inputStream);
        return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages);
    }

    public int getNumOfPages() {
        return numOfPages;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public TSEncoding getEncodingType() {
        return encodingType;
    }

    public static int getSerializedSize(String measurementID){
        return Integer.BYTES + measurementID.length() + Integer.BYTES + TSDataType.getSerializedSize() + Integer.BYTES  + CompressionType.getSerializedSize() + TSEncoding.getSerializedSize();
    }

    @Override
    public String toString() {
        return "ChunkHeader{" +
                "measurementID='" + measurementID + '\'' +
                ", dataSize=" + dataSize +
                ", dataType=" + dataType +
                ", compressionType=" + compressionType +
                ", encodingType=" + encodingType +
                ", numOfPages=" + numOfPages +
                ", serializedSize=" + serializedSize +
                '}';
    }
}
