package cn.edu.tsinghua.tsfile.file.footer;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.MetaMarker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RowGroupFooter {
    public static final byte MARKER = MetaMarker.RowGroupFooter;

    String deltaObjectID;
    long dataSize;
    int numberOfChunks;
    /**
     * The time when endRowgroup() is called.
     */
    private long writtenTime;


    private int serializedSize ;//this filed does not need to be sieralized.

    public int getSerializedSize() {
        return serializedSize;
    }



    public RowGroupFooter(String deltaObjectID, long dataSize, int numberOfChunks) {
        this.deltaObjectID = deltaObjectID;
        this.dataSize = dataSize;
        this.numberOfChunks = numberOfChunks;
        this.serializedSize = Byte.BYTES + Integer.BYTES + deltaObjectID.length() + Long.BYTES + Integer.BYTES;
    }

    public String getDeltaObjectID() {
        return deltaObjectID;
    }

    public long getDataSize() {
        return dataSize;
    }

    public int getNumberOfChunks() {
        return numberOfChunks;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int length=0;
        length+=ReadWriteIOUtils.write(MARKER, outputStream);
        length+=ReadWriteIOUtils.write(deltaObjectID,outputStream);
        length+=ReadWriteIOUtils.write(dataSize,outputStream);
        length+=ReadWriteIOUtils.write(numberOfChunks,outputStream);
        assert length == getSerializedSize();
        return length;
    }

    /**
     *
     * @param inputStream
     * @param markerRead  Whether the marker of the RowGroupFooter is read ahead.
     * @return
     * @throws IOException
     */
    public static RowGroupFooter deserializeFrom(InputStream inputStream, boolean markerRead) throws IOException {
        if (!markerRead) {
            byte marker = (byte) inputStream.read();
            if (marker != MARKER)
                MetaMarker.handleUnexpectedMarker(marker);
        }

        String deltaObjectID=ReadWriteIOUtils.readString(inputStream);
        long dataSize=ReadWriteIOUtils.readLong(inputStream);
        int numOfChunks=ReadWriteIOUtils.readInt(inputStream);
        return new RowGroupFooter(deltaObjectID, dataSize, numOfChunks);
    }

    public static int getSerializedSize(String deltaObjectID) {
        return Byte.BYTES + Integer.BYTES + deltaObjectID.length() + Long.BYTES + Integer.BYTES;
    }

    @Override
    public String toString() {
        return "RowGroupFooter{" +
                "deltaObjectID='" + deltaObjectID + '\'' +
                ", dataSize=" + dataSize +
                ", numberOfChunks=" + numberOfChunks +
                ", serializedSize=" + serializedSize +
                '}';
    }
}
