package cn.edu.tsinghua.tsfile.file.header;

import cn.edu.tsinghua.tsfile.file.IBytesConverter;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DataPageHeader implements IBytesConverter {

    public int num_values; // required
    /**
     * Number of rows in this data page *
     */
    public int num_rows; // required
    /**
     * Encoding used for this data page *
     *
     * @see TSEncoding
     */
    public TSEncoding encoding; // required
    /**
     * Optional digest/statistics for the data in this page*
     */
    public TsDigest digest; // optional
    /**
     * whether the values are compressed.
     * Which means the section of the page is compressed with the compression_type.
     * If missing it is considered compressed
     */
    public boolean is_compressed; // optional
    public long max_timestamp; // required
    public long min_timestamp; // required

    public DataPageHeader() {
    }

    public DataPageHeader(int num_values, int num_rows, TSEncoding encoding, long max_timestamp, long min_timestamp) {
        this.num_values = num_values;
        this.num_rows = num_rows;
        this.encoding = encoding;
        this.max_timestamp = max_timestamp;
        this.min_timestamp = min_timestamp;
    }

    public DataPageHeader(int num_values, int num_rows, TSEncoding encoding, TsDigest digest, boolean is_compressed, long max_timestamp, long min_timestamp) {
        this.num_values = num_values;
        this.num_rows = num_rows;
        this.encoding = encoding;
        this.digest = digest;
        this.is_compressed = is_compressed;
        this.max_timestamp = max_timestamp;
        this.min_timestamp = min_timestamp;
    }

    public int getNum_values() {
        return num_values;
    }

    public int getNum_rows() {
        return num_rows;
    }

    public TSEncoding getEncoding() {
        return encoding;
    }

    public TsDigest getDigest() {
        return digest;
    }

    public boolean isIs_compressed() {
        return is_compressed;
    }

    public long getMax_timestamp() {
        return max_timestamp;
    }

    public long getMin_timestamp() {
        return min_timestamp;
    }

    public void setDigest(TsDigest digest) {
        this.digest = digest;
    }

    public void setIs_compressed(boolean is_compressed) {
        this.is_compressed = is_compressed;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.write(num_values, outputStream);
        byteLen += ReadWriteToBytesUtils.write(num_rows, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(encoding, outputStream);
        if(encoding != null)byteLen += ReadWriteToBytesUtils.write(encoding.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(digest, outputStream);
        if(digest != null)byteLen += ReadWriteToBytesUtils.write(digest, outputStream);

        byteLen += ReadWriteToBytesUtils.write(is_compressed, outputStream);
        byteLen += ReadWriteToBytesUtils.write(max_timestamp, outputStream);
        byteLen += ReadWriteToBytesUtils.write(min_timestamp, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        num_values = ReadWriteToBytesUtils.readInt(inputStream);
        num_rows = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            encoding = TSEncoding.valueOf(ReadWriteToBytesUtils.readString(inputStream));
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            digest = ReadWriteToBytesUtils.readDigest(inputStream);
        }

        is_compressed = ReadWriteToBytesUtils.readBool(inputStream);
        max_timestamp = ReadWriteToBytesUtils.readLong(inputStream);
        min_timestamp = ReadWriteToBytesUtils.readLong(inputStream);
    }
}
