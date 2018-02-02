package cn.edu.tsinghua.tsfile.file.header;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DictionaryPageHeader {

    /**
     * Number of values in the dictionary *
     */
    public int num_values; // required
    /**
     * Encoding using this dictionary page *
     *
     * @see TSEncoding
     */
    public TSEncoding encoding; // required
    /**
     * If true, the entries in the dictionary are sorted in ascending order *
     */
    public boolean is_sorted; // optional

    public DictionaryPageHeader() {
    }

    public DictionaryPageHeader(int num_values, TSEncoding encoding) {
        this.num_values = num_values;
        this.encoding = encoding;
    }

    public DictionaryPageHeader(int num_values, TSEncoding encoding, boolean is_sorted) {
        this.num_values = num_values;
        this.encoding = encoding;
        this.is_sorted = is_sorted;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.write(num_values, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(encoding, outputStream);
        if(encoding != null)byteLen += ReadWriteToBytesUtils.write(encoding.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.write(is_sorted, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        num_values = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            encoding = TSEncoding.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        is_sorted = ReadWriteToBytesUtils.readBool(inputStream);
    }
}
