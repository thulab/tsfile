package cn.edu.tsinghua.tsfile.file.header;

import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PageHeader {
    public PageType type; // required
    /**
     * Uncompressed page size in bytes (not including this header) *
     */
    public int uncompressed_page_size; // required
    /**
     * Compressed page size in bytes (not including this header) *
     */
    public int compressed_page_size; // required
    /**
     * 32bit crc for the data below. This allows for disabling checksumming in HDFS
     * if only a few pages needs to be read
     *
     */
    public int crc; // optional
    public DataPageHeader data_page_header; // optional
    public IndexPageHeader index_page_header; // optional
    public DictionaryPageHeader dictionary_page_header; // optional

    public PageHeader() {
    }

    public PageHeader(PageType type, int uncompressed_page_size, int compressed_page_size) {
        this.type = type;
        this.uncompressed_page_size = uncompressed_page_size;
        this.compressed_page_size = compressed_page_size;
    }

    public PageHeader(PageType type, int uncompressed_page_size, int compressed_page_size, int crc, DataPageHeader data_page_header, IndexPageHeader index_page_header, DictionaryPageHeader dictionary_page_header) {
        this.type = type;
        this.uncompressed_page_size = uncompressed_page_size;
        this.compressed_page_size = compressed_page_size;
        this.crc = crc;
        this.data_page_header = data_page_header;
        this.index_page_header = index_page_header;
        this.dictionary_page_header = dictionary_page_header;
    }

    public DataPageHeader getData_page_header() {
        return data_page_header;
    }

    public PageType getType() {
        return type;
    }

    public int getUncompressed_page_size() {
        return uncompressed_page_size;
    }

    public int getCompressed_page_size() {
        return compressed_page_size;
    }

    public void setCrc(int crc) {
        this.crc = crc;
    }

    public void setData_page_header(DataPageHeader data_page_header) {
        this.data_page_header = data_page_header;
    }

    public void setIndex_page_header(IndexPageHeader index_page_header) {
        this.index_page_header = index_page_header;
    }

    public void setDictionary_page_header(DictionaryPageHeader dictionary_page_header) {
        this.dictionary_page_header = dictionary_page_header;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(type, outputStream);
        if(type != null)byteLen += ReadWriteToBytesUtils.write(type.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.write(uncompressed_page_size, outputStream);
        byteLen += ReadWriteToBytesUtils.write(compressed_page_size, outputStream);
        byteLen += ReadWriteToBytesUtils.write(crc, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(data_page_header, outputStream);
        if(data_page_header != null)byteLen += data_page_header.write(outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(index_page_header, outputStream);
        if(index_page_header != null)byteLen += index_page_header.write(outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(dictionary_page_header, outputStream);
        if(dictionary_page_header != null)byteLen += dictionary_page_header.write(outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            type = PageType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        uncompressed_page_size = ReadWriteToBytesUtils.readInt(inputStream);
        compressed_page_size = ReadWriteToBytesUtils.readInt(inputStream);
        crc = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
            data_page_header = new DataPageHeader();
            data_page_header.read(inputStream);
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
            index_page_header = new IndexPageHeader();
            index_page_header.read(inputStream);
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream)) {
            dictionary_page_header = new DictionaryPageHeader();
            dictionary_page_header.read(inputStream);
        }
    }
}

