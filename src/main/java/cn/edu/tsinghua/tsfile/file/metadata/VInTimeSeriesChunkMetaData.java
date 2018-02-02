package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * For more information, see ValueInTimeSeriesChunkMetaData
 * in cn.edu.thu.tsfile.format package
 */
public class VInTimeSeriesChunkMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(VInTimeSeriesChunkMetaData.class);

    private TSDataType dataType;

    private TsDigest digest;
    private int maxError;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    public VInTimeSeriesChunkMetaData() {
    }

    public VInTimeSeriesChunkMetaData(TSDataType dataType) {
        this.dataType = dataType;
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(dataType, outputStream);
        if(dataType != null)byteLen += ReadWriteToBytesUtils.write(dataType.toString(), outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(digest, outputStream);
        if(digest != null)byteLen += ReadWriteToBytesUtils.write(digest, outputStream);

        byteLen += ReadWriteToBytesUtils.write(maxError, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(enumValues, outputStream);
        if(enumValues != null)byteLen += ReadWriteToBytesUtils.write(enumValues, TSDataType.TEXT, outputStream);

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            dataType = TSDataType.valueOf(ReadWriteToBytesUtils.readString(inputStream));

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            digest = ReadWriteToBytesUtils.readDigest(inputStream);

        maxError = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            enumValues = ReadWriteToBytesUtils.readStringList(inputStream);
    }

    @Override
    public String toString() {
        return String.format("VInTimeSeriesChunkMetaData{ TSDataType %s, TSDigest %s, maxError %d, enumValues %s }", dataType, digest,
                maxError, enumValues);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TsDigest getDigest() {
        return digest;
    }

    public void setDigest(TsDigest digest) {
        this.digest = digest;
    }

    public int getMaxError() {
        return maxError;
    }

    public void setMaxError(int maxError) {
        this.maxError = maxError;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }
}
