package cn.edu.tsinghua.tsfile.timeseries.filter;

import cn.edu.tsinghua.tsfile.common.exception.filter.UnSupportFilterDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;

/**
 * @author ZJR
 * class to construct digest.
 */
public class DigestForFilter {

    private ByteBuffer min;
    private ByteBuffer max;
    private TSDataType type;

    public DigestForFilter(ByteBuffer min, ByteBuffer max, TSDataType type) {
        this.min = min;
        this.max = max;
        this.type = type;
    }

    public DigestForFilter(long minv, long maxv) {
        this.min = ByteBuffer.wrap(BytesUtils.longToBytes(minv));
        this.max = ByteBuffer.wrap(BytesUtils.longToBytes(maxv));
        this.type = TSDataType.INT64;
    }

    @SuppressWarnings("unchecked")
    private  <T extends Comparable<T>> T getValue(ByteBuffer value){
        switch (type) {
            case INT32:
                return (T) ((Integer) BytesUtils.bytesToInt(value.array()));
            case INT64:
                return (T) ((Long) BytesUtils.bytesToLong(value.array()));
            case FLOAT:
                return (T) ((Float) BytesUtils.bytesToFloat(value.array()));
            case DOUBLE:
                return (T) ((Double) BytesUtils.bytesToDouble(value.array()));
            case TEXT:
                return (T) new Binary(BytesUtils.bytesToString(value.array()));
            case BOOLEAN:
                return (T) (Boolean) BytesUtils.bytesToBool(value.array());
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    public <T extends Comparable<T>> T getMinValue() {
        return getValue(min);
    }

    public <T extends Comparable<T>> T getMaxValue() {
        return getValue(max);
    }

    public Class<?> getTypeClass() {
        switch (type) {
            case INT32:
                return Integer.class;
            case INT64:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TEXT:
                return String.class;
            case BOOLEAN:
                return Boolean.class;
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    public TSDataType getType() {
        return type;
    }

}
