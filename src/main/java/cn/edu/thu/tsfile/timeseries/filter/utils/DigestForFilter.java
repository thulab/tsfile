package cn.edu.thu.tsfile.timeseries.filter.utils;

import java.nio.ByteBuffer;

import cn.edu.thu.tsfile.common.exception.filter.UnSupportFilterDataTypeException;
import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;

/**
 * @description class to construct digest.
 * @author ZJR
 *
 */
public class DigestForFilter {

    private ByteBuffer min = null;
    private ByteBuffer max = null;
    private TSDataType type;
    
    public DigestForFilter(ByteBuffer min, ByteBuffer max, TSDataType type) {
        this.min = min;
        this.max = max;
        this.type = type;
    }
    
    public DigestForFilter(long minv, long maxv){
    	this.min = ByteBuffer.wrap(BytesUtils.longToBytes(minv));
    	this.max = ByteBuffer.wrap(BytesUtils.longToBytes(maxv));
    	this.type = TSDataType.INT64;
    }
    
    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T getMinValue() {
        switch (type) {
            case INT32:
                return (T) ((Integer) BytesUtils.bytesToInt(min.array()));
            case INT64:
                return (T) ((Long) BytesUtils.bytesToLong(min.array()));
            case FLOAT:
                return (T) ((Float) BytesUtils.bytesToFloat(min.array()));
            case DOUBLE:
                return (T) ((Double) BytesUtils.bytesToDouble(min.array()));
            case BYTE_ARRAY:
                return (T) new Binary(BytesUtils.bytesToString(min.array()));
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends Comparable<T>> T getMaxValue() {
        switch (type) {
            case INT32:
                return (T) ((Integer) BytesUtils.bytesToInt(max.array()));
            case INT64:
                return (T) ((Long) BytesUtils.bytesToLong(max.array()));
            case FLOAT:
                return (T) ((Float) BytesUtils.bytesToFloat(max.array()));
            case DOUBLE:
                return (T) ((Double) BytesUtils.bytesToDouble(max.array()));
            case BYTE_ARRAY:
                return (T) new Binary(BytesUtils.bytesToString(max.array()));
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
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
            case BYTE_ARRAY:
                return String.class;
            default:
                throw new UnSupportFilterDataTypeException("DigestForFilter unsupported datatype : " + type.toString());
        }
    }
    
    public TSDataType getType(){
    	return type;
    }

}
