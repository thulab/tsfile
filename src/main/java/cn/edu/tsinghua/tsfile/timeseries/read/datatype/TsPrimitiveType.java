package cn.edu.tsinghua.tsfile.timeseries.read.datatype;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.io.Serializable;

/**
 * TODO 应该和DataPoint合并
 * @author Jinrui Zhang
 */
public abstract class TsPrimitiveType implements Serializable {
    /**
     * implemented in subclasses
     * @return
     */
    public boolean getBoolean() {
        throw new UnsupportedOperationException("getBoolean() is not supported for current sub-class");
    }

    /**
     * implemented in subclasses
     * @return
     */
    public int getInt() {
        throw new UnsupportedOperationException("getInt() is not supported for current sub-class");
    }

    /**
     * implemented in subclasses
     * @return
     */
    public long getLong() {
        throw new UnsupportedOperationException("getLong() is not supported for current sub-class");
    }

    /**
     * implemented in subclasses
     * @return
     */
    public float getFloat() {
        throw new UnsupportedOperationException("getFloat() is not supported for current sub-class");
    }

    /**
     * implemented in subclasses
     * @return
     */
    public double getDouble() {
        throw new UnsupportedOperationException("getDouble() is not supported for current sub-class");
    }

    /**
     * implemented in subclasses
     * @return
     */
    public Binary getBinary() {
        throw new UnsupportedOperationException("getBinary() is not supported for current sub-class");
    }

    /**
     * @return size of one instance of current class
     */
    public abstract int getSize();


    public abstract Object getValue();

    /**
     * get value in String format
     * @return
     */
    public abstract String getStringValue();

    /**
     * get corresponding data type of subclasses
     * @return
     */
    public abstract TSDataType getDataType();

    public String toString() {
        return getStringValue();
    }

    public boolean equals(Object object) {
        return (object instanceof TsPrimitiveType) && (((TsPrimitiveType) object).getValue().equals(getValue()));
    }


    public static class TsBoolean extends TsPrimitiveType {
        /** value in boolean **/
        public boolean value;

        /**
         * init value
         * @param value
         */
        public TsBoolean(boolean value) {
            this.value = value;
        }

        public boolean getBoolean() {
            return value;
        }

        @Override
        public int getSize() {
            // class size + boolean size
            return 4 + 1;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public TSDataType getDataType() {
            return TSDataType.BOOLEAN;
        }
    }

    public static class TsInt extends TsPrimitiveType {
        /** value in int **/
        public int value;

        /**
         * init value
         * @param value
         */
        public TsInt(int value) {
            this.value = value;
        }

        public int getInt() {
            return value;
        }

        @Override
        public int getSize() {
            // class size + int size
            return 4 + 4;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public TSDataType getDataType() {
            return TSDataType.INT32;
        }
    }

    public static class TsLong extends TsPrimitiveType {
        /** value in long **/
        public long value;

        /**
         * init value
         * @param value
         */
        public TsLong(long value) {
            this.value = value;
        }

        public long getLong() {
            return value;
        }

        @Override
        public int getSize() {
            // class size + long size
            return 4 + 8;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public TSDataType getDataType() {
            return TSDataType.INT64;
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    public static class TsFloat extends TsPrimitiveType {
        /** value in float **/
        public float value;

        /**
         * init value
         * @param value
         */
        public TsFloat(float value) {
            this.value = value;
        }

        public float getFloat() {
            return value;
        }

        @Override
        public int getSize() {
            // class size + float size
            return 4 + 4;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public TSDataType getDataType() {
            return TSDataType.FLOAT;
        }
    }

    public static class TsDouble extends TsPrimitiveType {
        /** value in double **/
        public double value;

        /**
         * init value
         * @param value
         */
        public TsDouble(double value) {
            this.value = value;
        }

        public double getDouble() {
            return value;
        }

        @Override
        public int getSize() {
            // class size + boolean size
            return 4 + 8;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public TSDataType getDataType() {
            return TSDataType.DOUBLE;
        }
    }

    public static class TsBinary extends TsPrimitiveType {
        /** value in Binary **/
        public Binary value;

        /**
         * init value
         * @param value
         */
        public TsBinary(Binary value) {
            this.value = value;
        }

        public Binary getBinary() {
            return value;
        }

        @Override
        public int getSize() {
            // class size + Binary size
            return 4 + 4 + value.getLength();
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public TSDataType getDataType() {
            return TSDataType.TEXT;
        }
    }

    /**
     * get corresponding subclass of input data type and set its value
     * @param dataType
     * @param v
     * @return corresponding implementation of {@code TsPrimitiveType}
     */
    public static TsPrimitiveType getByType(TSDataType dataType, Object v) {
        switch (dataType) {
            case BOOLEAN:
                return new TsPrimitiveType.TsBoolean((boolean) v);
            case INT32:
                return new TsPrimitiveType.TsInt((int) v);
            case INT64:
                return new TsPrimitiveType.TsLong((long) v);
            case FLOAT:
                return new TsPrimitiveType.TsFloat((float) v);
            case DOUBLE:
                return new TsPrimitiveType.TsDouble((double) v);
            case TEXT:
                return new TsPrimitiveType.TsBinary((Binary) v);
            default:
                throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
        }
    }
}
