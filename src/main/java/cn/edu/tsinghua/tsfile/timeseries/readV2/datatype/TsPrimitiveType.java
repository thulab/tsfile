package cn.edu.tsinghua.tsfile.timeseries.readV2.datatype;

import cn.edu.tsinghua.tsfile.common.utils.Binary;

/**
 * @author Jinrui Zhang
 */
public abstract class TsPrimitiveType {
    public boolean getBoolean() {
        throw new UnsupportedOperationException("getBoolean() is not supported for current sub-class");
    }

    public int getInt() {
        throw new UnsupportedOperationException("getInt() is not supported for current sub-class");
    }

    public long getLong() {
        throw new UnsupportedOperationException("getLong() is not supported for current sub-class");
    }

    public float getFloat() {
        throw new UnsupportedOperationException("getFloat() is not supported for current sub-class");
    }

    public double getDouble() {
        throw new UnsupportedOperationException("getDouble() is not supported for current sub-class");
    }

    public Binary getBinary() {
        throw new UnsupportedOperationException("getBinary() is not supported for current sub-class");
    }

    public abstract Object getValue();

    public abstract String getStringValue();

    public String toString() {
        return getStringValue();
    }

    public boolean equals(Object object) {
        return (object instanceof TsPrimitiveType) && (((TsPrimitiveType) object).getValue().equals(getValue()));
    }


    public static class TsBoolean extends TsPrimitiveType {

        public boolean value;

        public TsBoolean(boolean value) {
            this.value = value;
        }

        public boolean getBoolean() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return null;
        }
    }

    public static class TsInt extends TsPrimitiveType {
        public int value;

        public TsInt(int value) {
            this.value = value;
        }

        public int getInt() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }
    }

    public static class TsLong extends TsPrimitiveType {
        public long value;

        public TsLong(long value) {
            this.value = value;
        }

        public long getLong() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }

        @Override
        public Object getValue() {
            return value;
        }
    }

    public static class TsFloat extends TsPrimitiveType {
        public float value;

        public TsFloat(float value) {
            this.value = value;
        }

        public float getFloat() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }
    }

    public static class TsDouble extends TsPrimitiveType {
        public double value;

        public TsDouble(double value) {
            this.value = value;
        }

        public double getDouble() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }
    }

    public static class TsBinary extends TsPrimitiveType {
        public Binary value;

        public TsBinary(Binary value) {
            this.value = value;
        }

        public Binary getBinary() {
            return value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public String getStringValue() {
            return String.valueOf(value);
        }
    }
}
