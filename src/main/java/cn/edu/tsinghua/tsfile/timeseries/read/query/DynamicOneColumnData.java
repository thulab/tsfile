package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

import java.util.ArrayList;

/**
 * DynamicOneColumnData is a self-defined data structure which is optimized for different type
 * of values. This class can be viewed as a collection which is more efficient than ArrayList.
 *
 * @author Jinrui Zhang
 */
public class DynamicOneColumnData {

    private static int CAPACITY = 1000;

    public int rowGroupIndex = 0;
    public long pageOffset = -1;
    public long leftSize = -1;
    public boolean hasReadAll = false;
    public TSDataType dataType;
    public int curIdx;
    public int insertTrueIndex = 0;

    public int timeArrayIdx;  // the number of ArrayList in timeRet
    private int curTimeIdx;      // the index of current ArrayList in timeRet
    public int timeLength;   // the insert timestamp number of timeRet
    private int valueArrayIdx;// the number of ArrayList in valueRet
    private int curValueIdx;     // the index of current ArrayList in valueRet
    public int valueLength;  // the insert value number of valueRet

    public boolean hasEmptyTime;
    public int emptyTimeArrayIdx;
    public int curEmptyTimeIdx;
    public int emptyTimeLength;

    public ArrayList<long[]> timeRet;
    public ArrayList<long[]> emptyTimeRet;
    public ArrayList<boolean[]> booleanRet;
    public ArrayList<int[]> intRet;
    public ArrayList<long[]> longRet;
    public ArrayList<float[]> floatRet;
    public ArrayList<double[]> doubleRet;
    public ArrayList<Binary[]> binaryRet;

    // some variables that record overflow information
    public DynamicOneColumnData insertTrue;
    public DynamicOneColumnData updateTrue;
    public DynamicOneColumnData updateFalse;
    public SingleSeriesFilterExpression timeFilter;

    public DynamicOneColumnData() {
        dataType = null;
    }

    public DynamicOneColumnData(TSDataType type) {
        dataType = type;
    }

    /**
     * @param type       Data type to record for this DynamicOneColumnData
     * @param recordTime whether to record time value for this DynamicOneColumnData
     */
    public DynamicOneColumnData(TSDataType type, boolean recordTime) {
        init(type, recordTime, false);
    }

    public DynamicOneColumnData(TSDataType type, boolean recordTime, boolean hasEmptyTime) {
        this.hasEmptyTime = hasEmptyTime;
        init(type, recordTime, hasEmptyTime);
    }

    public void init(TSDataType type, boolean recordTime, boolean hasEmptyTime) {
        this.dataType = type;
        this.valueArrayIdx = 0;
        this.curValueIdx = 0;
        this.valueLength = 0;
        this.curIdx = 0;
        CAPACITY = TSFileConfig.dynamicDataSize;

        if (recordTime) {
            timeRet = new ArrayList<>();
            timeRet.add(new long[CAPACITY]);
            timeArrayIdx = 0;
            curTimeIdx = 0;
            timeLength = 0;
        }

        if (hasEmptyTime) {
            emptyTimeRet = new ArrayList<>();
            emptyTimeRet.add(new long[CAPACITY]);
            emptyTimeArrayIdx = 0;
            curEmptyTimeIdx = 0;
            emptyTimeLength = 0;
        }

        switch (dataType) {
            case BOOLEAN:
                booleanRet = new ArrayList<>();
                booleanRet.add(new boolean[CAPACITY]);
                break;
            case INT32:
                intRet = new ArrayList<>();
                intRet.add(new int[CAPACITY]);
                break;
            case INT64:
                longRet = new ArrayList<>();
                longRet.add(new long[CAPACITY]);
                break;
            case FLOAT:
                floatRet = new ArrayList<>();
                floatRet.add(new float[CAPACITY]);
                break;
            case DOUBLE:
                doubleRet = new ArrayList<>();
                doubleRet.add(new double[CAPACITY]);
                break;
            case TEXT:
                binaryRet = new ArrayList<>();
                binaryRet.add(new Binary[CAPACITY]);
                break;
            case ENUMS:
                intRet = new ArrayList<>();
                intRet.add(new int[CAPACITY]);
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public void putTime(long v) {
        if (curTimeIdx == CAPACITY) {
            this.timeRet.add(new long[CAPACITY]);
            timeArrayIdx++;
            curTimeIdx = 0;
        }
        (timeRet.get(timeArrayIdx))[curTimeIdx++] = v;
        timeLength++;
    }

    public void putEmptyTime(long v) {
        if (curEmptyTimeIdx == CAPACITY) {
            this.emptyTimeRet.add(new long[CAPACITY]);
            emptyTimeArrayIdx++;
            curEmptyTimeIdx = 0;
            // System.out.println(v + "-" + emptyTimeLength + " - " +emptyTimeArrayIdx);
        }
        (emptyTimeRet.get(emptyTimeArrayIdx))[curEmptyTimeIdx++] = v;
        emptyTimeLength++;
    }

    /**
     * add all time and value from another DynamicOneColumnData to self.
     *
     * @param col DynamicOneColumnData to be merged
     */
    public void mergeRecord(DynamicOneColumnData col) {
        for (int i = 0; i < col.timeLength; i++) {
            putTime(col.getTime(i));
        }
        switch (dataType) {
            case BOOLEAN:
                for (int i = 0; i < col.valueLength; i++) {
                    putBoolean(col.getBoolean(i));
                }
                break;
            case INT32:
                for (int i = 0; i < col.valueLength; i++) {
                    putInt(col.getInt(i));
                }
                break;
            case INT64:
                for (int i = 0; i < col.valueLength; i++) {
                    putLong(col.getLong(i));
                }
                break;
            case FLOAT:
                for (int i = 0; i < col.valueLength; i++) {
                    putFloat(col.getFloat(i));
                }
                break;
            case DOUBLE:
                for (int i = 0; i < col.valueLength; i++) {
                    putDouble(col.getDouble(i));
                }
                break;
            case TEXT:
                for (int i = 0; i < col.valueLength; i++) {
                    putBinary(col.getBinary(i));
                }
                break;
            case ENUMS:
                for (int i = 0; i < col.valueLength; i++) {
                    putBinary(col.getBinary(i));
                }
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public void putBoolean(boolean v) {
        if (curValueIdx == CAPACITY) {
            if (this.booleanRet.size() <= valueArrayIdx + 1) {
                this.booleanRet.add(new boolean[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.booleanRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putInt(int v) {
        if (curValueIdx == CAPACITY) {
            if (this.intRet.size() <= valueArrayIdx + 1) {
                this.intRet.add(new int[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.intRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putLong(long v) {
        if (curValueIdx == CAPACITY) {
            if (this.longRet.size() <= valueArrayIdx + 1) {
                this.longRet.add(new long[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.longRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putFloat(float v) {
        if (curValueIdx == CAPACITY) {
            if (this.floatRet.size() <= valueArrayIdx + 1) {
                this.floatRet.add(new float[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.floatRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putDouble(double v) {
        if (curValueIdx == CAPACITY) {
            if (this.doubleRet.size() <= valueArrayIdx + 1) {
                this.doubleRet.add(new double[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.doubleRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putBinary(Binary v) {
        if (curValueIdx == CAPACITY) {
            if (this.binaryRet.size() <= valueArrayIdx + 1) {
                this.binaryRet.add(new Binary[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.binaryRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    /**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception.
     */
    private void rangeCheck(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("Index is negative: " + idx);
        }
        if (idx >= valueLength) {
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + valueLength);
        }
    }

    /**
     * Checks if the given index is in range.  If not, throws an appropriate
     * runtime exception.
     */
    private void rangeCheckForTime(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("Index is negative: " + idx);
        }
        if (idx >= timeLength) {
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + valueLength);
        }
    }

    private void rangeCheckForEmptyTime(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("Index is negative: " + idx);
        }
        if (idx >= emptyTimeLength) {
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + valueLength);
        }
    }

    public boolean getBoolean(int idx) {
        rangeCheck(idx);
        return this.booleanRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setBoolean(int idx, boolean v) {
        rangeCheck(idx);
        this.booleanRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public int getInt(int idx) {
        rangeCheck(idx);
        return this.intRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setInt(int idx, int v) {
        rangeCheck(idx);
        this.intRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public long getLong(int idx) {
        rangeCheck(idx);
        return this.longRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setLong(int idx, long v) {
        rangeCheck(idx);
        this.longRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public float getFloat(int idx) {
        rangeCheck(idx);
        return this.floatRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setFloat(int idx, float v) {
        rangeCheck(idx);
        this.floatRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public double getDouble(int idx) {
        rangeCheck(idx);
        return this.doubleRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setDouble(int idx, double v) {
        rangeCheck(idx);
        this.doubleRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public Binary getBinary(int idx) {
        rangeCheck(idx);
        return this.binaryRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setBinary(int idx, Binary v) {
        this.binaryRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public long getTime(int idx) {
        rangeCheckForTime(idx);
        return this.timeRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setTime(int idx, long v) {
        rangeCheckForTime(idx);
        this.timeRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public long getEmptyTime(int idx) {
        rangeCheckForEmptyTime(idx);
        return this.emptyTimeRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public long[] getTimeAsArray() {
        long[] res = new long[timeLength];
        for (int i = 0; i < timeLength; i++) {
            res[i] = timeRet.get(i / CAPACITY)[i % CAPACITY];
        }
        return res;
    }

    public void putAnObject(Object v) {
        switch (dataType) {
            case BOOLEAN:
                putBoolean((boolean) v);
                break;
            case INT32:
                putInt((int) v);
                break;
            case INT64:
                putLong((long) v);
                break;
            case FLOAT:
                putFloat((float) v);
                break;
            case DOUBLE:
                putDouble((double) v);
                break;
            case TEXT:
                putBinary((Binary) v);
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public Comparable<?> getAnObject(int idx) {
        switch (dataType) {
            case BOOLEAN:
                return getBoolean(idx);
            case DOUBLE:
                return getDouble(idx);
            case TEXT:
                return getBinary(idx);
            case FLOAT:
                return getFloat(idx);
            case INT32:
                return getInt(idx);
            case INT64:
                return getLong(idx);
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public void setAnObject(int idx, Comparable<?> v) {
        switch (dataType) {
            case BOOLEAN:
                setBoolean(idx, (Boolean) v);
                break;
            case DOUBLE:
                setDouble(idx, (Double) v);
                break;
            case TEXT:
                setBinary(idx, (Binary) v);
                break;
            case FLOAT:
                setFloat(idx, (Float) v);
                break;
            case INT32:
                setInt(idx, (Integer) v);
                break;
            case INT64:
                setLong(idx, (Long) v);
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public String getStringValue(int idx) {
        switch (dataType) {
            case BOOLEAN:
                return String.valueOf(getBoolean(idx));
            case INT32:
                return String.valueOf(getInt(idx));
            case INT64:
                return String.valueOf(getLong(idx));
            case FLOAT:
                return String.valueOf(getFloat(idx));
            case DOUBLE:
                return String.valueOf(getDouble(idx));
            case TEXT:
                return String.valueOf(getBinary(idx));
            case ENUMS:
                return String.valueOf(getBinary(idx));
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public String getStringTimeValuePair(int idx) {
        String v;
        switch (dataType) {
            case BOOLEAN:
                v = String.valueOf(getBoolean(idx));
                break;
            case INT32:
                v = String.valueOf(getInt(idx));
                break;
            case INT64:
                v = String.valueOf(getLong(idx));
                break;
            case FLOAT:
                v = String.valueOf(getFloat(idx));
                break;
            case DOUBLE:
                v = String.valueOf(getDouble(idx));
                break;
            case TEXT:
                v = String.valueOf(getBinary(idx));
                break;
            case ENUMS:
                v = String.valueOf(getBinary(idx));
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
        String t = String.valueOf(getTime(idx));
        StringBuffer sb = new StringBuffer();
        sb.append(t);
        sb.append("\t");
        sb.append(v);
        return sb.toString();
    }

    public void putAValueFromDynamicOneColumnData(DynamicOneColumnData B, int idx) {
        switch (dataType) {
            case BOOLEAN:
                putBoolean(B.getBoolean(idx));
                break;
            case INT32:
                putInt(B.getInt(idx));
                break;
            case INT64:
                putLong(B.getLong(idx));
                break;
            case FLOAT:
                putFloat(B.getFloat(idx));
                break;
            case DOUBLE:
                putDouble(B.getDouble(idx));
                break;
            case TEXT:
                putBinary(B.getBinary(idx));
                break;
            case ENUMS:
                putBinary(B.getBinary(idx));
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    /**
     * Remove the data whose index position is between size and valueLength.
     * @param size the data whose position is greater than size will be removed
     */
    public void rollBack(int size) {
        //rollback the length
        valueLength -= size;
        timeLength -= size;
        if (size <= curValueIdx) {
            curValueIdx -= size;
            curTimeIdx -= size;
        } else {
            size -= curValueIdx;
            size += CAPACITY;
            while (size > CAPACITY) {
                switch (dataType) {
                    case BOOLEAN:
                        booleanRet.remove(valueArrayIdx);
                        break;
                    case INT32:
                        intRet.remove(valueArrayIdx);
                        break;
                    case INT64:
                        longRet.remove(valueArrayIdx);
                        break;
                    case FLOAT:
                        floatRet.remove(valueArrayIdx);
                        break;
                    case DOUBLE:
                        doubleRet.remove(valueArrayIdx);
                        break;
                    case TEXT:
                        binaryRet.remove(valueArrayIdx);
                        break;
                    case ENUMS:
                        binaryRet.remove(valueArrayIdx);
                        break;
                    default:
                        throw new UnSupportedDataTypeException(String.valueOf(dataType));
                }
                valueArrayIdx--;
                timeRet.remove(timeArrayIdx);
                timeArrayIdx--;

                size -= CAPACITY;
            }
            curValueIdx = CAPACITY - size;
        }
    }

    /**
     * Remove the last empty time.
     */
    public void removeLastEmptyTime() {
        emptyTimeLength -= 1;
        curEmptyTimeIdx -= 1;

        // curEmptyTimeIdx will never == -1
        if (curEmptyTimeIdx == 0) {
            if (emptyTimeArrayIdx == 0) {
                curEmptyTimeIdx = 0;
            } else {
                curEmptyTimeIdx = CAPACITY;
                emptyTimeRet.remove(emptyTimeArrayIdx);
                emptyTimeArrayIdx -= 1;
            }
        }
    }

    public void clearData() {
        this.init(dataType, true, hasEmptyTime);
    }

    public DynamicOneColumnData sub(int startPos) {
        return sub(startPos, this.valueLength - 1);
    }

    /**
     * Extract the needed data between start position and end position.
     *
     * @param startPos start position of index
     * @param endPos end position of index
     * @return the new DynamicOneColumnData whose data is equals to position startPos and position endPos
     */
    public DynamicOneColumnData sub(int startPos, int endPos) {
        DynamicOneColumnData subRes = new DynamicOneColumnData(dataType, true);
        for (int i = startPos; i <= endPos; i++) {
            subRes.putTime(getTime(i));
            subRes.putAValueFromDynamicOneColumnData(this, i);
        }
        return subRes;
    }

    public void putOverflowInfo(DynamicOneColumnData insertTrue, DynamicOneColumnData updateTrue,
                                DynamicOneColumnData updateFalse, SingleSeriesFilterExpression timeFilter) {
        this.insertTrue = insertTrue;
        this.updateTrue = updateTrue;
        this.updateFalse = updateFalse;
        this.timeFilter = timeFilter;
    }

    public void copyFetchInfoTo(DynamicOneColumnData oneColRet) {
        oneColRet.rowGroupIndex = this.rowGroupIndex;
        oneColRet.pageOffset = this.pageOffset;
        oneColRet.leftSize = this.leftSize;
        oneColRet.hasReadAll = this.hasReadAll;
        oneColRet.insertTrueIndex = this.insertTrueIndex;
        oneColRet.insertTrue = this.insertTrue;
        oneColRet.updateFalse = this.updateFalse;
        oneColRet.updateTrue = this.updateTrue;
        oneColRet.timeFilter = this.timeFilter;
    }

    public void plusRowGroupIndexAndInitPageOffset() {

        this.rowGroupIndex++;
        //RowGroupIndex's change means that The pageOffset should be updateTo the value in next RowGroup.
        //But we don't know the value, so set the pageOffset to -1. And we calculate the accuracy value
        //in the reading procedure.
        this.pageOffset = -1;
    }

    public int getRowGroupIndex() {
        return this.rowGroupIndex;
    }


}
