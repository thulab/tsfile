package cn.edu.thu.tsfile.file.metadata.statistics;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.common.utils.BytesUtils;

/**
 * Statistics for string type
 * @author CGF
 */
public class BinaryStatistics extends Statistics<Binary>{
    private Binary max;
    private Binary min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = new Binary(maxBytes);
        min = new Binary(minBytes);
    }

    @Override
    public Binary getMin() {
        return min;
    }

    @Override
    public Binary getMax() {
        return max;
    }

    public void initializeStats(Binary min, Binary max) {
        this.min = min;
        this.max = max;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        BinaryStatistics stringStats = (BinaryStatistics) stats;
        if (isEmpty) {
            initializeStats(stringStats.getMin(), stringStats.getMax());
            isEmpty = false;
        } else {
            updateStats(stringStats.getMin(), stringStats.getMax());
        }
    }

    @Override
    public void updateStats(Binary value) {
        if (isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
            isEmpty = false;
        }
    }

    private void updateStats(Binary minValue, Binary maxValue) {
        if (minValue.compareTo(min) < 0) {
            min = minValue;
        }
        if (maxValue.compareTo(max) > 0) {
            max = maxValue;
        }
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.StringToBytes(max.getStringValue());
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.StringToBytes(min.getStringValue());
    }
}
