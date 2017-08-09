package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * @author CGF
 */
public class BooleanStatistics extends Statistics<Boolean> {
    private boolean max;
    private boolean min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToBool(maxBytes);
        min = BytesUtils.bytesToBool(minBytes);
    }

    @Override
    public void updateStats(boolean value) {
        if (isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
            isEmpty = false;
        }
    }

    private void updateStats(boolean minValue, boolean maxValue) {
        if (!minValue && min) {
            min = minValue;
        }
        if (maxValue && !max) {
            max = maxValue;
        }
    }

    @Override
    public Boolean getMax() {
        return max;
    }

    @Override
    public Boolean getMin() {
        return min;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        BooleanStatistics intStats = (BooleanStatistics) stats;
        if (isEmpty) {
            initializeStats(intStats.getMin(), intStats.getMax());
            isEmpty = false;
        } else {
            updateStats(intStats.getMin(), intStats.getMax());
        }

    }

    public void initializeStats(boolean min, boolean max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.boolToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.boolToBytes(min);
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + "]";
    }
}
