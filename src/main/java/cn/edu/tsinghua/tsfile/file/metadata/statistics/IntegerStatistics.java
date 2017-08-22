package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for int type
 *
 * @author kangrong
 */
public class IntegerStatistics extends Statistics<Integer> {
    private int max;
    private int min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToInt(maxBytes);
        min = BytesUtils.bytesToInt(minBytes);
    }

    @Override
    public void updateStats(int value) {
        if (isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
            isEmpty = false;
        }
    }

    private void updateStats(int minValue, int maxValue) {
        if (minValue < min) {
            min = minValue;
        }
        if (maxValue > max) {
            max = maxValue;
        }
    }

    @Override
    public Integer getMax() {
        return max;
    }

    @Override
    public Integer getMin() {
        return min;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        IntegerStatistics intStats = (IntegerStatistics) stats;
        if (isEmpty) {
            initializeStats(intStats.getMin(), intStats.getMax());
            isEmpty = false;
        } else {
            updateStats(intStats.getMin(), intStats.getMax());
        }

    }

    public void initializeStats(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.intToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.intToBytes(min);
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + "]";
    }
}
