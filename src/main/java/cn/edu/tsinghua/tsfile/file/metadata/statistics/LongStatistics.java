package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for long type
 *
 * @author kangrong
 */
public class LongStatistics extends Statistics<Long> {
    private long max;
    private long min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToLong(maxBytes);
        min = BytesUtils.bytesToLong(minBytes);
    }

    @Override
    public Long getMin() {
        return min;
    }

    @Override
    public Long getMax() {
        return max;
    }

    @Override
    public void updateStats(long value) {
        if (isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
        }
    }

    @Override
    public void updateStats(long minValue, long maxValue) {
        if (minValue < min) {
            min = minValue;
        }
        if (maxValue > max) {
            max = maxValue;
        }
    }


    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        LongStatistics longStats = (LongStatistics) stats;
        if (isEmpty) {
            initializeStats(longStats.getMin(), longStats.getMax());
            isEmpty = false;
        } else {
            updateStats(longStats.getMin(), longStats.getMax());
        }

    }


    public void initializeStats(long min, long max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.longToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.longToBytes(min);
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + "]";
    }
}
