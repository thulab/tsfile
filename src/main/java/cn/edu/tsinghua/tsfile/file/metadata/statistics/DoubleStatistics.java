package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for double type
 *
 * @author kangrong
 */
public class DoubleStatistics extends Statistics<Double> {
    private double max;
    private double min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToDouble(maxBytes);
        min = BytesUtils.bytesToDouble(minBytes);
    }

    @Override
    public void updateStats(double value) {
        if (this.isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
        }
    }

    private void updateStats(double minValue, double maxValue) {
        if (minValue < min) {
            min = minValue;
        }
        if (maxValue > max) {
            max = maxValue;
        }
    }

    @Override
    public Double getMax() {
        return max;
    }

    @Override
    public Double getMin() {
        return min;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        DoubleStatistics intStats = (DoubleStatistics) stats;
        if (this.isEmpty) {
            initializeStats(intStats.getMin(), intStats.getMax());
            isEmpty = false;
        } else {
            updateStats(intStats.getMin(), intStats.getMax());
        }

    }

    public void initializeStats(double min, double max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.doubleToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.doubleToBytes(min);
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + "]";
    }
}
