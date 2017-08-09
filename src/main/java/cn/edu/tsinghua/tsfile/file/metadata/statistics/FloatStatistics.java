package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

/**
 * Statistics for float type
 *
 * @author kangrong
 */
public class FloatStatistics extends Statistics<Float> {
    private float max;
    private float min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToFloat(maxBytes);
        min = BytesUtils.bytesToFloat(minBytes);
    }

    @Override
    public void updateStats(float value) {
        if (this.isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
        }
    }

    private void updateStats(float minValue, float maxValue) {
        if (minValue < min) {
            min = minValue;
        }
        if (maxValue > max) {
            max = maxValue;
        }
    }

    @Override
    public Float getMax() {
        return max;
    }

    @Override
    public Float getMin() {
        return min;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        FloatStatistics intStats = (FloatStatistics) stats;
        if (isEmpty) {
            initializeStats(intStats.getMin(), intStats.getMax());
            isEmpty = false;
        } else {
            updateStats(intStats.getMin(), intStats.getMax());
        }

    }

    public void initializeStats(float min, float max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.floatToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.floatToBytes(min);
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + "]";
    }
}
