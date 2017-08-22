package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;

import java.math.BigDecimal;

/**
 * Statistics for BigDecimal type
 *
 * @author kangrong
 */
public class BigDecimalStatistics extends Statistics<BigDecimal> {
    private BigDecimal max;
    private BigDecimal min;


    @Override
    public void updateStats(BigDecimal value) {
        if (this.isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
        }
    }

    private void updateStats(BigDecimal minValue, BigDecimal maxValue) {
        if (minValue.doubleValue() < min.doubleValue()) {
            min = minValue;
        }
        if (maxValue.doubleValue() > max.doubleValue()) {
            max = maxValue;
        }
    }

    @Override
    public BigDecimal getMax() {
        return max;
    }

    @Override
    public BigDecimal getMin() {
        return min;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        BigDecimalStatistics intStats = (BigDecimalStatistics) stats;
        if (this.isEmpty) {
            initializeStats(intStats.getMin(), intStats.getMax());
            isEmpty = false;
        } else {
            updateStats(intStats.getMin(), intStats.getMax());
        }

    }

    public void initializeStats(BigDecimal min, BigDecimal max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.doubleToBytes(max.doubleValue());
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.doubleToBytes(min.doubleValue());
    }

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = new BigDecimal(BytesUtils.bytesToDouble(maxBytes));
        min = new BigDecimal(BytesUtils.bytesToDouble(minBytes));
    }

    @Override
    public String toString() {
        return "[max:" + max + ",min:" + min + "]";
    }
}
