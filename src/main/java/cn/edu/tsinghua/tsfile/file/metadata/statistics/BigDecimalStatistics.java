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
	private BigDecimal first;
	private double sum;

	@Override
	public void updateStats(BigDecimal value) {
		if (this.isEmpty) {
			initializeStats(value, value, value, value.doubleValue());
			isEmpty = false;
		} else {
			updateStats(value, value, value, value.doubleValue());
		}
	}

	private void updateStats(BigDecimal minValue, BigDecimal maxValue, BigDecimal firstValue, double sumValue) {
		if (minValue.doubleValue() < min.doubleValue()) {
			min = minValue;
		}
		if (maxValue.doubleValue() > max.doubleValue()) {
			max = maxValue;
		}
		sum += sumValue;
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
	public BigDecimal getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		BigDecimalStatistics bigDecimalStats = (BigDecimalStatistics) stats;
		if (this.isEmpty) {
			initializeStats(bigDecimalStats.getMin(), bigDecimalStats.getMax(), bigDecimalStats.getFirst(),
					bigDecimalStats.getSum());
			isEmpty = false;
		} else {
			updateStats(bigDecimalStats.getMin(), bigDecimalStats.getMax(), bigDecimalStats.getFirst(),
					bigDecimalStats.getSum());
		}

	}

	public void initializeStats(BigDecimal min, BigDecimal max, BigDecimal first, double sum) {
		this.min = min;
		this.max = max;
		this.first = first;
		this.sum = sum;
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
	public byte[] getFirstBytes() {
		return BytesUtils.doubleToBytes(first.doubleValue());
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = new BigDecimal(BytesUtils.bytesToDouble(maxBytes));
		min = new BigDecimal(BytesUtils.bytesToDouble(minBytes));
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + "]";
	}
}
