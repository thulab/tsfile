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
	private double first;
	private double sum;

	@Override
	public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
		max = BytesUtils.bytesToDouble(maxBytes);
		min = BytesUtils.bytesToDouble(minBytes);
	}

	@Override
	public void updateStats(double value) {
		if (this.isEmpty) {
			initializeStats(value, value, value, value);
			isEmpty = false;
		} else {
			updateStats(value, value, value, value);
		}
	}

	private void updateStats(double minValue, double maxValue, double firstValue, double sumValue) {
		if (minValue < min) {
			min = minValue;
		}
		if (maxValue > max) {
			max = maxValue;
		}
		sum += sumValue;
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
	public Double getFirst() {
		return first;
	}

	@Override
	public double getSum() {
		return sum;
	}

	@Override
	protected void mergeStatisticsValue(Statistics<?> stats) {
		DoubleStatistics doubleStats = (DoubleStatistics) stats;
		if (this.isEmpty) {
			initializeStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(), doubleStats.getSum());
			isEmpty = false;
		} else {
			updateStats(doubleStats.getMin(), doubleStats.getMax(), doubleStats.getFirst(), doubleStats.getSum());
		}

	}

	public void initializeStats(double min, double max, double first, double sum) {
		this.min = min;
		this.max = max;
		this.first = first;
		this.sum = sum;
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
	public byte[] getFirstBytes() {
		return BytesUtils.doubleToBytes(first);
	}

	@Override
	public byte[] getSumBytes() {
		return BytesUtils.doubleToBytes(sum);
	}

	@Override
	public String toString() {
		return "[max:" + max + ",min:" + min + ",first:" + first + ",sum:" + sum + "]";
	}

}
