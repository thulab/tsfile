package cn.edu.thu.tsfile.timeseries.write;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;

/**
 * {@code WriteSupport<T>} is template class for different input formats. Each
 * input format should implement the method write(T record).
 * 
 * @author kangrong
 *
 * @param <T>
 */
abstract public class WriteSupport<T> {
	/**
	 * initialize WriteSupport
	 * 
	 * @param groupWriters
	 *            - a deltaObjectId-IRowGroupWriter map
	 */
	public abstract void init(Map<String, IRowGroupWriter> groupWriters);

	/**
	 * write method is used to receive record T and arrange it to suitable
	 * RowGroupWriter.
	 * 
	 * @param record
	 *            - input record
	 * @throws WriteProcessException
	 */
	public abstract void write(T record) throws IOException, WriteProcessException;

	/**
	 * query the data in memory
	 * 
	 * @param deltaObjectId
	 * @param measurementId
	 * @return
	 */
	public abstract Pair<DynamicOneColumnData, Pair<List<ByteArrayInputStream>, CompressionTypeName>> query(
			String deltaObjectId, String measurementId);
}
