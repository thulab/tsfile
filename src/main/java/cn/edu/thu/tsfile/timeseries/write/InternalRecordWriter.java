package cn.edu.thu.tsfile.timeseries.write;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;
import cn.edu.thu.tsfile.timeseries.write.series.RowGroupWriterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * {@code InternalRecordWriter<T>} is the entrance for writing processing. It
 * receives a record in type of {@code T} and send it to responding row group
 * write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user
 * should call {@code close()} method to flush the last data outside and close
 * the normal outputStream and error outputStream.
 *
 * @param <T>
 *            - record type
 * @author kangrong
 */
public abstract class InternalRecordWriter<T> {
	private static final Logger LOG = LoggerFactory.getLogger(InternalRecordWriter.class);
	private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;

	protected long recordCount = 0;
	private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
	protected Map<String, IRowGroupWriter> groupWriters = new HashMap<String, IRowGroupWriter>();

	protected final TSFileIOWriter deltaFileWriter;
	protected final WriteSupport<T> writeSupport;
	protected final FileSchema schema;
	protected final int pageSize;

	private long rowGroupSizeThreshold;
	protected final long primaryRowGroupSize;
	private int oneRowMaxSize;

	public InternalRecordWriter(TSFileConfig conf, TSFileIOWriter tsfileWriter, WriteSupport<T> writeSupport,
								FileSchema schema) {
		this.deltaFileWriter = tsfileWriter;
		this.writeSupport = writeSupport;
		this.schema = schema;
		this.primaryRowGroupSize = conf.rowGroupSize;
		this.oneRowMaxSize = schema.getCurrentRowMaxSize();
		this.rowGroupSizeThreshold = primaryRowGroupSize - oneRowMaxSize;
		this.pageSize = conf.pageSize;
		writeSupport.init(groupWriters);
	}

	/**
	 * Confirm whether the record is legal. If legal, add it into this
	 * RecordWriter.
	 *
	 * @param record
	 *            - a record responding a line
	 * @return - whether the record has been added into RecordWriter legally
	 * @throws IOException
	 */
	abstract protected boolean checkRowGroup(T record) throws IOException;

	/**
	 * write a record in type of T
	 *
	 * @param record
	 *            - record responding a data line
	 */
	public void write(T record) throws IOException, WriteProcessException {
		if (checkRowGroup(record)) {
			writeSupport.write(record);
			++recordCount;
			checkMemorySize();
		}

	}

	/**
	 * <b>Note that</b>, before calling this method, all {@code IRowGroupWriter}
	 * instance existing in {@code groupWriters} have been reset for next
	 * writing stage, thus we don't add new {@code IRowGroupWriter} if its
	 * deltaObjecyId has existed.
	 *
	 * @param deltaObjectId
	 *            - delta object to be add
	 */
	protected void addGroupToInternalRecordWriter(String deltaObjectId) {
		if (!groupWriters.containsKey(deltaObjectId)) {
			IRowGroupWriter groupWriter = new RowGroupWriterImpl(deltaObjectId, schema, pageSize);
			groupWriters.put(deltaObjectId, groupWriter);
		}
	}

	/**
	 * calculate total memory size occupied by all RowGroupWriter instances
	 *
	 * @return
	 */
	public long updateMemSizeForAllGroup() {
		int memTotalSize = 0;
		for (IRowGroupWriter group : groupWriters.values()) {
			memTotalSize += group.updateMaxGroupMemSize();
		}
		return memTotalSize;
	}

	/**
	 * check occupied memory size, if it exceeds the rowGroupSize threshold,
	 * flush them to given OutputStream.
	 *
	 * @throws IOException
	 */
	protected void checkMemorySize() throws IOException {
		if (recordCount >= recordCountForNextMemCheck) {
			long memSize = updateMemSizeForAllGroup();
			if (memSize > rowGroupSizeThreshold) {
				LOG.info("start_write_row_group, memory space occupy:" + memSize);
				flushRowGroup(true);
				recordCountForNextMemCheck = rowGroupSizeThreshold / oneRowMaxSize;
			} else {
				recordCountForNextMemCheck = (rowGroupSizeThreshold - memSize) / oneRowMaxSize;
			}
		}
	}

	/**
	 * flush the data in all series writers and their page writers to
	 * outputStream.
	 *
	 * @throws IOException
	 */
	protected void flushRowGroup(boolean isFillRowGroup) throws IOException {
		// at the present stage, just flush one block
		String deltaType = schema.getDeltaType();
		if (recordCount > 0) {
			long totalMemStart = deltaFileWriter.getPos();
			for (String deltaObjectId : schema.getDeltaObjectAppearedSet()) {
				long memSize = deltaFileWriter.getPos();
				deltaFileWriter.startRowGroup(recordCount, deltaObjectId, deltaType);
				IRowGroupWriter groupWriter = groupWriters.get(deltaObjectId);
				groupWriter.flushToFileWriter(deltaFileWriter);
				deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - memSize);
			}
			long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
			if (isFillRowGroup) {
				fillInRowGroupSize(actualTotalRowGroupSize);
				LOG.info("total row group size:{}, actual:{}, filled:{}", primaryRowGroupSize, actualTotalRowGroupSize,
						primaryRowGroupSize - actualTotalRowGroupSize);
			}
			else
				LOG.info("total row group size:{}, row group is not filled", actualTotalRowGroupSize);
			LOG.info("write row group end");
			recordCount = 0;
			reset();
		}
	}

	protected void fillInRowGroupSize(long actualRowGroupSize) throws IOException {
		if (actualRowGroupSize > primaryRowGroupSize)
			LOG.warn("too large actual row group size!:actual:{},threshold:{}", actualRowGroupSize,
					primaryRowGroupSize);
		deltaFileWriter.fillInRowGroup(primaryRowGroupSize - actualRowGroupSize);
	}

	/**
	 * <b>Note that</b> we don't need to reset RowGroupWriter explicitly, since
	 * after calling {@code flushToFileWriter()}, RowGroupWriter resets itself.
	 */
	private void reset() {
		schema.resetUnusedDeltaObjectId(groupWriters);
	}

	/**
	 * calling this method to write the last data remaining in memory and close
	 * the normal and error OutputStream
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		LOG.info("start close file");
		updateMemSizeForAllGroup();
		flushRowGroup(false);
		deltaFileWriter.endFile();
	}
}
