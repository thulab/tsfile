package cn.edu.tsinghua.tsfile.timeseries.write.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.constant.StatisticConstant;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

/**
 * TSFileIOWriter is used to construct metadata and write data stored in memory
 * to output stream.
 *
 * @author kangrong
 */
public class TsFileIOWriter {

	/** version info **/
	public static final byte[] magicStringBytes;
	public static final TsFileMetaDataConverter metadataConverter = new TsFileMetaDataConverter();
	private static final Logger LOG = LoggerFactory.getLogger(TsFileIOWriter.class);

	static {
		// init version info
		magicStringBytes = BytesUtils.StringToBytes(TSFileConfig.MAGIC_STRING);
	}

	/** actual file writer **/
	private ITsRandomAccessFileWriter out;
	/** all RowGroupMetaData **/
	protected List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
	/** the RowGroupMetaData which is currently being written **/
	private RowGroupMetaData currentRowGroupMetaData;
	/** the TimeSeriesMetaData which is currently being written **/
	private TimeSeriesChunkMetaData currentChunkMetaData;

	/**
	 * empty constructor
	 */
	public TsFileIOWriter(){
		
	}
	
	public void setIOWriter(ITsRandomAccessFileWriter out){
		this.out = out;
	}

	/**
	 * start to write a new tsfile.
	 * @param file be used to output written data
	 * @throws IOException if I/O error occurs
	 */
	public TsFileIOWriter(File file) throws IOException {
		// init file writer
		this.out = new TsRandomAccessFileWriter(file);
		// write file header
		startFile();
	}

	/**
	 * start to write a new tsfile.
	 * @param output be used to output written data
	 * @throws IOException if I/O error occurs
	 */
	public TsFileIOWriter(ITsRandomAccessFileWriter output) throws IOException {
		// init file writer
		this.out = output;
		// write file header
		startFile();
	}

	/**
	 * This is just used to restore one TSFile from List of RowGroupMetaData and
	 * the offset.
	 *
	 * @param output
	 *            be used to output written data
	 * @param offset
	 *            offset to restore
	 * @param rowGroups
	 *            given a constructed row group list for fault recovery
	 * @throws IOException
	 *             if I/O error occurs
	 */
	public TsFileIOWriter(ITsRandomAccessFileWriter output, long offset, List<RowGroupMetaData> rowGroups)
			throws IOException {
		// set file writer
		this.out = output;
		// set file writer to offset
		out.seek(offset);
		// set RowGroup metadatas
		this.rowGroupMetaDatas = rowGroups;
	}

	/**
	 * Writes given <code>ListByteArrayOutputStream</code> to output stream.
	 * This method is called when total memory size exceeds the row group size
	 * threshold.
	 *
	 * @param bytes
	 *            - data of several pages which has been packed
	 * @throws IOException
	 *             if an I/O error occurs.
	 */
	public void writeBytesToStream(ListByteArrayOutputStream bytes) throws IOException {
		bytes.writeAllTo(out.getOutputStream());
	}

	/**
	 * write version info at the beginning of a new tsfile
	 * @throws IOException
	 */
	private void startFile() throws IOException {
		out.write(magicStringBytes);
	}

	/**
	 * start a {@linkplain RowGroupMetaData RowGroupMetaData}.
	 *
	 * @param recordCount
	 *            - the record count of this time series input in this stage
	 * @param deltaObjectId
	 *            - delta object id
	 */
	public void startRowGroup(long recordCount, String deltaObjectId) {
		LOG.debug("start row group:{}", deltaObjectId);
		// init a new RowGroupMetadata
		currentRowGroupMetaData = new RowGroupMetaData(deltaObjectId, recordCount, 0, new ArrayList<>(), "");
	}

	/**
	 * start a {@linkplain RowGroupMetaData RowGroupMetaData}. numOfRows is set to default value 0.
	 * @param deltaObjectId
	 */
	public void startRowGroup(String deltaObjectId) {
		LOG.debug("start row group:{}", deltaObjectId);
		// init a new RowGroupMetadata
		currentRowGroupMetaData = new RowGroupMetaData(deltaObjectId, 0, 0, new ArrayList<>(), "");
	}

	/**
	 * start a {@linkplain TimeSeriesChunkMetaData TimeSeriesChunkMetaData}.
	 *
	 * @param descriptor
	 *            - measurement of this time series
	 * @param compressionCodecName
	 *            - compression name of this time series
	 * @param tsDataType
	 *            - data type
	 * @param statistics
	 *            - statistic of the whole series
	 * @param maxTime
	 *            - maximum timestamp of the whole series in this series
	 * @param minTime
	 *            - minimum timestamp of the whole series in this series
	 * @throws IOException
	 *             if I/O error occurs
	 */
	public void startSeries(MeasurementDescriptor descriptor, CompressionTypeName compressionCodecName,
			TSDataType tsDataType, Statistics<?> statistics, long maxTime, long minTime) throws IOException {
		LOG.debug("start series:{}", descriptor);
		// init a new TimeSeriesChunkMetaData
		currentChunkMetaData = new TimeSeriesChunkMetaData(descriptor.getMeasurementId(), TSChunkType.VALUE,
				out.getPos(), compressionCodecName);
		// init a new TimeMetaData for TimeSeriesChunkMetaData
		TInTimeSeriesChunkMetaData t = new TInTimeSeriesChunkMetaData(tsDataType, minTime, maxTime);
		// add this new TimeMetaData to the new TimeSeriesChunkMetaData
		currentChunkMetaData.setTInTimeSeriesChunkMetaData(t);

		// init a new ValueMetaData for TimeSeriesChunkMetaData
		VInTimeSeriesChunkMetaData v = new VInTimeSeriesChunkMetaData(tsDataType);
		// init a new TsDigest
		TsDigest tsDigest = new TsDigest();
		Map<String, ByteBuffer> statisticsMap = new HashMap<>();
		// set statistics to statisticsMap
		// TODO add your statistics
		statisticsMap.put(StatisticConstant.MAX_VALUE,ByteBuffer.wrap(statistics.getMaxBytes()));
		statisticsMap.put(StatisticConstant.MIN_VALUE,ByteBuffer.wrap(statistics.getMinBytes()));
		statisticsMap.put(StatisticConstant.FIRST,ByteBuffer.wrap(statistics.getFirstBytes()));
		statisticsMap.put(StatisticConstant.SUM,ByteBuffer.wrap(statistics.getSumBytes()));
		statisticsMap.put(StatisticConstant.LAST,ByteBuffer.wrap(statistics.getLastBytes()));
		// add statistics info to the new TsDigest
		tsDigest.setStatistics(statisticsMap);

		// add TsDigest to the new ValueMetaData
		v.setDigest(tsDigest);
		// add new ValueMetaData info to MeasurementDescriptor
		descriptor.setDataValues(v);
		// add this new ValueMetaData to the new TimeSeriesChunkMetaData
		currentChunkMetaData.setVInTimeSeriesChunkMetaData(v);
	}

	/**
	 * end a {@linkplain TimeSeriesChunkMetaData TimeSeriesChunkMetaData}.
	 * @param size total byte size of this TimeSeriesChunkMetaData
	 * @param totalValueCount total data points num of this TimeSeriesChunkMetaData
	 */
	public void endSeries(long size, long totalValueCount) {
		LOG.debug("end series:{},totalvalue:{}", currentChunkMetaData, totalValueCount);
		// set total byte size to this TimeSeriesChunkMetaData
		currentChunkMetaData.setTotalByteSize(size);
		// set total data points num to this TimeSeriesChunkMetaData
		currentChunkMetaData.setNumRows(totalValueCount);
		// add this TimeSeriesChunkMetaData to current RowGroupMetaData
		currentRowGroupMetaData.addTimeSeriesChunkMetaData(currentChunkMetaData);
		// reset current TimeSeriesChunkMetaData to null
		currentChunkMetaData = null;
	}

	/**
	 * end a {@linkplain RowGroupMetaData RowGroupMetaData}.
	 * @param memSize total byte size of this RowGroupMetaData
	 */
	public void endRowGroup(long memSize) {
		// set total byte size to this RowGroupMetaData
		currentRowGroupMetaData.setTotalByteSize(memSize);
		// add this RowGroupMetaData to all RowGroupMetaDatas
		rowGroupMetaDatas.add(currentRowGroupMetaData);
		LOG.debug("end row group:{}", currentRowGroupMetaData);
		// reset this RowGroupMetaData to null
		currentRowGroupMetaData = null;
	}

	/**
	 * end a {@linkplain RowGroupMetaData RowGroupMetaData}.
	 * @param memSize total byte size of this RowGroupMetaData
	 * @param recordCount total data points num of this RowGroupMetaData
	 */
	public void endRowGroup(long memSize,long recordCount) {
		// set total byte size to this RowGroupMetaData
		currentRowGroupMetaData.setTotalByteSize(memSize);
		// set total data points num to this RowGroupMetaData
		currentRowGroupMetaData.setNumOfRows(recordCount);
		// add this RowGroupMetaData to all RowGroupMetaDatas
		rowGroupMetaDatas.add(currentRowGroupMetaData);
		LOG.debug("end row group:{}", currentRowGroupMetaData);
		// reset this RowGroupMetaData to null
		currentRowGroupMetaData = null;
	}

	/**
	 * write {@linkplain TsFileMetaData TSFileMetaData} to output stream and
	 * close it.
	 *
	 * @param schema
	 *            FileSchema
	 * @throws IOException
	 *             if I/O error occurs
	 */
	public void endFile(FileSchema schema) throws IOException {
		// 1. get all TimeSeriesMetadatas of this TsFile
		List<TimeSeriesMetadata> timeSeriesList = schema.getTimeSeriesMetadatas();
		LOG.debug("get time series list:{}", timeSeriesList);
		// clustering rowGroupMetadata and build the range

		/** Map of all TsDeltaObject (<deltaObjectID, TsDeltaObject>) **/
		Map<String, TsDeltaObject> tsDeltaObjectMap = new HashMap<>();
		String currentDeltaObject;
		TsRowGroupBlockMetaData currentTsRowGroupBlockMetaData;

		LinkedHashMap<String, TsRowGroupBlockMetaData> tsRowGroupBlockMetaDataMap = new LinkedHashMap<>();
		// 2. loop all RowGroupMetaData
		for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDatas) {
			// 2.1 get current deltaObjectID
			currentDeltaObject = rowGroupMetaData.getDeltaObjectID();
			// 2.2 if tsRowGroupBlockMetaDataMap doesn't contain this deltaObjectID,
			//		init a new TsRowGroupBlockMetaData and set its deltaObjectID
			//		and add it to tsRowGroupBlockMetaDataMap.
			if (!tsRowGroupBlockMetaDataMap.containsKey(currentDeltaObject)) {
				TsRowGroupBlockMetaData tsRowGroupBlockMetaData = new TsRowGroupBlockMetaData();
				tsRowGroupBlockMetaData.setDeltaObjectID(currentDeltaObject);
				tsRowGroupBlockMetaDataMap.put(currentDeltaObject, tsRowGroupBlockMetaData);
			}
			// 2.3 add current rowGroupMetaData to corresponding TsRowGroupBlockMetaData
			tsRowGroupBlockMetaDataMap.get(currentDeltaObject).addRowGroupMetaData(rowGroupMetaData);
		}
		/** Iterator of tsRowGroupBlockMetaDataMap **/
		Iterator<Map.Entry<String, TsRowGroupBlockMetaData>> iterator = tsRowGroupBlockMetaDataMap.entrySet()
				.iterator();
		/** used to record file writer offset **/
		long offset;
		long offsetIndex;
		/** size of RowGroupMetadataBlock in byte **/
		int metadataBlockSize;

		/** start time for a delta object **/
		long startTime;

		/** end time for a delta object **/
		long endTime;

		// 3. loop all TsRowGroupBlockMetaDatas in tsRowGroupBlockMetaDataMap
		while (iterator.hasNext()) {
			// 3.1 init startTime and endTime
			startTime = Long.MAX_VALUE;
			endTime = Long.MIN_VALUE;

			// 3.2 get current deltaObjectID and current TsRowGroupBlockMetaData
			Map.Entry<String, TsRowGroupBlockMetaData> entry = iterator.next();
			currentDeltaObject = entry.getKey();
			currentTsRowGroupBlockMetaData = entry.getValue();

			// 3.3 loop all RowGroupMetaDatas in current TsRowGroupBlockMetaData
			for (RowGroupMetaData rowGroupMetaData : currentTsRowGroupBlockMetaData.getRowGroups()) {
				// update startTime and endTime
				for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData
						.getTimeSeriesChunkMetaDataList()) {
					startTime = Long.min(startTime,
							timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime());
					endTime = Long.max(endTime, timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime());
				}
			}
			// 3.4 get current offset of file writer
			offsetIndex = out.getPos();
			// 3.5 flush tsRowGroupBlockMetaDatas in order
			ReadWriteThriftFormatUtils.writeRowGroupBlockMetadata(currentTsRowGroupBlockMetaData.convertToThrift(),
					out.getOutputStream());
			// 3.6 get current offset of file writer
			offset = out.getPos();
			// 3.7 create a new TsDeltaObject of offset, startTime and endTime
			TsDeltaObject tsDeltaObject = new TsDeltaObject(offsetIndex, (int) (offset - offsetIndex), startTime,
					endTime);
			// 3.8 add this new TsDeltaObject to tsDeltaObjectMap
			tsDeltaObjectMap.put(currentDeltaObject, tsDeltaObject);
		}

		// 4. create a new TsFileMetaData of all TsDeltaObjects, all TimeSeriesMetaDatas and version info
		TsFileMetaData tsFileMetaData = new TsFileMetaData(tsDeltaObjectMap, timeSeriesList,
				TSFileConfig.currentVersion);
		// 5. set properties of this TsFile to this new TsFileMetaData
		Map<String, String> props = schema.getProps();
		tsFileMetaData.setProps(props);
		// 6. flush this TsFileMetaData
		serializeTsFileMetadata(tsFileMetaData);
		// 7. close file writer
		out.close();
		LOG.info("output stream is closed");
	}

	/**
	 * get the length of normal OutputStream.
	 *
	 * @return - length of normal OutputStream
	 * @throws IOException
	 *             if I/O error occurs
	 */
	public long getPos() throws IOException {
		return out.getPos();
	}

	/**
	 * write a TsFileMetaData
	 * @param footer the TsFileMetaData to be written
	 * @throws IOException
	 */
	private void serializeTsFileMetadata(TsFileMetaData footer) throws IOException {
		// get start offset
		long footerIndex = out.getPos();
		LOG.debug("serialize the footer,file pos:{}", footerIndex);
		// write the TsFileMetaData
		TsFileMetaDataConverter metadataConverter = new TsFileMetaDataConverter();
		ReadWriteThriftFormatUtils.writeFileMetaData(metadataConverter.toThriftFileMetadata(footer),
				out.getOutputStream());
		LOG.debug("serialize the footer finished, file pos:{}", out.getPos());
		// write byte size of this TsFileMetaData
		out.write(BytesUtils.intToBytes((int) (out.getPos() - footerIndex)));
		// write version info
		out.write(magicStringBytes);
	}

	/**
	 * fill in output stream to complete row group threshold.
	 *
	 * @param diff
	 *            how many bytes that will be filled.
	 * @throws IOException
	 *             if diff is greater than Integer.max_value
	 */
	public void fillInRowGroup(long diff) throws IOException {
		if (diff <= Integer.MAX_VALUE) {
			out.write(new byte[(int) diff]);
		} else {
			throw new IOException("write too much blank byte array!array size:" + diff);
		}
	}

	/**
	 * Get the list of RowGroupMetaData in memory.
	 *
	 * @return - current list of RowGroupMetaData
	 */
	public List<RowGroupMetaData> getRowGroups() {
		return rowGroupMetaDatas;
	}
}
