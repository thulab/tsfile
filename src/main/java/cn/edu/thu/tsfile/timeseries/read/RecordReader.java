package cn.edu.thu.tsfile.timeseries.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description This class implements several read methods which can read data in different ways.<br>
 * This class provides some APIs for reading.
 * @author Jinrui Zhang
 *
 */

public class RecordReader {

	private static final Logger logger = LoggerFactory.getLogger(RecordReader.class);
	private ReaderManager readerManager;

	public RecordReader(String path) throws IOException {
		this.readerManager = new ReaderManager(new LocalFileInput(path));
	}

	public RecordReader(TSRandomAccessFileReader raf) throws IOException {
		this.readerManager = new ReaderManager(raf);
	}

	/**
	 * Read function 1#1: read one column without filter
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @return
	 * @throws IOException 
	 */
	public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize
			, String deltaObjectUID, String measurementId) throws IOException {
		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
		int i = 0;
		if (res != null) {
			i = res.getRowGroupIndex();
		}
		for (; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
			res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementId);
			res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			if (res.length >= fetchSize) {
				res.hasReadAll = false;
				break;
			}
		}
		return res;
	}

	/**
	 * Read function 1#2: read one column without filter from one specific
	 * RowGroupReader
	 * 
	 * @param rowGroupReader
	 * @param measurementId
	 * @return
	 * @throws IOException
	 */
	private DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize,
			RowGroupReader rowGroupReader, String measurementId) throws IOException {
		DynamicOneColumnData v = rowGroupReader.getValueReaders().get(measurementId).readOneColumn(res, fetchSize);
		return v;
	}

	/**
	 * Read function 1#3: read one column without filter from one specific
	 * RowGroupReader according to the index
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param idx
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
			String measurementId, int idx) throws IOException {
		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);

		if (idx >= rowGroupReaderList.size()) {
			logger.error("RowGroup index is not right. Index :" + idx + ". Size: " + rowGroupReaderList.size());
			return null;
		}

		RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
		res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementId);
		res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());

		return res;
	}

	/**
	 * Read function 1#4: read one column without filter from one specific
	 * RowGroupReader(s) according to the indexList
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param idxs
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
			String measurementId, ArrayList<Integer> idxs) throws IOException {

		List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();

		int i = 0;
		if (res != null) {
			i = res.getRowGroupIndex();
		}
		for (; i < idxs.size(); i++) {
			int idx = idxs.get(i);
			RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
			if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
				continue;
			}
			res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementId);
			res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			if (res.length >= fetchSize) {
				res.hasReadAll = false;
				break;
			}
		}
		return res;
	}
	
	/**
	 * Read funtion 2#1: read one column with filter
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
												   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
												   SingleSeriesFilterExpression valueFilter) throws IOException {

		int i = 0;
		if (res != null) {
			i = res.getRowGroupIndex();
		}

		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
		for (; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
			res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
			res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			if (res.length >= fetchSize) {
				res.hasReadAll = false;
				break;
			}
		}

		return res;
	}

	public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
			, SingleSeriesFilterExpression valueFilter) throws IOException{
		String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
		String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
		return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter);
	}
	
	public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
			, SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException{
		String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
		String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
		return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter, idxs);
	}
	
	/**
	 * Read funtion 2#2: read one column with filter from specific
	 * RowGroupReader
	 * 
	 * @param rowGroupReader,
	 *            specific RowGroupReader
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @return
	 * @throws IOException
	 */
	private DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize,
			RowGroupReader rowGroupReader, String measurementId, SingleSeriesFilterExpression timeFilter,
			SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {

		res = rowGroupReader.getValueReaders().get(measurementId).readOneColumnUseFilter(res, fetchSize, timeFilter,
				freqFilter, valueFilter);
		return res;
	}

	/**
	 * Read funtion 2#3: read one column with filter from specific
	 * RowGroupReader according to the index
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param idx,
	 *            index for RowGroupReader to be read.
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
			String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, int idx) throws IOException {

		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
		if (idx >= rowGroupReaderList.size()) {
			logger.error("RowGroup index is not right. Index :" + idx + ". Size: " + rowGroupReaderList.size());
			return null;
		}

		RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
		res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
		res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
		return res;
	}

	/**
	 * Read funtion 2#4: read one column with filter from specific
	 * RowGroupReader(s) according to the indexList
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param timeFilter
	 * @param freqFilter
	 * @param valueFilter
	 * @param idxs
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
			String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
			SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {

		List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();
		int i = 0;
		if (res != null) {
			i = res.getRowGroupIndex();
		}
		for (; i < idxs.size(); i++) {
			logger.info("GetValuesUseFilter and idxs. RowGroupIndex is :" + idxs.get(i));
			int idx = idxs.get(i);
			RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
			if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
				continue;
			}
			res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
			res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			if (res.length >= fetchSize) {
				res.hasReadAll = false;
				break;
			}
		}
		return res;
	}


	/**
	 * function 4#1: for cross getIndex. To get values in one column according
	 * to a time list
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param timeRet
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseTimeValue(String deltaObjectUID, String measurementId, long[] timeRet)
			throws IOException {
		DynamicOneColumnData res = null;
		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
		for (int i = 0; i < rowGroupReaderList.size(); i++) {
			RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
			if (i == 0) {
				res = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
				res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
			} else {
				DynamicOneColumnData tmpRes = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
				res.mergeRecord(tmpRes);
			}
		}
		return res;
	}

	/**
	 * function 4#2: for cross getIndex. To get values in one column according
	 * to a time list from specific RowGroupReader(s)
	 * 
	 * @param deltaObjectUID
	 * @param measurementId
	 * @param timeRet
	 * @param idxs
	 * @return
	 * @throws IOException
	 */
	public DynamicOneColumnData getValuesUseTimeValue(String deltaObjectUID, String measurementId, long[] timeRet,
			ArrayList<Integer> idxs) throws IOException {
		DynamicOneColumnData res = null;
		List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();

		boolean init = false;
		for (int i = 0; i < idxs.size(); i++) {
			int idx = idxs.get(i);
			RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
			if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
				continue;
			}
			if (!init) {
				res = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
				res.setDeltaObjectType(rowGroupReader.getDeltaObjectType());
				init = true;
			} else {
				DynamicOneColumnData tmpRes = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
				res.mergeRecord(tmpRes);
			}
		}
		return res;
	}


	/**
	 * Read time-value pairs whose time is be included in timeRet. WARNING: this
	 * function is only for "time" Series
	 * @param rowGroupReader RowGroupReader to be read
	 * @param measurement 
	 * @param timeRet
	 * @return
	 * @throws IOException
	 */
	private DynamicOneColumnData getValuesUseTimeValue(RowGroupReader rowGroupReader, String measurementId, long[] timeRet)
			throws IOException {
		return rowGroupReader.getValueReaders().get(measurementId).getValuesForGivenValues(timeRet);
	}

	public boolean isEnumsColumn(String deltaObjectUID, String sid) {
		List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
		for (RowGroupReader rowGroupReader : rowGroupReaderList) {
			if (rowGroupReader.getValueReaderForSpecificMeasurement(sid) == null) {
				continue;
			}
			if (rowGroupReader.getValueReaders().get(sid).getDataType() == TSDataType.ENUMS) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Get all series' schemas
	 */
	public ArrayList<SeriesSchema> getAllSeriesSchema() {
		HashMap<String, Integer> seriesMap = new HashMap<>();
		ArrayList<SeriesSchema> res = new ArrayList<>();
		List<RowGroupReader> rowGroupReaders = readerManager.getAllRowGroupReaders();
		for (RowGroupReader rgr : rowGroupReaders) {
			for (String measurement : rgr.seriesTypeMap.keySet()) {
				if (!seriesMap.containsKey(measurement)) {
					res.add(new SeriesSchema(measurement, rgr.seriesTypeMap.get(measurement), null));
					seriesMap.put(measurement, 1);
				}
			}
		}
		return res;
	}

	/**
	 * Get all deltaObjects' name
	 */
	public ArrayList<String> getAllDeltaObjects(){
		ArrayList<String> res = new ArrayList<>();
		HashMap<String, Integer> deltaObjectMap = new HashMap<>();
		List<RowGroupReader> rowGroupReaders = readerManager.getAllRowGroupReaders();
		for (RowGroupReader rgr : rowGroupReaders) {
			String deltaObjectUID = rgr.getDeltaObjectUID();
			if(!deltaObjectMap.containsKey(deltaObjectUID)){
				res.add(deltaObjectUID);
				deltaObjectMap.put(deltaObjectUID, 1);
			}
		}
		return res;
	}
	
	/**
	 * Get all series' schemas group by DeltaObject
	 */
	public HashMap<String, ArrayList<SeriesSchema>> getAllSeriesSchemasGroupByDeltaObject() {
		HashMap<String, ArrayList<SeriesSchema>> res = new HashMap<>();
		HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
		for (String deltaObjectUID : rowGroupReaders.keySet()) {
			HashMap<String, Integer> measurementMap = new HashMap<>();
			ArrayList<SeriesSchema> cols = new ArrayList<>();
			for (RowGroupReader rgr : rowGroupReaders.get(deltaObjectUID)) {
				for (String measurement : rgr.seriesTypeMap.keySet()) {
					if (!measurementMap.containsKey(measurement)) {
						cols.add(new SeriesSchema(measurement, rgr.seriesTypeMap.get(measurement), null));
						measurementMap.put(measurement, 1);
					}
				}
			}
			res.put(deltaObjectUID, cols);
		}
		return res;
	}

	/**
	 * Get all DeltaObjects' name with rowGroup count each.
	 */
	public HashMap<String, Integer> getDeltaObjectRowGroupCounts() {
		HashMap<String, Integer> res = new HashMap<>();
		HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
		for (String deltaObjectUID : rowGroupReaders.keySet()) {
			res.put(deltaObjectUID, rowGroupReaders.get(deltaObjectUID).size());
		}
		return res;
	}
	
	/**
	 * Get all DeltaObjects with type each.
	 */
	public HashMap<String, String> getDeltaObjectTypes() {
		HashMap<String, String> res = new HashMap<>();
		HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
		for (String deltaObjectUID : rowGroupReaders.keySet()) {

			RowGroupReader rgr = rowGroupReaders.get(deltaObjectUID).get(0);
			res.put(deltaObjectUID, rgr.getDeltaObjectType());
		}
		return res;
	}

	/**
	 * Get all RowGroups' offsets in the InputStream 
	 * @return res.get(i) represents the End-Position for specific rowGroup i in
	 *         this file.
	 */
	public ArrayList<Long> getRowGroupPosList() {
		ArrayList<Long> res = new ArrayList<>();
		long startPos = 0;
		for (RowGroupReader rowGroupReader : readerManager.getAllRowGroupReaders()) {
			long currentEndPos = rowGroupReader.getTotalByteSize() + startPos;
			res.add(currentEndPos);
			startPos = currentEndPos;
		}
		return res;
	}

	/**
	 * This method is used to create different kinds of {@code SingleSeriesFilterExpression} dynamically.
	 * @param deltaObject
	 * @param measurement
	 * @return A FilterSeries in specific type
	 */
	public FilterSeries<?> getColumnByMeasurementName(String deltaObject, String measurement) {
		TSDataType type = readerManager.getDataTypeBySeriesName(deltaObject, measurement);
		if (type == TSDataType.INT32) {
			return FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.INT64) {
			return FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.FLOAT) {
			return FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
		} else if (type == TSDataType.DOUBLE) {
			return FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
			// }else if(type == TSDataType.ENUMS){
			// return FilterApi.enumsColumn(deltaObject, measurement,
			// Column.VALUE_FILTER);
		} else if (type == TSDataType.BOOLEAN) {
			return FilterFactory.booleanFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
		}

		return null;
	}
	
	public List<RowGroupReader> getAllRowGroupReaders() {
		return readerManager.getAllRowGroupReaders();
	}
	
	public ReaderManager getReaderManager() {
		return readerManager;
	}
	
	public void close() throws IOException{
		readerManager.close();
	}
}
