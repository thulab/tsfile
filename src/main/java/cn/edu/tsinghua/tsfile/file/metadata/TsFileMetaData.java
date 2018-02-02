package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure
 */
public class TsFileMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetaData.class);

    private Map<String, TsDeltaObject> deltaObjectMap;

    /**
     * TSFile schema for this file. This schema contains metadata for all the time series. The schema
     * is represented as a list.
     */
    private List<TimeSeriesMetadata> timeSeriesList;

    /**
     * Version of this file
     */
    private int currentVersion;

    /**
     * Optional json metadata
     */
    private List<String> jsonMetaData;

    /**
     * String for application that wrote this file. This should be in the format <Application> version
     * <App Version> (build <App Build Hash>). e.g. impala version 1.0 (build SHA-1_hash_code)
     */
    private String createdBy;

    /**
     * User specified props
     */
    private Map<String, String> props;

    public TsFileMetaData() {
    }

    /**
     * @param timeSeriesList       - time series info list
     * @param currentVersion       - current version
     */
    public TsFileMetaData(Map<String, TsDeltaObject> deltaObjectMap, List<TimeSeriesMetadata> timeSeriesList, int currentVersion) {
        this.props = new HashMap<>();
        this.deltaObjectMap = deltaObjectMap;
        this.timeSeriesList = timeSeriesList;
        this.currentVersion = currentVersion;
    }

    /**
     * add time series metadata to list. THREAD NOT SAFE
     * @param timeSeries series metadata to add
     */
    public void addTimeSeriesMetaData(TimeSeriesMetadata timeSeries) {
        if (timeSeriesList == null) {
            timeSeriesList = new ArrayList<>();
        }
        timeSeriesList.add(timeSeries);
    }

//    /**
//     * get all delta object uid and their types
//     *
//     * @return set of {@code Pair<delta-object-uid, delta-object-type>}
//     */
//    public Set<Pair<String, String>> getAllDeltaObjects() {
//        // Pair<delta-object-uid, delta-object-type>
//        Set<Pair<String, String>> deltaObjectSet = new HashSet<Pair<String, String>>();
//        if (rowGroupMetadataList != null) {
//            for (RowGroupMetaData rowGroup : rowGroupMetadataList) {
//                deltaObjectSet.add(
//                        new Pair<String, String>(rowGroup.getDeltaObjectUID(), rowGroup.getDeltaObjectType()));
//            }
//        }
//        return deltaObjectSet;
//    }

    @Override
    public String toString() {
        return String.format("TSFMetaData { DeltaOjectMap: %s, timeSeries list %s, current version %d }", deltaObjectMap,
                timeSeriesList, currentVersion);
    }

    public int write(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteToBytesUtils.writeIsNull(deltaObjectMap, outputStream);
        if(deltaObjectMap != null){
            byteLen += ReadWriteToBytesUtils.write(deltaObjectMap.size(), outputStream);
            for(Map.Entry<String, TsDeltaObject> entry : deltaObjectMap.entrySet()){
                byteLen += ReadWriteToBytesUtils.write(entry.getKey(), outputStream);
                byteLen += ReadWriteToBytesUtils.write(entry.getValue(), outputStream);
            }
        }

        byteLen += ReadWriteToBytesUtils.writeIsNull(timeSeriesList, outputStream);
        if(timeSeriesList != null){
            byteLen += ReadWriteToBytesUtils.write(timeSeriesList.size(), outputStream);
            for(TimeSeriesMetadata timeSeriesMetadata : timeSeriesList){
                byteLen += ReadWriteToBytesUtils.write(timeSeriesMetadata, outputStream);
            }
        }

        byteLen += ReadWriteToBytesUtils.write(currentVersion, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(jsonMetaData, outputStream);
        if(jsonMetaData != null)byteLen += ReadWriteToBytesUtils.write(jsonMetaData, TSDataType.TEXT, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(createdBy, outputStream);
        if(createdBy != null)byteLen += ReadWriteToBytesUtils.write(createdBy, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(props, outputStream);
        if(props != null){
            byteLen += ReadWriteToBytesUtils.write(props.size(), outputStream);
            for(Map.Entry<String, String> entry : props.entrySet()){
                byteLen += ReadWriteToBytesUtils.write(entry.getKey(), outputStream);
                byteLen += ReadWriteToBytesUtils.write(entry.getValue(), outputStream);
            }
        }

        return byteLen;
    }

    public void read(InputStream inputStream) throws IOException {
        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            deltaObjectMap = new HashMap<>();
            int size = ReadWriteToBytesUtils.readInt(inputStream);
            String key;
            TsDeltaObject value;
            for(int i = 0;i < size;i++) {
                key = ReadWriteToBytesUtils.readString(inputStream);
                value = ReadWriteToBytesUtils.readTsDeltaObject(inputStream);
                deltaObjectMap.put(key, value);
            }
        }

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            timeSeriesList = new ArrayList<>();
            int size = ReadWriteToBytesUtils.readInt(inputStream);
            for(int i = 0;i < size;i++)
                timeSeriesList.add(ReadWriteToBytesUtils.readTimeSeriesMetadata(inputStream));
        }

        currentVersion = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            jsonMetaData = ReadWriteToBytesUtils.readStringList(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            createdBy = ReadWriteToBytesUtils.readString(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream)){
            props = new HashMap<>();
            int size = ReadWriteToBytesUtils.readInt(inputStream);
            String key;
            String value;
            for(int i = 0;i < size;i++) {
                key = ReadWriteToBytesUtils.readString(inputStream);
                value = ReadWriteToBytesUtils.readString(inputStream);
                props.put(key, value);
            }
        }
    }

    public List<TimeSeriesMetadata> getTimeSeriesList() {
        return timeSeriesList;
    }

    public void setTimeSeriesList(List<TimeSeriesMetadata> timeSeriesList) {
        this.timeSeriesList = timeSeriesList;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public List<String> getJsonMetaData() {
        return jsonMetaData;
    }

    public void setJsonMetaData(List<String> jsonMetaData) {
        this.jsonMetaData = jsonMetaData;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public void addProp(String key, String value) {
        props.put(key, value);
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> properties) {
        this.props.clear();
        this.props.putAll(properties);
    }

    public String getProp(String key) {
        if (props.containsKey(key))
            return props.get(key);
        else
            return null;
    }

	public Map<String, TsDeltaObject> getDeltaObjectMap() {
		return deltaObjectMap;
	}

	public void setDeltaObjectMap(Map<String, TsDeltaObject> deltaObjectMap) {
		this.deltaObjectMap = deltaObjectMap;
	}

	public boolean containsDeltaObject(String DeltaObjUID) {
        return this.deltaObjectMap.containsKey(DeltaObjUID);
    }

    public TsDeltaObject getDeltaObject(String DeltaObjUID) {
        return this.deltaObjectMap.get(DeltaObjUID);
    }

    //For Tsfile-Spark-Connector
    public boolean containsMeasurement(String measurement) {
        for(TimeSeriesMetadata ts: timeSeriesList ){
            if(ts.getMeasurementUID().equals(measurement)) {
                return true;
            }
        }
        return false;
    }

    //For Tsfile-Spark-Connector
    public TSDataType getType(String measurement) throws IOException{
        for(TimeSeriesMetadata ts: timeSeriesList ){
            if(ts.getMeasurementUID().equals(measurement)) {
                return ts.getType();
            }
        }
        throw new IOException("Measurement " + measurement + " does not exist in the current file.");
    }
}
