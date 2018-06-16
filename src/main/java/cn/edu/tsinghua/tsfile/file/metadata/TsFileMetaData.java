package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure
 */
public class TsFileMetaData {

    private Map<String, TsDeltaObjectMetadata> deltaObjectMap;

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
     * String for application that wrote this file. This should be in the format <Application> version
     * <App Version> (build <App Build Hash>). e.g. impala version 1.0 (build SHA-1_hash_code)
     */
    private String createdBy;

    private long firstTimeSeriesMetadataOffset;

    private long lastTimeSeriesMetadataOffset;

    private long firstTsDeltaObjectMetadataOffset;

    private long lastTsDeltaObjectMetadataOffset;

    public TsFileMetaData() {
    }

    /**
     * @param timeSeriesList       - time series info list
     * @param currentVersion       - current version
     */
    public TsFileMetaData(Map<String, TsDeltaObjectMetadata> deltaObjectMap, List<TimeSeriesMetadata> timeSeriesList, int currentVersion) {
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

    @Override
    public String toString() {
        return String.format("TSFMetaData { DeltaOjectMap: %s, timeSeries list %s, current version %d }", deltaObjectMap,
                timeSeriesList, currentVersion);
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

	public Map<String, TsDeltaObjectMetadata> getDeltaObjectMap() {
		return deltaObjectMap;
	}

	public void setDeltaObjectMap(Map<String, TsDeltaObjectMetadata> deltaObjectMap) {
		this.deltaObjectMap = deltaObjectMap;
	}

	public boolean containsDeltaObject(String DeltaObjUID) {
        return this.deltaObjectMap.containsKey(DeltaObjUID);
    }

    public TsDeltaObjectMetadata getDeltaObject(String DeltaObjUID) {
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

    public long getFirstTimeSeriesMetadataOffset() {
        return firstTimeSeriesMetadataOffset;
    }

    public void setFirstTimeSeriesMetadataOffset(long firstTimeSeriesMetadataOffset) {
        this.firstTimeSeriesMetadataOffset = firstTimeSeriesMetadataOffset;
    }

    public long getLastTimeSeriesMetadataOffset() {
        return lastTimeSeriesMetadataOffset;
    }

    public void setLastTimeSeriesMetadataOffset(long lastTimeSeriesMetadataOffset) {
        this.lastTimeSeriesMetadataOffset = lastTimeSeriesMetadataOffset;
    }

    public long getFirstTsDeltaObjectMetadataOffset() {
        return firstTsDeltaObjectMetadataOffset;
    }

    public void setFirstTsDeltaObjectMetadataOffset(long firstTsDeltaObjectMetadataOffset) {
        this.firstTsDeltaObjectMetadataOffset = firstTsDeltaObjectMetadataOffset;
    }

    public long getLastTsDeltaObjectMetadataOffset() {
        return lastTsDeltaObjectMetadataOffset;
    }

    public void setLastTsDeltaObjectMetadataOffset(long lastTsDeltaObjectMetadataOffset) {
        this.lastTsDeltaObjectMetadataOffset = lastTsDeltaObjectMetadataOffset;
    }
}
