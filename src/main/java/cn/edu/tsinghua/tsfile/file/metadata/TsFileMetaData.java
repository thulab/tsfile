package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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

    public int serialize(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        if(deltaObjectMap == null){
            byteLen += ReadWriteToBytesUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteToBytesUtils.write(deltaObjectMap.size(), outputStream);
            for (Map.Entry<String, TsDeltaObjectMetadata> entry : deltaObjectMap.entrySet()) {
                byteLen += ReadWriteToBytesUtils.write(entry.getKey(), outputStream);
                byteLen += ReadWriteToBytesUtils.write(entry.getValue(), outputStream);
            }
        }

        if(timeSeriesList == null){
            byteLen += ReadWriteToBytesUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteToBytesUtils.write(timeSeriesList.size(), outputStream);
            for(TimeSeriesMetadata timeSeriesMetadata : timeSeriesList)
                byteLen += ReadWriteToBytesUtils.write(timeSeriesMetadata, outputStream);
        }

        byteLen += ReadWriteToBytesUtils.write(currentVersion, outputStream);

        byteLen += ReadWriteToBytesUtils.writeIsNull(createdBy, outputStream);
        if(createdBy != null)byteLen += ReadWriteToBytesUtils.write(createdBy, outputStream);

        byteLen += ReadWriteToBytesUtils.write(firstTimeSeriesMetadataOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(lastTimeSeriesMetadataOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(firstTsDeltaObjectMetadataOffset, outputStream);
        byteLen += ReadWriteToBytesUtils.write(lastTsDeltaObjectMetadataOffset, outputStream);

        return byteLen;
    }

    public int serialize(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        if(deltaObjectMap == null){
            byteLen += ReadWriteToBytesUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteToBytesUtils.write(deltaObjectMap.size(), buffer);
            for (Map.Entry<String, TsDeltaObjectMetadata> entry : deltaObjectMap.entrySet()) {
                byteLen += ReadWriteToBytesUtils.write(entry.getKey(), buffer);
                byteLen += ReadWriteToBytesUtils.write(entry.getValue(), buffer);
            }
        }

        if(timeSeriesList == null){
            byteLen += ReadWriteToBytesUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteToBytesUtils.write(timeSeriesList.size(), buffer);
            for(TimeSeriesMetadata timeSeriesMetadata : timeSeriesList)
                byteLen += ReadWriteToBytesUtils.write(timeSeriesMetadata, buffer);
        }

        byteLen += ReadWriteToBytesUtils.write(currentVersion, buffer);

        byteLen += ReadWriteToBytesUtils.writeIsNull(createdBy, buffer);
        if(createdBy != null)byteLen += ReadWriteToBytesUtils.write(createdBy, buffer);

        byteLen += ReadWriteToBytesUtils.write(firstTimeSeriesMetadataOffset, buffer);
        byteLen += ReadWriteToBytesUtils.write(lastTimeSeriesMetadataOffset, buffer);
        byteLen += ReadWriteToBytesUtils.write(firstTsDeltaObjectMetadataOffset, buffer);
        byteLen += ReadWriteToBytesUtils.write(lastTsDeltaObjectMetadataOffset, buffer);

        return byteLen;
    }

    public static TsFileMetaData deserialize(InputStream inputStream) throws IOException {
        TsFileMetaData fileMetaData = new TsFileMetaData();

        int size = ReadWriteToBytesUtils.readInt(inputStream);
        if(size > 0) {
            Map<String, TsDeltaObjectMetadata> deltaObjectMap = new HashMap<>();
            String key;
            TsDeltaObjectMetadata value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteToBytesUtils.readString(inputStream);
                value = ReadWriteToBytesUtils.readDeltaObjectMetadata(inputStream);
                deltaObjectMap.put(key, value);
            }
            fileMetaData.deltaObjectMap = deltaObjectMap;
        }

        size = ReadWriteToBytesUtils.readInt(inputStream);
        if(size > 0) {
            List<TimeSeriesMetadata> timeSeriesList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                timeSeriesList.add(ReadWriteToBytesUtils.readTimeSeriesMetadata(inputStream));
            }
            fileMetaData.timeSeriesList = timeSeriesList;
        }

        fileMetaData.currentVersion = ReadWriteToBytesUtils.readInt(inputStream);

        if(ReadWriteToBytesUtils.readIsNull(inputStream))
            fileMetaData.createdBy = ReadWriteToBytesUtils.readString(inputStream);

        fileMetaData.firstTimeSeriesMetadataOffset = ReadWriteToBytesUtils.readLong(inputStream);
        fileMetaData.lastTimeSeriesMetadataOffset = ReadWriteToBytesUtils.readLong(inputStream);
        fileMetaData.firstTsDeltaObjectMetadataOffset = ReadWriteToBytesUtils.readLong(inputStream);
        fileMetaData.lastTsDeltaObjectMetadataOffset = ReadWriteToBytesUtils.readLong(inputStream);

        return fileMetaData;
    }

    public static TsFileMetaData deserialize(ByteBuffer buffer) throws IOException {
        TsFileMetaData fileMetaData = new TsFileMetaData();

        int size = ReadWriteToBytesUtils.readInt(buffer);
        if(size > 0) {
            Map<String, TsDeltaObjectMetadata> deltaObjectMap = new HashMap<>();
            String key;
            TsDeltaObjectMetadata value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteToBytesUtils.readString(buffer);
                value = ReadWriteToBytesUtils.readDeltaObjectMetadata(buffer);
                deltaObjectMap.put(key, value);
            }
            fileMetaData.deltaObjectMap = deltaObjectMap;
        }

        size = ReadWriteToBytesUtils.readInt(buffer);
        if(size > 0) {
            List<TimeSeriesMetadata> timeSeriesList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                timeSeriesList.add(ReadWriteToBytesUtils.readTimeSeriesMetadata(buffer));
            }
            fileMetaData.timeSeriesList = timeSeriesList;
        }

        fileMetaData.currentVersion = ReadWriteToBytesUtils.readInt(buffer);

        if(ReadWriteToBytesUtils.readIsNull(buffer))
            fileMetaData.createdBy = ReadWriteToBytesUtils.readString(buffer);

        fileMetaData.firstTimeSeriesMetadataOffset = ReadWriteToBytesUtils.readLong(buffer);
        fileMetaData.lastTimeSeriesMetadataOffset = ReadWriteToBytesUtils.readLong(buffer);
        fileMetaData.firstTsDeltaObjectMetadataOffset = ReadWriteToBytesUtils.readLong(buffer);
        fileMetaData.lastTsDeltaObjectMetadataOffset = ReadWriteToBytesUtils.readLong(buffer);

        return fileMetaData;
    }
}
