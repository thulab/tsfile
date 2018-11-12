package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure
 */
public class TsFileMetaData {

    private Map<String, TsDeltaObjectMetadata> deltaObjectMap;

    /**
     * TSFile schema for this file. This schema contains metadata for all the time series. The schema
     * is represented as a list.
     */
    private List<TimeSeriesMetadata> timeSeriesList;//FIXME this filed can be replaced by timeSeriesMetadataMap
    private Map<String, TimeSeriesMetadata> timeSeriesMetadataMap;

    /**
     * Version of this file
     */
    private int currentVersion;

    /**
     * String for application that wrote this file. This should be in the format <Application> version
     * <App Version> (build <App Build Hash>). e.g. impala version 1.0 (build SHA-1_hash_code)
     */
    private String createdBy;

    /**
     * relevant offset to the start of file metadata
     */
    private long firstTimeSeriesMetadataOffset;

    /**
     * relevant offset to the start of file metadata
     */
    private long lastTimeSeriesMetadataOffset;

    /**
     * relevant offset to the start of file metadata
     */
    private long firstTsDeltaObjectMetadataOffset;

    /**
     * relevant offset to the start of file metadata
     */
    private long lastTsDeltaObjectMetadataOffset;

    public TsFileMetaData() {
    }

    /**
     * @param timeSeriesList - time series info list
     * @param currentVersion - current version
     */
    public TsFileMetaData(Map<String, TsDeltaObjectMetadata> deltaObjectMap, List<TimeSeriesMetadata> timeSeriesList, int currentVersion) {
        this.deltaObjectMap = deltaObjectMap;
        this.timeSeriesList = timeSeriesList;
        this.timeSeriesMetadataMap = new HashMap<>(timeSeriesList.size());
        for (TimeSeriesMetadata metadata : timeSeriesList) {
            timeSeriesMetadataMap.put(metadata.getMeasurementUID(), metadata);
        }
        this.currentVersion = currentVersion;
    }

    /**
     * add time series metadata to list. THREAD NOT SAFE
     *
     * @param timeSeries series metadata to add
     */
    public void addTimeSeriesMetaData(TimeSeriesMetadata timeSeries) {
        if (timeSeriesList == null) {
            timeSeriesList = new ArrayList<>();
            timeSeriesMetadataMap = new HashMap<>();
        }
        timeSeriesList.add(timeSeries);
        timeSeriesMetadataMap.put(timeSeries.getMeasurementUID(), timeSeries);
    }

    @Override
    public String toString() {
        return "TsFileMetaData{" +
                "deltaObjectMap=" + deltaObjectMap +
                ", timeSeriesMetadataMap=" + timeSeriesMetadataMap +
                ", currentVersion=" + currentVersion +
                ", createdBy='" + createdBy + '\'' +
                '}';
    }

    public List<TimeSeriesMetadata> getTimeSeriesList() {
        return timeSeriesList;
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

    public boolean containsMeasurement(String measurement) {
        return timeSeriesMetadataMap.containsKey(measurement);
    }


    public TSDataType getType(String measurement) {
        if (containsMeasurement(measurement))
            return timeSeriesMetadataMap.get(measurement).getType();
        else
            return null;
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

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        if (deltaObjectMap == null) {
            byteLen += ReadWriteIOUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteIOUtils.write(deltaObjectMap.size(), outputStream);
            for (Map.Entry<String, TsDeltaObjectMetadata> entry : deltaObjectMap.entrySet()) {//TODO 应该排序
                byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
                byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);
            }
        }

        if (timeSeriesList == null) {
            byteLen += ReadWriteIOUtils.write(0, outputStream);
        } else {
            byteLen += ReadWriteIOUtils.write(timeSeriesList.size(), outputStream);
            for (TimeSeriesMetadata timeSeriesMetadata : timeSeriesList)
                byteLen += ReadWriteIOUtils.write(timeSeriesMetadata, outputStream);
        }

        byteLen += ReadWriteIOUtils.write(currentVersion, outputStream);

        byteLen += ReadWriteIOUtils.writeIsNull(createdBy, outputStream);
        if (createdBy != null) byteLen += ReadWriteIOUtils.write(createdBy, outputStream);

        //TODO: 赋值四个offset

        byteLen += ReadWriteIOUtils.write(firstTimeSeriesMetadataOffset, outputStream);
        byteLen += ReadWriteIOUtils.write(lastTimeSeriesMetadataOffset, outputStream);
        byteLen += ReadWriteIOUtils.write(firstTsDeltaObjectMetadataOffset, outputStream);
        byteLen += ReadWriteIOUtils.write(lastTsDeltaObjectMetadataOffset, outputStream);

        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        if (deltaObjectMap == null) {
            byteLen += ReadWriteIOUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteIOUtils.write(deltaObjectMap.size(), buffer);
            for (Map.Entry<String, TsDeltaObjectMetadata> entry : deltaObjectMap.entrySet()) {
                byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
                byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);
            }
        }

        if (timeSeriesList == null) {
            byteLen += ReadWriteIOUtils.write(0, buffer);
        } else {
            byteLen += ReadWriteIOUtils.write(timeSeriesList.size(), buffer);
            for (TimeSeriesMetadata timeSeriesMetadata : timeSeriesList)
                byteLen += ReadWriteIOUtils.write(timeSeriesMetadata, buffer);
        }

        byteLen += ReadWriteIOUtils.write(currentVersion, buffer);

        byteLen += ReadWriteIOUtils.writeIsNull(createdBy, buffer);
        if (createdBy != null) byteLen += ReadWriteIOUtils.write(createdBy, buffer);

        byteLen += ReadWriteIOUtils.write(firstTimeSeriesMetadataOffset, buffer);
        byteLen += ReadWriteIOUtils.write(lastTimeSeriesMetadataOffset, buffer);
        byteLen += ReadWriteIOUtils.write(firstTsDeltaObjectMetadataOffset, buffer);
        byteLen += ReadWriteIOUtils.write(lastTsDeltaObjectMetadataOffset, buffer);

        return byteLen;
    }

    public static TsFileMetaData deserializeFrom(InputStream inputStream) throws IOException {
        TsFileMetaData fileMetaData = new TsFileMetaData();

        int size = ReadWriteIOUtils.readInt(inputStream);
        if (size > 0) {
            Map<String, TsDeltaObjectMetadata> deltaObjectMap = new HashMap<>();
            String key;
            TsDeltaObjectMetadata value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(inputStream);
                value = TsDeltaObjectMetadata.deserializeFrom(inputStream);
                deltaObjectMap.put(key, value);
            }
            fileMetaData.deltaObjectMap = deltaObjectMap;
        }

        size = ReadWriteIOUtils.readInt(inputStream);
        if (size > 0) {
            List<TimeSeriesMetadata> timeSeriesList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                fileMetaData.addTimeSeriesMetaData(ReadWriteIOUtils.readTimeSeriesMetadata(inputStream));
            }
        }

        fileMetaData.currentVersion = ReadWriteIOUtils.readInt(inputStream);

        if (ReadWriteIOUtils.readIsNull(inputStream))
            fileMetaData.createdBy = ReadWriteIOUtils.readString(inputStream);

        fileMetaData.firstTimeSeriesMetadataOffset = ReadWriteIOUtils.readLong(inputStream);
        fileMetaData.lastTimeSeriesMetadataOffset = ReadWriteIOUtils.readLong(inputStream);
        fileMetaData.firstTsDeltaObjectMetadataOffset = ReadWriteIOUtils.readLong(inputStream);
        fileMetaData.lastTsDeltaObjectMetadataOffset = ReadWriteIOUtils.readLong(inputStream);

        return fileMetaData;
    }

    public static TsFileMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
        TsFileMetaData fileMetaData = new TsFileMetaData();

        int size = ReadWriteIOUtils.readInt(buffer);
        if (size > 0) {
            Map<String, TsDeltaObjectMetadata> deltaObjectMap = new HashMap<>();
            String key;
            TsDeltaObjectMetadata value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(buffer);
                value = TsDeltaObjectMetadata.deserializeFrom(buffer);
                deltaObjectMap.put(key, value);
            }
            fileMetaData.deltaObjectMap = deltaObjectMap;
        }

        size = ReadWriteIOUtils.readInt(buffer);
        if (size > 0) {
            List<TimeSeriesMetadata> timeSeriesList = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                fileMetaData.addTimeSeriesMetaData(ReadWriteIOUtils.readTimeSeriesMetadata(buffer));
            }
        }

        fileMetaData.currentVersion = ReadWriteIOUtils.readInt(buffer);

        if (ReadWriteIOUtils.readIsNull(buffer))
            fileMetaData.createdBy = ReadWriteIOUtils.readString(buffer);

        fileMetaData.firstTimeSeriesMetadataOffset = ReadWriteIOUtils.readLong(buffer);
        fileMetaData.lastTimeSeriesMetadataOffset = ReadWriteIOUtils.readLong(buffer);
        fileMetaData.firstTsDeltaObjectMetadataOffset = ReadWriteIOUtils.readLong(buffer);
        fileMetaData.lastTsDeltaObjectMetadataOffset = ReadWriteIOUtils.readLong(buffer);

        return fileMetaData;
    }


}
