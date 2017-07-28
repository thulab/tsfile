package cn.edu.thu.tsfile.file.metadata;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.converter.IConverter;
import cn.edu.thu.tsfile.format.FileMetaData;
import cn.edu.thu.tsfile.format.TimeSeries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure
 */
public class TSFileMetaData implements IConverter<FileMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TSFileMetaData.class);

    /**
     * Row groups in this file
     */
    private List<RowGroupMetaData> rowGroupMetadataList;

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

    public TSFileMetaData() {
    }

    /**
     * @param rowGroupMetadataList - rowGroup level metadata
     * @param timeSeriesList       - time series info list
     * @param currentVersion       - current version
     */
    public TSFileMetaData(List<RowGroupMetaData> rowGroupMetadataList, List<TimeSeriesMetadata> timeSeriesList,
                          int currentVersion) {
        props = new HashMap<>();
        this.rowGroupMetadataList = rowGroupMetadataList;
        this.timeSeriesList = timeSeriesList;
        this.currentVersion = currentVersion;
    }

    /**
     * add row group metadata to rowGroups. THREAD NOT SAFE
     * @param rowGroup - row group metadata to add
     */
    public void addRowGroupMetaData(RowGroupMetaData rowGroup) {
        if (rowGroupMetadataList == null) {
            rowGroupMetadataList = new ArrayList<RowGroupMetaData>();
        }
        rowGroupMetadataList.add(rowGroup);
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

    /**
     * get all delta object uid and their types
     *
     * @return set of {@code Pair<delta-object-uid, delta-object-type>}
     */
    public Set<Pair<String, String>> getAllDeltaObjects() {
        // Pair<delta-object-uid, delta-object-type>
        Set<Pair<String, String>> deltaObjectSet = new HashSet<Pair<String, String>>();
        if (rowGroupMetadataList != null) {
            for (RowGroupMetaData rowGroup : rowGroupMetadataList) {
                deltaObjectSet.add(
                        new Pair<String, String>(rowGroup.getDeltaObjectUID(), rowGroup.getDeltaObjectType()));
            }
        }
        return deltaObjectSet;
    }

    @Override
    public String toString() {
        return String.format(
                "TSFMetaData { RowGroupsMetaData: %s, timeSeries list %s, current version %d }", rowGroupMetadataList,
                timeSeriesList, currentVersion);
    }

    public List<RowGroupMetaData> getRowGroups() {
        return rowGroupMetadataList;
    }

    public void setRowGroups(List<RowGroupMetaData> rowGroupMetadataList) {
        this.rowGroupMetadataList = rowGroupMetadataList;
    }

    /**
     * create file metadata in thrift format. For more information about file metadata
     * in cn.edu.thu.tsfile.format package, see FileMetaData in tsfile-format
     *
     * @return file metadata in thrift format
     */
    @Override
    public FileMetaData convertToThrift() {
        try {
            List<TimeSeries> timeSeriesListInThrift = null;
            if (timeSeriesList != null) {
                timeSeriesListInThrift = new ArrayList<TimeSeries>();
                for (TimeSeriesMetadata timeSeries : timeSeriesList) {
                    timeSeriesListInThrift.add(timeSeries.convertToThrift());
                }
            }

            long numOfRows = 0;
            List<cn.edu.thu.tsfile.format.RowGroupMetaData> rowGroupMetaDataListInThrift = null;
            if (rowGroupMetadataList != null) {
                rowGroupMetaDataListInThrift =
                        new ArrayList<cn.edu.thu.tsfile.format.RowGroupMetaData>();
                for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
                    numOfRows += rowGroupMetaData.getNumOfRows();
                    rowGroupMetaDataListInThrift.add(rowGroupMetaData.convertToThrift());
                }
            }
            FileMetaData metaDataInThrift = new FileMetaData(currentVersion, timeSeriesListInThrift, numOfRows,
                    rowGroupMetaDataListInThrift);
            metaDataInThrift.setCreated_by(createdBy);
            metaDataInThrift.setJson_metadata(jsonMetaData);
            metaDataInThrift.setProperties(props);
            return metaDataInThrift;
        } catch (Exception e) {
            LOGGER.error(
                    "tsfile-file TSFileMetaData: failed to convert file metadata from TSFile to thrift, content is {}",
                    this, e);
            throw e;
        }
    }

    /**
     * receive file metadata in thrift format and convert it to tsfile format
     * @param metadataInThrift - file metadata in thrift format
     */
    @Override
    public void convertToTSF(FileMetaData metadataInThrift) {
        try {
            if (metadataInThrift.getTimeseries_list() == null) {
                timeSeriesList = null;
            } else {
                timeSeriesList = new ArrayList<TimeSeriesMetadata>();

                for (TimeSeries timeSeriesInThrift : metadataInThrift.getTimeseries_list()) {
                    TimeSeriesMetadata timeSeriesInTSFile = new TimeSeriesMetadata();
                    timeSeriesInTSFile.convertToTSF(timeSeriesInThrift);
                    timeSeriesList.add(timeSeriesInTSFile);
                }
            }

            List<cn.edu.thu.tsfile.format.RowGroupMetaData> rowGroupMetaDataListInThrift =
                    metadataInThrift.getRow_groups();
            if (rowGroupMetaDataListInThrift == null) {
                rowGroupMetadataList = null;
            } else {
                rowGroupMetadataList = new ArrayList<RowGroupMetaData>();
                for (cn.edu.thu.tsfile.format.RowGroupMetaData rowGroupMetaDataInThrift : rowGroupMetaDataListInThrift) {
                    RowGroupMetaData rowGroupMetaDataInTSFile = new RowGroupMetaData();
                    rowGroupMetaDataInTSFile.convertToTSF(rowGroupMetaDataInThrift);
                    rowGroupMetadataList.add(rowGroupMetaDataInTSFile);
                }
            }
            currentVersion = metadataInThrift.getVersion();
            createdBy = metadataInThrift.getCreated_by();
            jsonMetaData = metadataInThrift.getJson_metadata();
            props = metadataInThrift.getProperties();
        } catch (Exception e) {
            LOGGER.error(
                    "tsfile-file TSFileMetaData: failed to convert file metadata from thrift to TSFile, content is {}",
                    metadataInThrift, e);
            throw e;
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
}
