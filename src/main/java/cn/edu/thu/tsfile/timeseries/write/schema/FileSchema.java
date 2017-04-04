package cn.edu.thu.tsfile.timeseries.write.schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.thu.tsfile.timeseries.write.InternalRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.schema.converter.JsonConverter;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 
 * FileSchema stores the schema of registered measurements and delta objects that appeared in this
 * stage. All delta objects written to the same TSFile have the same schema. FileSchema takes the
 * JSON schema file as a parameter and registers measurement informations. FileSchema also records
 * all appeared delta object IDs in this stage.
 * 
 * @author kangrong
 *
 */
public class FileSchema {
    static private final Logger LOG = LoggerFactory.getLogger(FileSchema.class);
    /**
     * {@code appearDeltaObjectIdSet} responds to delta object that appeared in this stage. Stage
     * means the time period after the last <b>flushing to file</b> up to now.
     */
    private Set<String> appearDeltaObjectIdSet = new HashSet<String>();
    /**
     * {@code Map<measurementId, TSDataType>}
     */
    private Map<String, TSDataType> dataTypeMap = new HashMap<String, TSDataType>();
    /**
     * {@code Map<measurementId, MeasurementDescriptor>}
     */
    private Map<String, MeasurementDescriptor> descriptorMap =
            new HashMap<String, MeasurementDescriptor>();
    private String[] tempKeyArray = new String[10];
    /**
     * deltaType of this TSFile
     */
    private String deltaType;
    private List<TimeSeriesMetadata> tsMetadata = new ArrayList<TimeSeriesMetadata>();
    private int currentRowMaxSize;

    public int getCurrentRowMaxSize() {
        return currentRowMaxSize;
    }

    public void setCurrentRowMaxSize(int currentRowMaxSize) {
        this.currentRowMaxSize = currentRowMaxSize;
    }

    public FileSchema(JSONObject jsonSchema) throws WriteProcessException {
        JsonConverter.converterJsonToSchema(jsonSchema, this);
    }

    /**
     * judge whether given delta object id exists in this stage.
     * 
     * @param deltaObjectId - delta object id
     * @return - if this id appeared in this stage, return true, otherwise return false
     */
    public boolean hasDeltaObject(String deltaObjectId) {
        return appearDeltaObjectIdSet.contains(deltaObjectId);
    }

    /**
     * add a delta object id to this schema
     * 
     * @param deltaObjectId - delta object id to be added
     */
    public void addDeltaObject(String deltaObjectId) {
        appearDeltaObjectIdSet.add(deltaObjectId);
    }

    public Set<String> getDeltaObjectAppearedSet() {
        return appearDeltaObjectIdSet;
    }

    public void setDeltaType(String deltaType) {
        this.deltaType = deltaType;
    }

    public String getDeltaType() {
        return deltaType;
    }

    public void setSeriesType(String measurementUID, TSDataType type) {
        dataTypeMap.put(measurementUID, type);
    }

    public TSDataType getSeriesType(String measurementUID) {
        return dataTypeMap.get(measurementUID);
    }

    public void setDescriptor(String measurementUID, MeasurementDescriptor convertor) {
        descriptorMap.put(measurementUID, convertor);
    }

    public MeasurementDescriptor getDescriptor(String measurementUID) {
        return descriptorMap.get(measurementUID);
    }

    public Collection<MeasurementDescriptor> getDescriptor() {
        return descriptorMap.values();
    }

    /**
     * add a TimeSeriesMetadata into this fileSchema
     * 
     * @param measurementId - the measurement id of this TimeSeriesMetadata
     * @param type - the data type of this TimeSeriesMetadata
     * @param measurementObj - the json object of this measurement
     */
    public void addTimeSeriesMetadata(String measurementId, TSDataType type,
            JSONObject measurementObj) {
        TimeSeriesMetadata ts = new TimeSeriesMetadata(measurementId, type, deltaType);
        if (measurementObj.has(JsonFormatConstant.FreqType))
            ts.setFreqType(TSFreqType.valueOf(measurementObj.getString(JsonFormatConstant.FreqType)));
        else
            ts.setFreqType(TSFreqType.valueOf(TSFileDescriptor.getInstance().getConfig().defaultFreqType));
        LOG.debug("add Time Series:{}", ts);
        this.tsMetadata.add(ts);
    }

    public List<TimeSeriesMetadata> getTimeSeriesMetadatas() {
        return tsMetadata;
    }

    /**
     * This method is called in {@linkplain InternalRecordWriter
     * InternalRecordWriter} after flushing row group to file. The delta object id used in last
     * stage remains in this stage. The delta object id which not be used in last stage will be
     * removed
     * 
     * @param groupWriters - {@code Map<deltaObjectId, RowGroupWriter>}, a map remaining all
     *        {@linkplain IRowGroupWriter IRowGroupWriter}
     */
    public void resetUnusedDeltaObjectId(Map<String, IRowGroupWriter> groupWriters) {
        int size = groupWriters.size();
        if (size < tempKeyArray.length)
            tempKeyArray = new String[size];
        int i = 0;
        for (String id : groupWriters.keySet()) {
            tempKeyArray[i++] = id;
        }
        for (String existDeltaObjectId : tempKeyArray) {
            if (!appearDeltaObjectIdSet.contains(existDeltaObjectId)) {
                groupWriters.remove(existDeltaObjectId);
            }
        }
        appearDeltaObjectIdSet.clear();
    }
}
