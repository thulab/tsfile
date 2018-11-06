package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.converter.IConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.FreqType;
import cn.edu.tsinghua.tsfile.format.TimeInTimeSeriesChunkMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * For more information, see TimeInTimeSeriesChunkMetaData
 * in cn.edu.thu.tsfile.format package
 */
public class TInTimeSeriesChunkMetaData implements IConverter<TimeInTimeSeriesChunkMetaData> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TInTimeSeriesChunkMetaData.class);

    /** type of data **/
    private TSDataType dataType;
    /** start time of time series chunk **/
    private long startTime;
    /** end time of time series chunk */
    private long endTime;

    @Deprecated
    private TSFreqType freqType;
    @Deprecated
    private List<Integer> frequencies;

    /**
     * If values for data consist of enum values, metadata will store all possible values in time
     * series
     */
    private List<String> enumValues;

    /**
     * empty constructor
     */
    public TInTimeSeriesChunkMetaData() {
    }

    /**
     * init this TInTimeSeriesChunkMetaData
     * @param dataType the data type of this timeseries chunk
     * @param startTime the start time of this timeseries chunk
     * @param endTime the end time of this timseries chunk
     */
    public TInTimeSeriesChunkMetaData(TSDataType dataType, long startTime, long endTime) {
        this.dataType = dataType;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * Serialize class TInTimeSeriesChunkMetaData to thrift format for persisting data to disk.
     * @return class of thrift format
     */
    @Override
    public TimeInTimeSeriesChunkMetaData convertToThrift() {
        try {
            /** construct a new TimeInTimeSeriesChunkMetaData **/
            TimeInTimeSeriesChunkMetaData tTimeSeriesChunkMetaDataInThrift =
                    new TimeInTimeSeriesChunkMetaData(
                            dataType == null ? null : DataType.valueOf(dataType.toString()), startTime, endTime);
            /** set value of property Freq_type **/
            tTimeSeriesChunkMetaDataInThrift.setFreq_type(freqType == null ? null : FreqType.valueOf(freqType.toString()));
            /** set value of property Frequencies **/
            tTimeSeriesChunkMetaDataInThrift.setFrequencies(frequencies);
            /** set value of property Enum_values **/
            tTimeSeriesChunkMetaDataInThrift.setEnum_values(enumValues);
            return tTimeSeriesChunkMetaDataInThrift;
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TInTimeSeriesChunkMetaData: failed to convert TimeInTimeSeriesChunkMetaData from TSFile to thrift, content is {}",
                        this, e);
            throw e;
        }
    }

    /**
     * Deserialize class TimeInTimeSeriesChunkMetaData from thrift format to normal format.
     * @param tTimeSeriesChunkMetaDataInThrift thrift format of class TimeInTimeSeriesChunkMetaData
     */
    @Override
    public void convertToTSF(TimeInTimeSeriesChunkMetaData tTimeSeriesChunkMetaDataInThrift) {
        try {
            /** get the value of property Data_type **/
            dataType = tTimeSeriesChunkMetaDataInThrift.getData_type() == null ? null : TSDataType.valueOf(tTimeSeriesChunkMetaDataInThrift.getData_type().toString());
            /** get the value of property Freq_type **/
            freqType = tTimeSeriesChunkMetaDataInThrift.getFreq_type() == null ? null : TSFreqType.valueOf(tTimeSeriesChunkMetaDataInThrift.getFreq_type().toString());
            /** get the value of property Frequencies **/
            frequencies = tTimeSeriesChunkMetaDataInThrift.getFrequencies();
            /** get the value of property Startime **/
            startTime = tTimeSeriesChunkMetaDataInThrift.getStartime();
            /** get the value of property Endtime **/
            endTime = tTimeSeriesChunkMetaDataInThrift.getEndtime();
            /** get the value of property Enum_values **/
            enumValues = tTimeSeriesChunkMetaDataInThrift.getEnum_values();
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled())
                LOGGER.error(
                        "tsfile-file TInTimeSeriesChunkMetaData: failed to convert TimeInTimeSeriesChunkMetaData from thrift to TSFile, content is {}",
                        tTimeSeriesChunkMetaDataInThrift, e);
            throw e;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "TInTimeSeriesChunkMetaData{ TSDataType %s, TSFreqType %s, frequencies %s, starttime %d, endtime %d, enumValues %s }",
                dataType, freqType, frequencies, startTime, endTime, enumValues);
    }

    public TSDataType getDataType() {
        return dataType;
    }

    public void setDataType(TSDataType dataType) {
        this.dataType = dataType;
    }

    public TSFreqType getFreqType() {
        return freqType;
    }

    public void setFreqType(TSFreqType freqType) {
        this.freqType = freqType;
    }

    public List<Integer> getFrequencies() {
        return frequencies;
    }

    public void setFrequencies(List<Integer> frequencies) {
        this.frequencies = frequencies;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public List<String> getEnumValues() {
        return enumValues;
    }

    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }
}
