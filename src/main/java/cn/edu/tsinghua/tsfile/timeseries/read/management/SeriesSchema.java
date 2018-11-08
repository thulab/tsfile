package cn.edu.tsinghua.tsfile.timeseries.read.management;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.util.HashMap;

/**
 * This class define a schema for one time series.
 * This schema includes three main parameters which represent the {@code name},the {@code dataType} and
 * the {@code encoding} type for this time series. Some other arguments are put in {@code args}
 *
 * @author Jinrui Zhang
 */
public class SeriesSchema {
    /** measurementId **/
    public String name;
    /** data type **/
    public TSDataType dataType;
    /** encoding type **/
    public TSEncoding encoding;
    /** additional args **/
    private HashMap<String, String> args;

    /**
     * init this SeriesSchema
     * @param name
     * @param dataType
     * @param encoding
     */
    public SeriesSchema(String name, TSDataType dataType, TSEncoding encoding) {
        this.name = name;
        this.dataType = dataType;
        this.encoding = encoding;
        this.args = new HashMap<>();
    }

    /**
     * add one kv argument
     * @param key
     * @param value
     */
    public void putKeyValueToArgs(String key, String value) {
        this.args.put(key, value);
    }

    /**
     * get one specific argument
     * @param key
     * @return
     */
    public Object getValueFromArgs(String key) {
        return args.get(key);
    }

    /**
     * get all arguments
     * @return
     */
    public HashMap<String, String> getArgsMap() {
        return args;
    }

    /**
     * set all arguments
     * @param argsMap
     */
    public void setArgsMap(HashMap<String, String> argsMap) {
        this.args = argsMap;
    }
}
