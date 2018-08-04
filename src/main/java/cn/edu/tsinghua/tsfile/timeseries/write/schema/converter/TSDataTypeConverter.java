package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.metadata.MetadataArgsErrorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.utils.TSFileEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Each subclass of TSDataTypeConverter responds a enumerate value in
 * {@linkplain TSDataType TSDataType}, which stores several
 * configuration related to responding encoding type.<br>
 * Each TSDataType has a responding TSDataTypeConverter. The design referring to visit pattern
 * provides same outer interface for different TSDataTypes and gets rid of the duplicate switch-case
 * code.
 *
 * @author kangrong
 */
public abstract class TSDataTypeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(TSDataTypeConverter.class);

    /**
     * for ENUMS, JSON is a method of the initialization. Each ENUMS in json-format schema should
     * have data value parameters. initFromProps gets values from JSON object which would be
     * used latter. If this type has extra parameter to construct, override it.
     *
     * @param props - properties which contains information DataTypeConverter needs
     */
    public void initFromProps(Map<String, String> props) {
    }



    public static class ENUMS extends TSDataTypeConverter {
        private TSFileEnum tsfileEnum = null;

        /**
         * input a enum string value, return it ordinal integer
         *
         * @param v - enum string
         * @return - ordinal integer
         */
        public int parseValue(String v) {
            if (v == null || "".equals(v)) {
                LOG.warn("write enum null, String:{}", v);
                return -1;
            }
            if (tsfileEnum == null) {
                LOG.warn("TSDataTypeConverter is not initialized");
                return -1;
            }
            return tsfileEnum.enumOrdinal(v);
        }

        @Override
        public void initFromProps(Map<String, String> props) {
            if (props == null || !props.containsKey(JsonFormatConstant.ENUM_VALUES)) {
                LOG.warn("ENUMS has no data values.");
                return;
            }
            String valueStr = props.get(JsonFormatConstant.ENUM_VALUES).replaceAll("\"", "");
            valueStr = valueStr.substring(1, valueStr.length() - 1);
            String[] values = valueStr.split(",");
            tsfileEnum = new TSFileEnum();
            for (String value : values) {
                tsfileEnum.addTSFileEnum(value);
            }
        }

        @Override
        public String toString() {
            return tsfileEnum.toString();
        }
    }

}
