package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.metadata.MetadataArgsErrorException;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * 
 * @author kangrong
 *
 */
public class TSDataTypeConverterTest {
    private String noExists = "no_exists";
    private String errIntStr1 = "lqwk";
    private String[] enum_values = {"a", "s", "2", "d"};
    private String enum_values_tring = "a" + JsonFormatConstant.ENUM_VALUES_SEPARATOR + "s" + JsonFormatConstant.ENUM_VALUES_SEPARATOR
            + "2" + JsonFormatConstant.ENUM_VALUES_SEPARATOR + "d";
}
