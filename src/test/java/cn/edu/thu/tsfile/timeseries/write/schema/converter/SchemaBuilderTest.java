package cn.edu.thu.tsfile.timeseries.write.schema.converter;

import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.write.schema.SchemaBuilder;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author qiaojialin
 */
public class SchemaBuilderTest {
    @Test
    public void testJsonConverter() throws WriteProcessException {

        SchemaBuilder builder = new SchemaBuilder();
        Map<String, String> props = new HashMap<>();
        props.put("enum_values", "[\"MAN\",\"WOMAN\"]");
        props.put("compressor", "SNAPPY");
        MeasurementDescriptor descriptor = new MeasurementDescriptor("s3", TSDataType.ENUMS, TSEncoding.BITMAP, props);
        builder.addSeries(descriptor);
        props.clear();
        props.put(JsonFormatConstant.MAX_POINT_NUMBER, "3");
        builder.addSeries("s4", TSDataType.DOUBLE, "RLE", props);
        builder.addSeries("s5", TSDataType.INT32, TSEncoding.TS_2DIFF, null);
        props.clear();
        props.put(JsonFormatConstant.MAX_POINT_NUMBER, "2");
        builder.setProps(props);
        builder.addProp("key", "value");
        FileSchema fileSchema = builder.build();

        assertEquals("value", fileSchema.getProp("key"));
        assertEquals("{max_point_number=2, key=value}", fileSchema.getProps().toString());

        Collection<MeasurementDescriptor> measurements = fileSchema.getDescriptor();
        String[] measureDesStrings =
                {
                        "[,s3,ENUMS,BITMAP,,SNAPPY,[MAN, WOMAN],]",
                        "[,s4,DOUBLE,RLE,max_point_number:3,UNCOMPRESSED,]",
                        "[,s5,INT32,TS_2DIFF,max_point_number:2,UNCOMPRESSED,]"
                };
        int i = 0;
        for (MeasurementDescriptor desc : measurements) {
            assertEquals(measureDesStrings[i++], desc.toString());
        }

        List<TimeSeriesMetadata> tsMetadatas = fileSchema.getTimeSeriesMetadatas();
        String[] tsMetadataList =
                {
                        "TimeSeriesMetadata: measurementUID s3, type length 0, DataType ENUMS, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s4, type length 0, DataType DOUBLE, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s5, type length 0, DataType INT32, FreqType null,frequencies null",
                };
        for (int j = 0; j < tsMetadatas.size(); j++) {
            assertEquals(tsMetadataList[j], tsMetadatas.get(j).toString());
        }

    }
}
