package cn.edu.thu.tsfile.timeseries.write.schema.converter;

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
        builder.addSeries("s3", TSDataType.ENUMS, TSEncoding.BITMAP, props);
        props.clear();
        props.put("maxPointNumber", "2");
        builder.addSeries("s4", TSDataType.DOUBLE, "RLE", props);
        props.clear();
        props.put("maxPointNumber", "2");
        builder.addSeries("s5", TSDataType.INT32, TSEncoding.TS_2DIFF, props);
        builder.setProps(props);
        builder.addProp("key", "value");
        FileSchema fileSchema = builder.build();

        assertEquals("value", fileSchema.getProp("key"));
        assertEquals("{maxPointNumber=2, key=value}", fileSchema.getProps().toString());

        Collection<MeasurementDescriptor> measurements = fileSchema.getDescriptor();
        String[] measureDesStrings =
                {
                        "[,s3,ENUMS,BITMAP,,SNAPPY,[MAN, WOMAN],]",
                        "[,s4,DOUBLE,RLE,maxPointNumber:2,UNCOMPRESSED,]",
                        "[,s5,INT32,TS_2DIFF,maxPointNumber:2,UNCOMPRESSED,]"
                };
        int i = 0;
        for (MeasurementDescriptor desc : measurements) {
            assertEquals(measureDesStrings[i++], desc.toString());
        }

        List<TimeSeriesMetadata> tsMetadatas = fileSchema.getTimeSeriesMetadatas();
        String[] tsMetadatasList =
                {
                        "TimeSeriesMetadata: measurementUID s3, type ength 0, DataType ENUMS, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s4, type ength 0, DataType DOUBLE, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s5, type ength 0, DataType INT32, FreqType null,frequencies null",
                };
        for (int j = 0; j < tsMetadatas.size(); j++) {
            assertEquals(tsMetadatasList[j], tsMetadatas.get(j).toString());
        }

    }
}
