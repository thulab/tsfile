package cn.edu.thu.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import cn.edu.thu.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author kangrong
 */
public class JsonConverterTest {

    @Test
    public void testJsonConverter() throws WriteProcessException {
        String path = "src/test/resources/test_schema.json";
        JSONObject obj = null;
        try {
            obj = new JSONObject(new JSONTokener(new FileReader(new File(path))));
        } catch (JSONException | FileNotFoundException e) {
            e.printStackTrace();
            fail();
        }

        FileSchema fileSchema = new FileSchema(obj);
        assertEquals("test_type", fileSchema.getDeltaType());
        Collection<MeasurementDescriptor> measurements = fileSchema.getDescriptor();
        String[] measureDesStrings =
                {
                        "[,s3,ENUMS,BITMAP,,SNAPPY,[MAN, WOMAN],]",
                        "[,s4,DOUBLE,RLE,max_point_number:2,UNCOMPRESSED,]",
                        "[,s5,INT32,TS_2DIFF,max_point_number:2,UNCOMPRESSED,]",
                        "[,s1,INT32,RLE,max_point_number:2,UNCOMPRESSED,]",
                        "[,s2,INT64,TS_2DIFF,max_point_number:2,UNCOMPRESSED,]",

                };
        int i = 0;
        for (MeasurementDescriptor desc : measurements) {
            assertEquals(measureDesStrings[i++], desc.toString());
        }

        List<TimeSeriesMetadata> tsMetadatas = fileSchema.getTimeSeriesMetadatas();
        String[] tsMetadataList =
                {
                        "TimeSeriesMetadata: measurementUID s1, type length 0, DataType INT32, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s2, type length 0, DataType INT64, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s3, type length 0, DataType ENUMS, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s4, type length 0, DataType DOUBLE, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s5, type length 0, DataType INT32, FreqType null,frequencies null",
                };
        for (int j = 0; j < tsMetadatas.size(); j++) {
            assertEquals(tsMetadataList[j], tsMetadatas.get(j).toString());
        }

    }

}
