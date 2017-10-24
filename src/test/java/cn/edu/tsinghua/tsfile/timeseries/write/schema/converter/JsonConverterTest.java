package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

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
        Collection<MeasurementDescriptor> measurements = fileSchema.getDescriptor().values();
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

        List<TimeSeriesMetadata> tsMetadataList = fileSchema.getTimeSeriesMetadatas();
        String[] tsMetadatas =
                {
                        "TimeSeriesMetadata: measurementUID s1, type length 0, DataType INT32, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s2, type length 0, DataType INT64, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s3, type length 0, DataType ENUMS, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s4, type length 0, DataType DOUBLE, FreqType null,frequencies null",
                        "TimeSeriesMetadata: measurementUID s5, type length 0, DataType INT32, FreqType null,frequencies null",
                };
        Collections.sort(tsMetadataList, (x,y)->x.getMeasurementUID().compareTo(y.getMeasurementUID()));
        Arrays.sort(tsMetadatas, (x,y)->x.compareTo(y));
        for (int j = 0; j < tsMetadataList.size(); j++) {
            assertEquals(tsMetadatas[j], tsMetadataList.get(j).toString());
        }

    }

}
