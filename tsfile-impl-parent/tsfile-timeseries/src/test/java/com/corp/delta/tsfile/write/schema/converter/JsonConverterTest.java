package com.corp.delta.tsfile.write.schema.converter;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Collection;
import java.util.List;

import com.corp.delta.tsfile.write.exception.WriteProcessException;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import com.corp.delta.tsfile.file.metadata.TimeSeriesMetadata;
import com.corp.delta.tsfile.write.desc.MeasurementDescriptor;
import com.corp.delta.tsfile.write.schema.FileSchema;

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
        String[] measureDescsStrings =
                {
                        "[,s3,ENUMS,BITMAP,,SNAPPY,[MAN, WOMAN],]",
                        "[,s4,DOUBLE,RLE,maxPointNumber:2,UNCOMPRESSED,DFT,DFT_PACK_LENGTH,32,DFT_RATE,0.8,DFT_WRITE_Main_FREQ,true,DFT_WRITE_ENCODING,false,DFT_OVERLAP_RATE,0.02,DFT_MAIN_FREQ_NUM,3,]",
                        "[,s5,INT32,TS_2DIFF,maxPointNumber:2,UNCOMPRESSED,DFT,DFT_PACK_LENGTH,10000,DFT_RATE," +
                                "0.4,DFT_WRITE_Main_FREQ,true,DFT_WRITE_ENCODING,false,DFT_OVERLAP_RATE,0.0," +
                                "DFT_MAIN_FREQ_NUM,2,]",
                        "[,s1,INT32,RLE,maxPointNumber:2,UNCOMPRESSED,]",
                        "[,s2,INT64,TS_2DIFF,maxPointNumber:2,UNCOMPRESSED,]",

                };
        int i = 0;
        for (MeasurementDescriptor desc : measurements) {
//            System.out.println(desc.toString());
            assertEquals(measureDescsStrings[i++], desc.toString());
        }

        List<TimeSeriesMetadata> tsMetadatas = fileSchema.getTimeSeriesMetadatas();
        String[] tsMetadatasList =
                {
                        "TimeSeriesMetadata: measurementUID s1, type ength 0, DataType INT32, FreqType SINGLE_FREQ,frequencies null",
                        "TimeSeriesMetadata: measurementUID s2, type ength 0, DataType INT64, FreqType SINGLE_FREQ,frequencies null",
                        "TimeSeriesMetadata: measurementUID s3, type ength 0, DataType ENUMS, FreqType SINGLE_FREQ,frequencies null",
                        "TimeSeriesMetadata: measurementUID s4, type ength 0, DataType DOUBLE, FreqType SINGLE_FREQ,frequencies null",
                        "TimeSeriesMetadata: measurementUID s5, type ength 0, DataType INT32, FreqType SINGLE_FREQ,frequencies null",
                };
        for (int j = 0; j < tsMetadatas.size(); j++) {
//            System.out.println(tsMetadatas.get(j).toString());
            assertEquals(tsMetadatasList[j], tsMetadatas.get(j).toString());
        }

    }

}
