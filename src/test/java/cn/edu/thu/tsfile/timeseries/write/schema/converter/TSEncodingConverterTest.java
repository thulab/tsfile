package cn.edu.thu.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.exception.metadata.MetadataArgsErrorException;
import cn.edu.thu.tsfile.file.metadata.enums.TSEncoding;
import org.json.JSONObject;
import org.junit.Test;

/**
 * 
 * @author kangrong
 *
 */
public class TSEncodingConverterTest {
    private String noExists = "no_exists";
    private String intStr1 = "1";
    private Integer int1 = 1;
    private String doubleStr1 = "1.2d";
    private Double double1 = 1.2d;
    private String floatStr1 = "1.2f";
    private Float float1 = 1.2f;
    private String boolStr1 = "true";
    private Boolean bool1 = true;
    private String errFloatStr1 = "lqwk";
    private String errDoubleStr1 = "lqwk";
    private String errIntStr1 = "lqwk";
    private String errBoolStr1 = "lqwk";

    @Test
    public void testCheckParameterNoParameter() {
        TSEncoding encode = TSEncoding.PLAIN;
        try {
            assertEquals(null, TSEncodingConverter.checkParameter(encode, noExists, noExists));
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
        }
    }

    @Test
    public void testCheckParameterRLE() {
        TSEncoding encode = TSEncoding.RLE;
        try {
            assertEquals(int1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, intStr1));
        } catch (MetadataArgsErrorException e1) {
            assertTrue(false);
        }
        try {
            TSEncodingConverter.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, errIntStr1);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
            assertEquals("paramter max_point_number meets error integer format :lqwk", e.getMessage());
        }
    }

    @Test
    public void testCheckParameterTS_2DIFF() {
        TSEncoding encode = TSEncoding.TS_2DIFF;
        try {
            assertEquals(int1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, intStr1));
        } catch (MetadataArgsErrorException e1) {
            assertTrue(false);
        }
        try {
            TSEncodingConverter.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, errIntStr1);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
            assertEquals("paramter max_point_number meets error integer format :lqwk", e.getMessage());
        }
    }

//     @Test
    public void testCheckParameterPLA() {
        TSEncoding encode = TSEncoding.PLA;
        try {
            assertEquals(double1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.PLA_MAX_ERROR, doubleStr1));
        } catch (MetadataArgsErrorException e1) {
            assertTrue(false);
        }
        try {
            TSEncodingConverter.checkParameter(encode, JsonFormatConstant.PLA_MAX_ERROR, errDoubleStr1);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
        }
    }

    // @Test
    public void testCheckParameterSDT() {
        TSEncoding encode = TSEncoding.SDT;
        try {
            assertEquals(double1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.PLA_MAX_ERROR, doubleStr1));
        } catch (MetadataArgsErrorException e1) {
            assertTrue(false);
        }
        try {
            TSEncodingConverter.checkParameter(encode, JsonFormatConstant.SDT_MAX_ERROR, errFloatStr1);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException); 
        }
    }

     @Test
    public void testCheckParameterDFT() {
        TSEncoding encode = TSEncoding.DFT;
        try {
            assertEquals(int1, TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_PACK_LENGTH, intStr1));

            assertEquals(double1, TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_RATE, doubleStr1));
            assertEquals(bool1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_WRITE_Main_FREQ, boolStr1));
            assertEquals(bool1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_WRITE_ENCODING, boolStr1));
            assertEquals(float1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_OVERLAP_RATE, floatStr1));
            assertEquals(int1,
                    TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_MAIN_FREQ_NUM, intStr1));
        } catch (MetadataArgsErrorException e1) {
            assertTrue(false);
        }
        try {
            TSEncodingConverter.checkParameter(encode, JsonFormatConstant.DFT_MAIN_FREQ_NUM, errBoolStr1);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof MetadataArgsErrorException);
        }
        
        //check initFromObject
        TSEncodingConverter dft = TSEncodingConverter.getConverter(TSEncoding.DFT);
        JSONObject allConf = new JSONObject();
        dft.initFromJsonObject("s1", allConf);
    }
}
