//package com.corp.delta.tsfile.conf;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.fail;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//
//import com.corp.delta.tsfile.common.conf.TSFileConfig;
//import com.corp.delta.tsfile.common.conf.TSFileDescriptor;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import com.corp.delta.tsfile.constant.TimeseriesTestConstant;
//import com.corp.delta.tsfile.file.metadata.enums.TSEncoding;
//
///**
// *
// * @author kangrong
// *
// */
//public class TSFileDescriptorTest {
//    private String yamlFileName = "src/test/resources/test.yaml";
//    private long preRowGroupSize;
//    private int prePageSize;
//    private TSEncoding timeSeriesEncoder;
//    private boolean dftWriteMain;
//    private float dftOverlapRate;
//    private String deltaDataDir;
//    private double sdtMaxErr;
//    private TSFileConfig conf;
//    @Before
//    public void before() {
//        conf = TSFileDescriptor.getInstance().getConfig();
//        preRowGroupSize = conf.rowGroupSize;
//        prePageSize = conf.pageSize;
//        timeSeriesEncoder = TSEncodingconf.timeSeriesEncoder;
//        dftWriteMain = conf.defaultDFTWriteMain;
//        dftOverlapRate = conf.defaultDFTOverlapRate;
//        deltaDataDir = conf.deltaDataDir;
//        sdtMaxErr = conf.defaultSDTMaxError;
//    }
//
//    @After
//    public void after() {
//        conf.rowGroupSize = preRowGroupSize;
//        conf.pageSize = prePageSize;
//        conf.timeSeriesEncoder = timeSeriesEncoder;
//        conf.defaultDFTWriteMain = dftWriteMain;
//        conf.defaultDFTOverlapRate = dftOverlapRate;
//        conf.deltaDataDir = deltaDataDir;
//        conf.defaultSDTMaxError = sdtMaxErr;
//    }
//
//    @Test
//    public void testLoadYaml() {
//        String yamlPath = yamlFileName;
//        File file = new File(yamlPath);
//        InputStream inputStream;
//        try {
//            inputStream = new FileInputStream(file);
//            if (inputStream.available() > 0) {
//                TSFileDescriptor.loadYaml(inputStream);
//                TSFileConfig conf = conf;
//                assertEquals(conf.rowGroupSize, 321321);
//                assertEquals(conf.pageSize, 123123);
//                assertEquals(conf.timeSeriesEncoder, TSEncoding.PLA);
//                assertEquals(false, conf.defaultDFTWriteMain);
//                assertEquals(1.22f, conf.defaultDFTOverlapRate,
//                        TimeseriesTestConstant.float_min_delta);
//                assertEquals("test", conf.deltaDataDir);
//                assertEquals(100.12334, conf.defaultSDTMaxError,
//                        TimeseriesTestConstant.double_min_delta);
//            } else
//                fail();
//        } catch (IOException e) {
//            fail(e.getMessage());
//        }
//
//    }
//
//}
