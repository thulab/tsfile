package cn.edu.tsinghua.tsfile.timeseries.readV1;

import cn.edu.tsinghua.tsfile.common.utils.Binary;

import cn.edu.tsinghua.tsfile.encoding.common.EndianType;
import cn.edu.tsinghua.tsfile.encoding.decoder.*;
import cn.edu.tsinghua.tsfile.encoding.encoder.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.PageDataReader;
import cn.edu.tsinghua.tsfile.timeseries.write.series.PageWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zhangjinrui on 2017/12/21.
 */
public class PageDataReaderTest {

    private static final int POINTS_COUNT_IN_ONE_PAGE = 1000000;

    @Test
    public void testLong() {

        LoopWriteReadTest test = new LoopWriteReadTest("Test INT64", new LongRleEncoder(EndianType.BIG_ENDIAN),
                new LongRleDecoder(EndianType.BIG_ENDIAN), TSDataType.INT64, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return Long.valueOf(Long.MAX_VALUE - i);
            }
        };
        test.test();
    }

    @Test
    public void testBoolean() {
        LoopWriteReadTest test = new LoopWriteReadTest("Test Boolean", new IntRleEncoder(EndianType.BIG_ENDIAN),
                new IntRleDecoder(EndianType.BIG_ENDIAN), TSDataType.BOOLEAN, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return i % 3 == 0 ? true : false;
            }
        };
        test.test();
    }

    @Test
    public void testInt() {
        LoopWriteReadTest test = new LoopWriteReadTest("Test INT32", new IntRleEncoder(EndianType.BIG_ENDIAN),
                new IntRleDecoder(EndianType.BIG_ENDIAN), TSDataType.INT32, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return Integer.valueOf(i);
            }
        };
        test.test();
    }

    @Test
    public void testFloat() {
        LoopWriteReadTest test = new LoopWriteReadTest("Test FLOAT", new SinglePrecisionEncoder(),
                new SinglePrecisionDecoder(), TSDataType.FLOAT, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return Float.valueOf(i) / 10 - Float.valueOf(i) / 100;
            }
        };
        test.test();

        LoopWriteReadTest test2 = new LoopWriteReadTest("Test FLOAT", new SinglePrecisionEncoder(),
                new SinglePrecisionDecoder(), TSDataType.FLOAT, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return Float.valueOf(i) / 100 - Float.valueOf(i) / 10;
            }
        };
        test2.test();
    }

    @Test
    public void testDouble() {
        LoopWriteReadTest test = new LoopWriteReadTest("Test Double", new DoublePrecisionEncoder(),
                new DoublePrecisionDecoder(), TSDataType.DOUBLE, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return Double.valueOf(i) / 10 - Double.valueOf(i) / 100;
            }
        };
        test.test();

        LoopWriteReadTest test2 = new LoopWriteReadTest("Test Double", new DoublePrecisionEncoder(),
                new DoublePrecisionDecoder(), TSDataType.DOUBLE, POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return Double.valueOf(i) / 1000 - Double.valueOf(i) / 100;
            }
        };
        test2.test();
    }

    @Test
    public void testBinary() {
        LoopWriteReadTest test = new LoopWriteReadTest("Test Double",
                new PlainEncoder(EndianType.LITTLE_ENDIAN, TSDataType.TEXT, 1000),
                new PlainDecoder(EndianType.LITTLE_ENDIAN),
                TSDataType.TEXT,
                POINTS_COUNT_IN_ONE_PAGE) {
            @Override
            public Object generateValueByIndex(int i) {
                return new Binary(new StringBuilder("TEST TEXT").append(i).toString());
            }
        };
        test.test();
    }

    private abstract static class LoopWriteReadTest {
        private Encoder encoder;
        private Decoder decoder;
        private TSDataType dataType;
        private PageWriter pageWriter;
        private String name;
        private int count;

        public LoopWriteReadTest(String name, Encoder encoder, Decoder decoder, TSDataType dataType, int count) {
            this.name = name;
            this.encoder = encoder;
            this.decoder = decoder;
            this.dataType = dataType;
            this.count = count;
        }

        public void test() {
            try {
                pageWriter = new PageWriter();
                pageWriter.setTimeEncoder(new DeltaBinaryEncoder.LongDeltaEncoder());
                pageWriter.setValueEncoder(this.encoder);
                writeData();

                InputStream page = new ByteArrayInputStream(pageWriter.getUncompressedBytes().array());

                PageDataReader pageReader = new PageDataReader(page, dataType, decoder, new DeltaBinaryDecoder.LongDeltaDecoder());

                int index = 0;
                long startTimestamp = System.currentTimeMillis();
                while (pageReader.hasNext()) {
                    TimeValuePair timeValuePair = pageReader.next();
                    Assert.assertEquals(Long.valueOf(index), (Long) timeValuePair.getTimestamp());
                    Assert.assertEquals(generateValueByIndex(index), timeValuePair.getValue().getValue());
                    index++;
                }
                long endTimestamp = System.currentTimeMillis();
                System.out.println("TestName: [" + name + "]\n\tTSDataType: " + dataType +
                        "\tRead-Count:" + count + "\tTime-used:" + (endTimestamp - startTimestamp) + "ms");
                Assert.assertEquals(count, index);
            } catch (IOException e) {
                e.printStackTrace();
                Assert.fail("Fail when executing test: [" + name + "]");
            }
        }

        private void writeData() throws IOException {
            for (int i = 0; i < count; i++) {
                switch (dataType) {
                    case BOOLEAN:
                        pageWriter.write(Long.valueOf(i), (Boolean) generateValueByIndex(i));
                        break;
                    case INT32:
                        pageWriter.write(Long.valueOf(i), (Integer) generateValueByIndex(i));
                        break;
                    case INT64:
                        pageWriter.write(Long.valueOf(i), (Long) generateValueByIndex(i));
                        break;
                    case FLOAT:
                        pageWriter.write(Long.valueOf(i), (Float) generateValueByIndex(i));
                        break;
                    case DOUBLE:
                        pageWriter.write(Long.valueOf(i), (Double) generateValueByIndex(i));
                        break;
                    case TEXT:
                        pageWriter.write(Long.valueOf(i), (Binary) generateValueByIndex(i));
                        break;

                }
            }
        }

        public abstract Object generateValueByIndex(int i);
    }

}
