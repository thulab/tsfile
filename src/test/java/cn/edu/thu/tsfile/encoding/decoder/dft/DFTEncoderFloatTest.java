package cn.edu.thu.tsfile.encoding.decoder.dft;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import cn.edu.thu.tsfile.encoding.encoder.dft.DFTEncoder;
import org.junit.Test;

import cn.edu.thu.tsfile.encoding.encoder.dft.DFTFloatEncoder;

/**
 * test DFTEncoder for float
 * 
 * @author kangrong
 *
 */
public class DFTEncoderFloatTest {
    private DFTEncoder<Float> writer;
    private DFTDecoder<Float> reader;
    private static final int ROW_NUM = 10000;
    private Random ran = new Random();
    // private final long BASIC_FACTOR = 1;
    ByteArrayOutputStream out;

    // private DeltaBinaryValueWriter writer;

    @Test
    public void testBasic() throws IOException {
        System.out.println("write basic");
        float max = 0 - Float.MAX_VALUE;
        float min = Float.MAX_VALUE;
        float data[] = new float[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = i * i;
            if (max < data[i])
                max = data[i];
            if (min > data[i])
                min = data[i];
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.4);
        double expectDelta = (max - min) * 0.005;
        System.out.println(delta);
        System.out.println("max:" + max + ",min:" + min + ",delta:" + (max - min));
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);

        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        expectDelta = (max - min) * 0.001;
        System.out.println(delta);
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);
    }

    @Test
    public void testSin() throws IOException {
        System.out.println("write sin");
        float data[] = new float[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            // data[i] =
            // (float) (Math.sin(2 * Math.PI * 0.01 * i) + Math.sin(2 * Math.PI * 0.2 * i) + Math
            // .sin(2 * Math.PI * 0.5 * i));
            data[i] = (float) (Math.sin(2 * Math.PI * 0.05 * i));
            data[i] *= 100;
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.001);
        System.out.println(delta);
        assertTrue(delta < 200. / 100);
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        System.out.println(delta);
        assertTrue(delta < 0.01d);
    }

    @Test
    public void testSinFreq() throws IOException {
        System.out.println("test sin frequency");
        float data[] = new float[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = (float) (Math.sin(2 * Math.PI * 0.05 * i));
            data[i] *= 100;
        }
        List<float[]> freqs = shouldReadAndWriteFreq(data, ROW_NUM, 0.01, 5);
        assertEquals(freqs.size(), 1);
        assertEquals(freqs.get(0).length, 5);
    }

    @Test
    public void testNoEnoughSin() throws IOException {
        System.out.println("write not enough sin");
        float data[] = new float[ROW_NUM / 2];
        for (int i = 0; i < ROW_NUM / 2; i++) {
            data[i] = (float) (Math.sin(2 * Math.PI * 0.05 * i));
            data[i] *= 100;
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.001);
        System.out.println(delta);
        assertTrue(delta < 200. / 100);
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        System.out.println(delta);
        assertTrue(delta < 0.01d);
    }

    @Test
    public void testMultiPackSin() throws IOException {
        System.out.println("write multi pack sin");
        float data[] = new float[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = (float) (Math.sin(2 * Math.PI * 0.05 * i));
            data[i] *= 100;
        }
        double delta = shouldReadAndWrite(data, ROW_NUM / 2, 0.001);
        System.out.println(delta);
        assertTrue(delta < 200. / 100);
        delta = shouldReadAndWrite(data, ROW_NUM / 2, 1);
        System.out.println(delta);
        assertTrue(delta < 0.01d);
    }

    @Test
    public void testMultiSin() throws IOException {
        System.out.println("write testMultiSin");
        float data[] = new float[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] =
                    (float) (Math.sin(2 * Math.PI * 0.01 * i) + Math.sin(2 * Math.PI * 0.2 * i) + Math
                            .sin(2 * Math.PI * 0.5 * i));
            data[i] *= 100 + 100;
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.001);
        System.out.println(delta);
        assertTrue(delta < 200. / 100);
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        System.out.println(delta);
        assertTrue(delta < 0.01d);
    }

    @Test
    public void testRandom() throws IOException {
        System.out.println("write random");
        float data[] = new float[ROW_NUM];
        float max = 0 - Float.MAX_VALUE;
        float min = Float.MAX_VALUE;
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = ran.nextFloat() * 100;
            if (max < data[i])
                max = data[i];
            if (min > data[i])
                min = data[i];
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.4);
        System.out.println(delta);
        System.out.println("max:" + max + ",min:" + min + ",delta:" + (max - min));
        assertTrue(delta < (max - min) * 0.12);
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        System.out.println(delta);
        assertTrue("expect small than:" + (max - min) / 100 + ",but actual is:" + delta,
                delta < 0.01d);
    }

    @Test
    public void testMaxMin() throws IOException {
        System.out.println("write maxmin");
        float max = 0 - Float.MAX_VALUE;
        float min = Float.MAX_VALUE;
        float data[] = new float[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = (i & 1) == 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            if (max < data[i])
                max = data[i];
            if (min > data[i])
                min = data[i];
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.001);
        double expectDelta = (max - min) * 0.001;
        System.out.println(delta);
        System.out.println("max:" + max + ",min:" + min + ",delta:" + (max - min));
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        System.out.println(delta);
        assertTrue("expect small than:" + 81 + ",but actual is:" + delta, delta < 81d);
    }

    private void writeData(float[] data, int length, int offset) throws IOException {
        for (int i = 0; i < length & i + offset < data.length; i++) {
            writer.encode(data[offset + i], out);
        }
        writer.flush(out);
    }

    private void writeData(float[] data, int length) throws IOException {
        writeData(data, length, 0);
        // writeData(data, data.length, 0);
    }

    private ByteArrayInputStream in;

    private double shouldReadAndWrite(float[] data, int length, double rate) throws IOException {
        writer = new DFTFloatEncoder(length, 0.6f, 0.7f);
        System.out.println("source data size:" + 4 * length + " byte");
        out = new ByteArrayOutputStream();
        writeData(data, length);
        byte[] page = out.toByteArray();
        System.out.println("encoding data size:" + page.length + " byte");
        reader = new DFTFloatDecoder();
        in = new ByteArrayInputStream(page);
        int i = 0;
        float[] actual = new float[length];
        while (reader.hasNext(in)) {
            actual[i++] = reader.readFloat(in);
            // System.out.println(reader.readFloat(in));
        }
        double rmse = computeRMSE(data, actual, length, 0);
        return rmse;
    }

    public static double computeRMSE(float[] expected, float[] actual, int length, int offset) {
        if (length < actual.length) {
            throw new IllegalArgumentException("out of index size");
        }
        double rms = 0;
        double tmp;
        for (int i = 0; i < length && i + offset < expected.length; i++) {
            tmp = (expected[i + offset] - actual[i]);
            rms += tmp * tmp;
        }
        return Math.sqrt(rms / expected.length);
    }

    private List<float[]> shouldReadAndWriteFreq(float[] data, int length, double rate,
            int mainFreqNum) throws IOException {
        return shouldReadAndWriteFreq(data, length, rate, mainFreqNum, 0);
    }

    private List<float[]> shouldReadAndWriteFreq(float[] data, int length, double rate,
            int mainFreqNum, float overlapRate) throws IOException {
        writer = new DFTFloatEncoder(length, rate, overlapRate);
        writer.setIsEncoding(false);
        writer.setIsWriteMainFreq(true);
        writer.setMainFreqNum(mainFreqNum);
        System.out.println("source data size:" + 4 * length + " byte");
        out = new ByteArrayOutputStream();
        writeData(data, length);
        byte[] page = out.toByteArray();
        System.out.println("encoding data size:" + page.length + " byte");
        reader = new DFTFloatDecoder();
        in = new ByteArrayInputStream(page);
        return reader.getMainFrequency(in);
    }

    @Test
    public void testOverlap() throws IOException {
        System.out.println("test overlap encoding result");
        int length = 10000;
        float encodingRate = 0.4f;
        float data[] = new float[length];
        for (int i = 0; i < data.length; i++) {
            data[i] = ran.nextFloat() * 100;
        }
        // test if overlap encoding result is correct
        writer = new DFTFloatEncoder(length, encodingRate, 0);
        out = new ByteArrayOutputStream();
        writeData(data, length);
        byte[] page = out.toByteArray();
        reader = new DFTFloatDecoder();
        in = new ByteArrayInputStream(page);
        int i = 0;
        float[] expected = new float[data.length];
        while (reader.hasNext(in)) {
            expected[i++] = reader.readFloat(in);
        }

        float overlapRate = 0.02f;
        writer = new DFTFloatEncoder(length, encodingRate, overlapRate);
        out = new ByteArrayOutputStream();
        writeData(data, length);
        page = out.toByteArray();
        reader = new DFTFloatDecoder();
        in = new ByteArrayInputStream(page);
        i = 0;
        float[] overlapResult = new float[data.length];
        while (reader.hasNext(in)) {
            overlapResult[i++] = reader.readFloat(in);
        }
        assertArrayEquals(expected, overlapResult, 0.001f);
        System.out.println("test overlap encoding result successfully!");

        System.out.println("test whether overlap main freqs is correct");
        // test if overlap main frequency result is correct
        // overlap size is 500 and pack length is 1000
        int packLength = 3000;
        overlapRate = 0.5f;
        int mainFreqNum = 3;
        System.out.println("data length:" + length);
        System.out.println("DFT pack length:" + packLength);
        System.out.println("data overlap rate:" + overlapRate);
        System.out.println("data overlap size:" + overlapRate * packLength);
        System.out.println("mainFreqNum:" + mainFreqNum);
        writer = new DFTFloatEncoder(packLength, encodingRate, overlapRate);
        writer.setIsEncoding(false);
        writer.setIsWriteMainFreq(true);

        writer.setMainFreqNum(mainFreqNum);

        out = new ByteArrayOutputStream();
        writeData(data, length);
        page = out.toByteArray();
        reader = new DFTFloatDecoder();
        in = new ByteArrayInputStream(page);

        int offset = 0;
        while (in.available() > 0) {
            float[] overlapFreqs = reader.getPackMainFrequency(in);
            writer = new DFTFloatEncoder(packLength, encodingRate, 0);
            writer.setIsEncoding(false);
            writer.setIsWriteMainFreq(true);
            writer.setMainFreqNum(mainFreqNum);

            out = new ByteArrayOutputStream();
            writeData(data, packLength, offset);
            page = out.toByteArray();
            DFTDecoder<Float> reader1 = new DFTFloatDecoder();
            ByteArrayInputStream subIn = new ByteArrayInputStream(page);
            float[] subFreqs = reader1.getPackMainFrequency(subIn);
            assertTrue(subIn.available() == 0);
            assertArrayEquals(overlapFreqs, subFreqs, 0.001f);
            System.out.println("====================================================");
            for (int j = 0; j < subFreqs.length; j++) {
                System.out.println(overlapFreqs[j] + "\t" + subFreqs[j]);
            }
            offset += packLength - packLength * overlapRate;
        }
        System.out.println("test overlap main freqs is successfully!");
    }

}
