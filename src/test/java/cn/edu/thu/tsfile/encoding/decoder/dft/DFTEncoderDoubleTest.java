package cn.edu.thu.tsfile.encoding.decoder.dft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import cn.edu.thu.tsfile.encoding.encoder.dft.DFTDoubleEncoder;
import cn.edu.thu.tsfile.encoding.encoder.dft.DFTEncoder;
import org.jtransforms.utils.IOUtils;
import org.junit.Test;

/**
 * test DFTEncoder for double
 * @author kangrong
 *
 */
public class DFTEncoderDoubleTest {
    private DFTEncoder<Double> writer;
    private DFTDecoder<Double> reader;
    private static final int ROW_NUM = 10000;
    private Random ran = new Random();
    // private final long BASIC_FACTOR = 1;
    ByteArrayOutputStream out;

    // private DeltaBinaryValueWriter writer;

     @Test
    public void testBasic() throws IOException {
        System.out.println("write basic");
        double max = 0 - Double.MAX_VALUE;
        double min = Double.MAX_VALUE;
        double data[] = new double[ROW_NUM];
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
        double data[] = new double[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            // data[i] =
            // (double) (Math.sin(2 * Math.PI * 0.01 * i) + Math.sin(2 * Math.PI * 0.2 * i) + Math
            // .sin(2 * Math.PI * 0.5 * i));
            data[i] = (double) (Math.sin(2 * Math.PI * 0.05 * i));
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
        double data[] = new double[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = (double) (Math.sin(2 * Math.PI * 0.05 * i));
            data[i] *= 100;
        }
        List<float[]> freqs = shouldReadAndWriteFreq(data, ROW_NUM, 0.01, 5);
        assertEquals(freqs.size(), 1);
        assertEquals(freqs.get(0).length, 5);
    }


    @Test
    public void testMultiSin() throws IOException {
        System.out.println("write testMultiSin");
        double data[] = new double[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] =
                    (double) (Math.sin(2 * Math.PI * 0.01 * i) + Math.sin(2 * Math.PI * 0.2 * i) + Math
                            .sin(2 * Math.PI * 0.5 * i));
            data[i] *= 100+100;
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
        double data[] = new double[ROW_NUM];
        double max = 0 - Double.MAX_VALUE;
        double min = Double.MAX_VALUE;
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = ran.nextDouble() * 100;
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
        double max = 0 - Double.MAX_VALUE;
        double min = Double.MAX_VALUE;
        double data[] = new double[ROW_NUM];
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
        assertTrue("expect small than:" + 81 + ",but actual is:" + delta,
                delta < 81d);
    }

    private void writeData(double[] data, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            writer.encode(data[i], out);
        }
        writer.flush(out);
    }

    private ByteArrayInputStream in;

    private double shouldReadAndWrite(double[] data, int length, double rate) throws IOException {
        writer = new DFTDoubleEncoder(length, rate, 0);
        System.out.println("source data size:" + 4 * length + " byte");
        out = new ByteArrayOutputStream();
        writeData(data, length);
        byte[] page = out.toByteArray();
        System.out.println("encoding data size:" + page.length + " byte");
        reader = new DFTDoubleDecoder();
        in = new ByteArrayInputStream(page);
        int i = 0;
        double[] expected = new double[data.length];
        while (reader.hasNext(in)) {
            expected[i++] = reader.readDouble(in);
        }
        double rmse = IOUtils.computeRMSE(data, expected);
        return rmse;
    }
    

    private List<float[]> shouldReadAndWriteFreq(double[] data, int length, double rate,
            int mainFreqNum) throws IOException {
        return shouldReadAndWriteFreq(data, length, rate, mainFreqNum, 0);
    }

    private List<float[]> shouldReadAndWriteFreq(double[] data, int length, double rate,
            int mainFreqNum, float overlapRate) throws IOException {
        writer = new DFTDoubleEncoder(length, rate, overlapRate);
        writer.setIsEncoding(false);
        writer.setIsWriteMainFreq(true);
        writer.setMainFreqNum(mainFreqNum);
        System.out.println("source data size:" + 4 * length + " byte");
        out = new ByteArrayOutputStream();
        writeData(data, length);
        byte[] page = out.toByteArray();
        System.out.println("encoding data size:" + page.length + " byte");
        reader = new DFTDoubleDecoder();
        in = new ByteArrayInputStream(page);
        return reader.getMainFrequency(in);
    }

}
