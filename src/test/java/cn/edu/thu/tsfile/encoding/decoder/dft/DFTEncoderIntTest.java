package cn.edu.thu.tsfile.encoding.decoder.dft;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import cn.edu.thu.tsfile.encoding.encoder.dft.DFTIntEncoder;
import org.junit.Test;

/**
 * test DFTEncoder for integer
 * @author kangrong
 *
 */
public class DFTEncoderIntTest {
    private DFTIntEncoder writer;
    private DFTIntDecoder reader;
    private static final int ROW_NUM = 10000;
    private Random ran = new Random();
    // private final long BASIC_FACTOR = 1;
    ByteArrayOutputStream out;

    // private DeltaBinaryValueWriter writer;

     @Test
    public void testBasic() throws IOException {
        System.out.println("write basic");
        int max = 0 - Integer.MAX_VALUE;
        int min = Integer.MAX_VALUE;
        int data[] = new int[ROW_NUM];
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
        int data[] = new int[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            // data[i] =
            // (int) (Math.sin(2 * Math.PI * 0.01 * i) + Math.sin(2 * Math.PI * 0.2 * i) + Math
            // .sin(2 * Math.PI * 0.5 * i));
            data[i] = (int) (Math.sin(2 * Math.PI * 0.05 * i));
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
    public void testMultiSin() throws IOException {
        System.out.println("write testMultiSin");
        int data[] = new int[ROW_NUM];
        int max = 0 - Integer.MAX_VALUE;
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < ROW_NUM; i++) {
            float temp =
                    (float) (Math.sin(2 * Math.PI * 0.01 * i) + Math.sin(2 * Math.PI * 0.2 * i) + Math
                            .sin(2 * Math.PI * 0.5 * i));
            data[i] = (int)(temp *200+100);
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

    @Test
    public void testRandom() throws IOException {
        System.out.println("write random");
        int data[] = new int[ROW_NUM];
        int max = 0 - Integer.MAX_VALUE;
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = ran.nextInt() * 100;
            if (max < data[i])
                max = data[i];
            if (min > data[i])
                min = data[i];
        }
        
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.4);
        double expectDelta = Integer.MAX_VALUE * 0.22;
        System.out.println(delta);
        System.out.println("max:" + max + ",min:" + min + ",delta:" + (max - min));
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        expectDelta = Integer.MAX_VALUE * 0.22;
        System.out.println(delta);
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);
    }

     @Test
    public void testMaxMin() throws IOException {
        System.out.println("write maxmin");
        int max = 0 - Integer.MAX_VALUE;
        int min = Integer.MAX_VALUE;
        int data[] = new int[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = (i & 1) == 0 ? Integer.MAX_VALUE : Integer.MIN_VALUE;
            if (max < data[i])
                max = data[i];
            if (min > data[i])
                min = data[i];
        }
        double delta = shouldReadAndWrite(data, ROW_NUM, 0.001);
        double expectDelta = Integer.MAX_VALUE * 0.001;
        System.out.println(delta);
        System.out.println("max:" + max + ",min:" + min + ",delta:" + (max - min));
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);
        expectDelta = Integer.MAX_VALUE * 0.001;
        delta = shouldReadAndWrite(data, ROW_NUM, 1);
        System.out.println(delta);
        assertTrue("expect small than:" + expectDelta + ",but actual is:" + delta,
                delta < expectDelta);
    }

    private void writeData(int[] data, int length) {
        for (int i = 0; i < length; i++) {
            writer.encode(data[i], out);
        }
        writer.flush(out);
    }

    private ByteArrayInputStream in;

    private double shouldReadAndWrite(int[] data, int length, double rate) throws IOException {
        writer = new DFTIntEncoder(length, rate, 0);
        System.out.println("source data size:" + 4 * length + " byte");
        out = new ByteArrayOutputStream();
        writeData(data, length);
        byte[] page = out.toByteArray();
        System.out.println("encoding data size:" + page.length + " byte");
        reader = new DFTIntDecoder();
        in = new ByteArrayInputStream(page);
        int i = 0;
        int[] expected = new int[data.length];
        while (reader.hasNext(in)) {
            expected[i++] = reader.readInt(in);
        }
        double rmse = computeRMSE(data, expected);
        return rmse;
    }
    /**
     * it's directly rewrite from org.jtransforms.utils.computeRMSE(float[] a, float[] b)
     * @param a
     * @param b
     * @return
     */
    public static double computeRMSE(int[] a, int[] b)
    {
        if (a.length != b.length) {
            throw new IllegalArgumentException("Arrays are not the same size");
        }
        double rms = 0;
        double tmp;
        for (int i = 0; i < a.length; i++) {
            tmp = (a[i] - b[i]);
            rms += tmp * tmp;
        }
        return Math.sqrt(rms / a.length);
    }
}
