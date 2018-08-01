package cn.edu.tsinghua.tsfile.compress;

import cn.edu.tsinghua.tsfile.common.utils.ByteBufferUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 
 * @author kangrong
 *
 */
public class SnappyTest {
    private String randomString(int length){
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char) (ThreadLocalRandom.current().nextInt(33, 128)));
        }
        return builder.toString();
    }

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testBytes() throws UnsupportedEncodingException, IOException {
//        String input =
//                "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
//                        + "Snappy, a fast compresser/decompresser.";
//        byte[] uncom= input.getBytes("UTF-8");
//        File file=new File("/Users/hxd/Desktop/Fu - 2011 - A review on time series data mining.pdf");
//        byte[] uncom = new byte[(int)file.length()];
//        IOUtils.read(new FileInputStream(file), uncom);
        String input= randomString(50000);
        byte[] uncom= input.getBytes("UTF-8");
        long time=System.currentTimeMillis();
        byte[] compressed = Snappy.compress(uncom);
        System.out.println("compression time cost:" + (System.currentTimeMillis() - time));
        time= System.currentTimeMillis();
        byte[] uncompressed = Snappy.uncompress(compressed);
        System.out.println("decompression time cost:" + (System.currentTimeMillis() - time));

        //String result = new String(uncompressed, "UTF-8");
        //assertEquals(input, result);
    }

    @Test
    public void testByteBuffer() throws UnsupportedEncodingException, IOException {
        String input =randomString(5000);
        ByteBuffer source = ByteBuffer.allocateDirect(input.getBytes().length);
        source.put(input.getBytes());
        source.flip();
//        File file=new File("/Users/hxd/Desktop/Fu - 2011 - A review on time series data mining.pdf");
//        ByteBuffer uncompressed = ByteBuffer.allocateDirect((int)file.length());
//        IOUtils.read(FileChannel.open(Paths.get(file.getAbsolutePath())), uncompressed);

        long time=System.currentTimeMillis();
        ByteBuffer compressed = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(source.remaining()));
        Snappy.compress(source, compressed);
        System.out.println("compression time cost:" + (System.currentTimeMillis() - time));
        Snappy.uncompressedLength(compressed);
        time=System.currentTimeMillis();
        ByteBuffer uncompressedByteBuffer = ByteBuffer.allocateDirect(Snappy.uncompressedLength(compressed)+1);
        Snappy.uncompress(compressed, uncompressedByteBuffer);
        System.out.println("decompression time cost:" + (System.currentTimeMillis() - time));
        System.out.println(uncompressedByteBuffer.remaining());
        assert input.equals(ByteBufferUtil.string(uncompressedByteBuffer));
    }


}
