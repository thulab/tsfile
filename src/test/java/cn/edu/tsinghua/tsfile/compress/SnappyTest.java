package cn.edu.tsinghua.tsfile.compress;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import cn.edu.tsinghua.tsfile.common.utils.ByteBufferUtil;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

/**
 * 
 * @author kangrong
 *
 */
public class SnappyTest {

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
        File file=new File("/Users/hxd/Desktop/Fu - 2011 - A review on time series data mining.pdf");
        byte[] uncom = new byte[(int)file.length()];
        IOUtils.read(new FileInputStream(file), uncom);
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
//        String input =
//                "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
//                        + "Snappy, a fast compresser/decompresser.";
//        ByteBuffer uncompressed = ByteBuffer.allocateDirect(input.getBytes().length);
//        uncompressed.put(input.getBytes());
//        uncompressed.flip();
        File file=new File("/Users/hxd/Desktop/Fu - 2011 - A review on time series data mining.pdf");
        ByteBuffer uncompressed = ByteBuffer.allocateDirect((int)file.length());
        IOUtils.read(FileChannel.open(Paths.get(file.getAbsolutePath())), uncompressed);


        long time=System.currentTimeMillis();
        ByteBuffer compressed = ByteBuffer.allocateDirect(Snappy.maxCompressedLength(uncompressed.remaining()));
        Snappy.compress(uncompressed, compressed);
        System.out.println("compression time cost:" + (System.currentTimeMillis() - time));

        time=System.currentTimeMillis();
        ByteBuffer uncompressedByteBuffer = ByteBuffer.allocateDirect(Snappy.uncompressedLength(compressed)+1);
        Snappy.uncompress(compressed, uncompressedByteBuffer);
        System.out.println("decompression time cost:" + (System.currentTimeMillis() - time));
        System.out.println(uncompressedByteBuffer.remaining());
        System.out.println(ByteBufferUtil.string(uncompressedByteBuffer));

    }

}
