package com.corp.delta.tsfile.compress;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xerial.snappy.Snappy;

import com.corp.delta.tsfile.common.utils.bytesinput.BytesInput;
import com.corp.delta.tsfile.compress.Compressor.NoCompressor;
import com.corp.delta.tsfile.compress.Compressor.SnappyCompressor;
import com.corp.delta.tsfile.compress.UnCompressor.NoUnCompressor;
import com.corp.delta.tsfile.compress.UnCompressor.SnappyUnCompressor;

/**
 * 
 * @author kangrong
 *
 */
public class CompressTest {
    private final String inputString = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
        + "Snappy, a fast compresser/decompresser.";
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

    @Test
    public void noCompressorTest() throws UnsupportedEncodingException, IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(inputString.getBytes("UTF-8"));
      NoCompressor compressor = new NoCompressor();
      NoUnCompressor unCompressor = new NoUnCompressor();
      BytesInput compressed = compressor.compress(BytesInput.from(out));
      byte[] uncompressed = unCompressor.uncompress(compressed.toByteArray());
      String result = new String(uncompressed, "UTF-8");
      assertEquals(inputString, result);
    }

    @Test
    public void snappyCompressorTest() throws UnsupportedEncodingException, IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(inputString.getBytes("UTF-8"));
      SnappyCompressor compressor = new SnappyCompressor();
      SnappyUnCompressor unCompressor = new SnappyUnCompressor();
      BytesInput compressed = compressor.compress(BytesInput.from(out));
      byte[] uncompressed = unCompressor.uncompress(compressed.toByteArray());
      String result = new String(uncompressed, "UTF-8");
      assertEquals(inputString, result);
    }
    
	@Test
	public void snappyTest() throws UnsupportedEncodingException, IOException {
		byte[] compressed = Snappy.compress(inputString.getBytes("UTF-8"));
		byte[] uncompressed = Snappy.uncompress(compressed);

		String result = new String(uncompressed, "UTF-8");
		assertEquals(inputString, result);
	}

}
