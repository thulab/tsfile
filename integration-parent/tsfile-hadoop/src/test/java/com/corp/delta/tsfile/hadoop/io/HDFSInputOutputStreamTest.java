package com.corp.delta.tsfile.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class HDFSInputOutputStreamTest {

	private HDFSInputStream hdfsInputStream = null;
	private HDFSOutputStream hdfsOutputStream = null;
	private int lenOfBytes = 50;
	private byte b = 10;
	private byte[] bs = new byte[lenOfBytes];
	private byte[] rbs = new byte[lenOfBytes];
	private String filename = "testinputandoutputstream.file";
	private Path path;
	private FileSystem fileSystem;

	@Before
	public void setUp() throws Exception {
		hdfsOutputStream = new HDFSOutputStream(filename, new Configuration(), true);
		path = new Path(filename);
		hdfsInputStream = new HDFSInputStream(filename);
	}

	@After
	public void tearDown() throws Exception {
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}

	}

	@Test
	public void test() throws Exception {
		// write one byte
		hdfsOutputStream.write(b);
		assertEquals(1, hdfsOutputStream.getPos());
		hdfsOutputStream.close();
		fileSystem = FileSystem.get(new Configuration());
		assertEquals(true, fileSystem.exists(path));
		fileSystem.delete(path, true);
		// write bytes
		hdfsOutputStream = new HDFSOutputStream(filename, new Configuration(), true);
		hdfsOutputStream.write(bs);
		assertEquals(bs.length, hdfsOutputStream.getPos());
		hdfsOutputStream.close();
		// read bytes using hdfs inputstream
		hdfsInputStream = new HDFSInputStream(filename);
		assertEquals(0, hdfsInputStream.getPos());
		assertEquals(lenOfBytes, hdfsInputStream.length());
		hdfsInputStream.seek(10);
		assertEquals(10, hdfsInputStream.getPos());
		hdfsInputStream.seek(0);
		hdfsInputStream.read(rbs, 0, rbs.length);
		assertEquals(lenOfBytes, hdfsInputStream.getPos());
		assertArrayEquals(bs, rbs);
	}

}