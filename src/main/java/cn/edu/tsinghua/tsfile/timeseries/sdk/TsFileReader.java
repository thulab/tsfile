package cn.edu.tsinghua.tsfile.timeseries.sdk;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;

import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

public class TsFileReader extends FileReader{
	cn.edu.tsinghua.tsfile.timeseries.read.FileReader reader;
	public TsFileReader(File file) throws FileNotFoundException {
		super(file);
	}
	public TsFileReader(FileDescriptor fd) {
		super(fd);
	}

	public TsFileReader(String fileName) throws FileNotFoundException {
		super(fileName);
	}
	@Override
	public String getEncoding() {
		return super.getEncoding();
	}
	/**
	 * read next record
	 * @return the next record, or null at the end of the file.
	 * @throws IOException
	 */
	public TSRecord readRecord() throws IOException{
		return null;
	}
	@Override
	public int read() throws IOException {
		throw new IOException("not supported. Use readRecord() instead.");
	}
	
	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException {
		// TODO Auto-generated method stub
		return super.read(cbuf, offset, length);
	}
	@Override
	public boolean ready() throws IOException {
		return super.ready();
	}
	@Override
	public void close() throws IOException {
		super.close();
	}
	@Override
	public int read(CharBuffer target) throws IOException {
		// TODO Auto-generated method stub
		return super.read(target);
	}
	@Override
	public int read(char[] cbuf) throws IOException {
		// TODO Auto-generated method stub
		return super.read(cbuf);
	}
	@Override
	public long skip(long n) throws IOException {
		// TODO Auto-generated method stub
		return super.skip(n);
	}
	@Override
	public boolean markSupported() {
		// TODO Auto-generated method stub
		return super.markSupported();
	}
	@Override
	public void mark(int readAheadLimit) throws IOException {
		// TODO Auto-generated method stub
		super.mark(readAheadLimit);
	}
	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		super.reset();
	}

	
}
