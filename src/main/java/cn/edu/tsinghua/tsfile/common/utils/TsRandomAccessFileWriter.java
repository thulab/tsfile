package cn.edu.tsinghua.tsfile.common.utils;

import java.io.*;

/**
 * RandomAccessOutputStream implements the tsfile file writer interface and extends OutputStream. <br>
 * The main difference between RandomAccessOutputStream and general OutputStream
 * is:RandomAccessOutputStream provide method {@code getPos} for random accessing. It also
 * implements {@code getOutputStream} to return an OutputStream supporting tsfile-format
 *
 * @author kangrong
 */
public class TsRandomAccessFileWriter implements ITsRandomAccessFileWriter {
	private static final String DEFAULT_FILE_MODE = "rw";
	private RandomAccessFile out;
	private OutputStream outputStream;
	private long offset;

	public TsRandomAccessFileWriter(File file) throws IOException {
		this(file, DEFAULT_FILE_MODE);
	}

	public TsRandomAccessFileWriter(File file, String mode) throws IOException {
		out = new RandomAccessFile(file, mode);
		offset = 0;
		outputStream=new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				TsRandomAccessFileWriter.this.write(b);
				offset += 1;
			}
			@Override
			public void write(byte b[], int off, int len) throws IOException {
				out.write(b, off, len);
				offset += len;
			}
			@Override
			public void write(byte b[]) throws IOException {
				TsRandomAccessFileWriter.this.write(b);
				offset += b.length;
			}

			@Override
			public void close() throws IOException {
				TsRandomAccessFileWriter.this.close();
			}
		};
	}
	
	@Override
	public void write(int b) throws IOException {
		out.write(b);
		offset += 1;
	}

	@Override
	public void write(byte b[]) throws IOException {
		out.write(b);
		offset += b.length;
	}

	@Override
	public long getPos() throws IOException {
		return offset;
	}

	@Override
	public void seek(long offset) throws IOException {
		out.seek(offset);
		this.offset = offset;
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

	@Override
	public OutputStream getOutputStream() {
		return outputStream;
	}
}
