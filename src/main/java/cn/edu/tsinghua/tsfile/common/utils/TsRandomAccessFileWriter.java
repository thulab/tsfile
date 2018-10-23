package cn.edu.tsinghua.tsfile.common.utils;

import java.io.*;

/**
 * @deprecated  using TsFileWriter instead.
 * RandomAccessOutputStream implements the tsfile file writer interface and extends OutputStream. <br>
 * The main difference between RandomAccessOutputStream and general OutputStream
 * is:RandomAccessOutputStream provide method {@code getPos} for random accessing. It also
 * implements {@code getOutputStream} to return an OutputStream supporting tsfile-format
 *
 * @author kangrong
 */
@Deprecated
public class TsRandomAccessFileWriter implements ITsRandomAccessFileWriter {
	private FileOutputStream outputStream;

	public TsRandomAccessFileWriter(File file) throws IOException {
		outputStream = new FileOutputStream(file, true);
	}

	@Override
	public void write(int b) throws IOException {
		outputStream.write(b);
	}

	@Override
	public void write(byte b[]) throws IOException {
		outputStream.write(b);
	}

	@Override
	public long getPos() throws IOException {
		//return out.length();//WTF???
		return outputStream.getChannel().position();
	}

	@Override
	public void truncate(long length) throws IOException {
		outputStream.getChannel().truncate(length);
	}

	@Override
	public void close() throws IOException {
		outputStream.close();
	}

	@Override
	public OutputStream getOutputStream() {
		return outputStream;
	}
}
