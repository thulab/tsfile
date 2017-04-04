package cn.edu.thu.tsfile.timeseries.read;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import cn.edu.thu.tsfile.compress.UnCompressor;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.format.PageHeader;

/**
 * @description PageReader is used to read a page in a column.
 * @author Jinrui Zhang
 *
 */
public class PageReader {
	private ByteArrayInputStream bis;
	private PageHeader pageHeader = null;
	private UnCompressor unCompressor = null;

	public PageReader(ByteArrayInputStream bis, CompressionTypeName compressionTypeName) {
		this.bis = bis;
		unCompressor = UnCompressor.getUnCompressor(compressionTypeName);
	}

	public boolean hasNextPage() {
		if (bis.available() > 0)
			return true;
		return false;
	}

	public PageHeader getNextPageHeader() throws IOException {
		if (pageHeader != null) {
			return pageHeader;
		}
		if (bis.available() > 0) {
			pageHeader = ReadWriteThriftFormatUtils.readPageHeader(bis);
			return pageHeader;

		}
		return null;
	}

	public ByteArrayInputStream getNextPage() throws IOException {
		if (bis.available() > 0) {
			pageHeader = getNextPageHeader();
			int pageSize = pageHeader.getCompressed_page_size();
			byte[] pageContent = new byte[pageSize];
			bis.read(pageContent, 0, pageSize);
			pageContent = unCompressor.uncompress(pageContent);
			pageHeader = null;
			return new ByteArrayInputStream(pageContent);
		}
		return null;
	}

	public void readPage(InputStream in, byte[] buf, int pageSize) throws IOException {
		in.read(buf, 0, pageSize);
	}

	public void skipCurrentPage() {
		long skipSize = this.pageHeader.getCompressed_page_size();
		bis.skip(skipSize);
		pageHeader = null;
	}
}
