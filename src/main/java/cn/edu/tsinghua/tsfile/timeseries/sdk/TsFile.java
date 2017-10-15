package cn.edu.tsinghua.tsfile.timeseries.sdk;

import java.io.File;
import java.net.URI;


public class TsFile extends File{
	private static final long serialVersionUID = -8406927036748411639L;

	public TsFile(File parent, String child) {
		super(parent, child);
	}

	public TsFile(String parent, String child) {
		super(parent, child);
	}

	public TsFile(String pathname) {
		super(pathname);
	}

	public TsFile(URI uri) {
		super(uri);
	}

}
