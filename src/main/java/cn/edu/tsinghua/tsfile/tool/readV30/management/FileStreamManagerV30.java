package cn.edu.tsinghua.tsfile.tool.readV30.management;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This class provides some function to get one FileReader for one path.
 * Maybe in the later version, every FileReader will be managed by this class.
 *
 * @author Jinrui Zhang
 */
public class FileStreamManagerV30 {
    private static final Logger logger = LoggerFactory.getLogger(cn.edu.tsinghua.tsfile.timeseries.read.management.FileStreamManager.class);
    
	private static class FileStreamManagerHolder{
		private static final cn.edu.tsinghua.tsfile.timeseries.read.management.FileStreamManager INSTANCE = new cn.edu.tsinghua.tsfile.timeseries.read.management.FileStreamManager();
	}
	
    private FileStreamManagerV30() {
    }

    public static final cn.edu.tsinghua.tsfile.timeseries.read.management.FileStreamManager getInstance() {
        return FileStreamManagerHolder.INSTANCE;
    }

    public ITsRandomAccessFileReader getLocalRandomAccessFileReader(String path) throws FileNotFoundException {
        return new TsRandomAccessLocalFileReader(path);
    }

    public void closeLocalRandomAccessFileReader(TsRandomAccessLocalFileReader localFileInput) throws IOException {
        localFileInput.close();
    }

    public void close(ITsRandomAccessFileReader raf) {
        try {
            raf.close();
        } catch (IOException e) {
            logger.error("Error when close RAF: {}", e.getMessage());
        }
    }
}
