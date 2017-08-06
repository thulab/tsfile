package cn.edu.thu.tsfile.timeseries.read.management;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;

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
public class FileStreamManager {
    private static final Logger logger = LoggerFactory.getLogger(FileStreamManager.class);
    
	private static class FileStreamManagerHolder{
		private static final FileStreamManager INSTANCE = new FileStreamManager();
	}
	
    private FileStreamManager() {
    }

    public static final FileStreamManager getInstance() {
        return FileStreamManagerHolder.INSTANCE;
    }

    public TSRandomAccessFileReader getLocalRandomAcessFileReader(String path) throws FileNotFoundException {
        return new LocalFileInput(path);
    }

    public void closeLocalRandomAcessFileReader(LocalFileInput localFileInput) throws IOException {
        localFileInput.close();
    }

    public void close(TSRandomAccessFileReader raf) {
        try {
            raf.close();
        } catch (IOException e) {
            logger.error("Error when close RAF: {}", e.getMessage());
        }
    }
}
