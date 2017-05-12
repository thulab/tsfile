package cn.edu.thu.tsfile.common.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.constant.SystemConstant;


/**
 * TSFileDescriptor is used to load TSFileConfig and provide configure
 * information
 *
 * @author kangrong
 */
public class TSFileDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(TSFileDescriptor.class);

	private static TSFileDescriptor descriptor = new TSFileDescriptor();

	private final String CONFIG_DEFAULT_PATH = "/tsfile.properties";

	private TSFileDescriptor() {
		loadYaml();
	}

	public static TSFileDescriptor getInstance() {
		return descriptor;
	}

	public TSFileConfig getConfig() {
		return conf;
	}

	private TSFileConfig conf = new TSFileConfig();

	/**
	 * load an yaml file and set TSFileConfig variables
	 *
	 */
	private void loadYaml() {
		String url = System.getProperty(SystemConstant.TSFILE_HOME, CONFIG_DEFAULT_PATH);
		InputStream inputStream = null;
		if (url.equals(CONFIG_DEFAULT_PATH)) {
			inputStream = this.getClass().getResourceAsStream(url);
			return;
		} else {
			url = url + "/conf/tsfile.properties";
			try {
				File file = new File(url);
				inputStream = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				LOGGER.error("Fail to find config file {}", url, e);
				System.exit(1);
			}
		}
		LOGGER.info("Start to read config file {}", url);
		Properties properties = new Properties();
		try {
		    properties.load(inputStream);
		    
		    conf.rowGroupSize = Integer.parseInt(properties.getProperty("rowGroupSize", conf.rowGroupSize+""));
		    conf.pageSize = Integer.parseInt(properties.getProperty("pageSize",conf.pageSize+""));
		    conf.timeSeriesEncoder = properties.getProperty("timeSeriesEncoder", conf.timeSeriesEncoder);
		    conf.defaultSeriesEncoder = properties.getProperty("defaultSeriesEncoder", conf.defaultSeriesEncoder);
		    conf.compressName = properties.getProperty("compressName", conf.compressName);
		    conf.defaultRleBitWidth = Integer.parseInt(properties.getProperty("defaultRleBitWidth", conf.defaultRleBitWidth+""));
		    conf.defaultEndian = properties.getProperty("defaultEndian", conf.defaultEndian);
		    conf.defaultDeltaBlockSize = Integer.parseInt(properties.getProperty("defaultDeltaBlockSize", conf.defaultDeltaBlockSize+""));
		    conf.defaultPLAMaxError = Double.parseDouble(properties.getProperty("defaultPLAMaxError", conf.defaultPLAMaxError+""));
		    conf.defaultSDTMaxError = Double.parseDouble(properties.getProperty("defaultSDTMaxError", conf.defaultSDTMaxError+""));
		    
		} catch (IOException e) {
		    LOGGER.warn("Cannot load config file, use default configuration", e);
		} catch (Exception e) {
		    LOGGER.warn("Error format in config file, use default configuration", e);
		}
		if(inputStream != null){
		    try {
			inputStream.close();
		    } catch (IOException e) {
			LOGGER.error("Fail to close config file input stream", e);
		    }
		}
	}

	public static void main(String[] args) {
	    TSFileDescriptor descriptor = TSFileDescriptor.getInstance();
	    TSFileConfig config = descriptor.getConfig();
	    
	    System.out.println(config.rowGroupSize);
	    System.out.println(config.pageSize);
	    System.out.println(config.timeSeriesEncoder);
	    System.out.println(config.defaultSeriesEncoder);
	    System.out.println(config.compressName);
	    System.out.println(config.defaultRleBitWidth);
	    System.out.println(config.defaultEndian);
	    System.out.println(config.defaultDeltaBlockSize);
	    System.out.println(config.defaultPLAMaxError);
	    System.out.println(config.defaultSDTMaxError);
	}
}
