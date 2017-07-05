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
 * TSFileDescriptor is used to load TSFileConfig and provide configure information
 *
 * @author kangrong
 */
public class TSFileDescriptor {
	private static final Logger LOGGER = LoggerFactory.getLogger(TSFileDescriptor.class);

	private static TSFileDescriptor descriptor = new TSFileDescriptor();

	private TSFileDescriptor() {
		loadProps();
	}

	public static TSFileDescriptor getInstance() {
		return descriptor;
	}

	public TSFileConfig getConfig() {
		return conf;
	}

	private TSFileConfig conf = new TSFileConfig();

	/**
	 * load an .properties file and set TSFileConfig variables
	 *
	 */
	private void loadProps() {
		String url = System.getProperty(SystemConstant.TSFILE_HOME, TSFileConfig.CONFIG_DEFAULT_PATH);
		InputStream inputStream = null;
		if (url.equals(TSFileConfig.CONFIG_DEFAULT_PATH)) {
			try {
			    inputStream = new FileInputStream(new File(url));
			} catch (FileNotFoundException e) {
			    LOGGER.error("Fail to find config file {}", url, e);
			    return;
			}
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
		    conf.maxPointNumberInPage = Integer.parseInt(properties.getProperty("maxPointNumberInPage", conf.maxPointNumberInPage+""));
		    conf.timeDataType = properties.getProperty("timeDataType",conf.timeDataType);
		    conf.maxStringLength = Integer.parseInt(properties.getProperty("maxStringLength",conf.maxStringLength+""));
		    conf.floatPrecision = Integer.parseInt(properties.getProperty("floatPrecision", conf.floatPrecision+""));		    
		    conf.timeSeriesEncoder = properties.getProperty("timeSeriesEncoder", conf.timeSeriesEncoder);
		    conf.valueSeriesEncoder = properties.getProperty("valueSeriesEncoder", conf.valueSeriesEncoder);
		    conf.compressor = properties.getProperty("compressor", conf.compressor);		    
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
}
