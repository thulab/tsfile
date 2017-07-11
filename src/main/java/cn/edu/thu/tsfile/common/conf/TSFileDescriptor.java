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
			url = url + "/conf/" + TSFileConfig.CONFIG_NAME;
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
		    conf.groupSizeInByte = Integer.parseInt(properties.getProperty("group_size_in_byte", conf.groupSizeInByte+""));
		    conf.pageSizeInByte = Integer.parseInt(properties.getProperty("page_size_in_byte",conf.pageSizeInByte+""));
		    conf.maxNumberOfPointsInPage = Integer.parseInt(properties.getProperty("max_number_of_points_in_page", conf.maxNumberOfPointsInPage+""));
		    conf.timeSeriesDataType = properties.getProperty("time_series_data_type",conf.timeSeriesDataType);
		    conf.maxStringLength = Integer.parseInt(properties.getProperty("max_string_length",conf.maxStringLength+""));
		    conf.floatPrecision = Integer.parseInt(properties.getProperty("float_precision", conf.floatPrecision+""));		    
		    conf.timeSeriesEncoder = properties.getProperty("time_series_encoder", conf.timeSeriesEncoder);
		    conf.valueEncoder = properties.getProperty("value_encoder", conf.valueEncoder);
		    conf.compressor = properties.getProperty("compressor", conf.compressor);		    
		} catch (IOException e) {
		    LOGGER.warn("Cannot load config file, use default configuration", e);
		} catch (Exception e) {
			LOGGER.error("Loading settings {} failed.", url, e);
			//System.exit(1);
		} finally {
			if (inputStream != null) {
				try {
					inputStream.close();
					inputStream = null;
				} catch (IOException e) {
				}
			}
		}
	}
}
