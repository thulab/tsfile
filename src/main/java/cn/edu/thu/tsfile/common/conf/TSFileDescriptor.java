package cn.edu.thu.tsfile.common.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

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

	private final String CONFIG_DEFAULT_PATH = "/tsfile.yaml";

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
			url = url + "/conf/tsfile.yaml";
			try {
				File file = new File(url);
				inputStream = new FileInputStream(file);
			} catch (FileNotFoundException e) {
				LOGGER.error("Fail to find config file {}", url, e);
				System.exit(1);
			}
		}
		LOGGER.info("start to read config file {}", url);
		try {
			Yaml yaml = new Yaml();
			conf = yaml.loadAs(inputStream, TSFileConfig.class);
		} catch (Exception e) {
			LOGGER.error("Loading settings {} failed.", url, e);
			System.exit(1);
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
