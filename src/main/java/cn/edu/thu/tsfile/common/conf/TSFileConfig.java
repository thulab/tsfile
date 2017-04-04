package cn.edu.thu.tsfile.common.conf;

import cn.edu.thu.tsfile.common.constant.SystemConstant;

/**
 * TSFileConfig is a configure class. Every variables is public and has default
 * value.
 *
 * @author kangrong
 */
public class TSFileConfig {
    /**
     * String encoder with UTF-8 encodes a character to at most 4 bytes.
     */
    public static final int byteSizePerChar = 4;
    /**
     * line count threshold for checking page memory occupied size
     */
    public int pageCheckSizeThreshold = 100;
    /**
     * default data type of time is LONG
     */
    public String defaultTimeType = "INT64";
    /**
     * current version is 0
     */
    public int currentVersion = 0;

    /**
     * max length limitation of input string
     */
    public int defaultMaxStringLength = 128;
    /**
     * the memory size threshold for flushing to disk or HDFS, default value is
     * 128 * 1024 * 1024
     */
    public int rowGroupSize = 128 * 1024 * 1024;
    /**
     * the memory size for each series writer to pack page, default value is
     * 8*1024
     */
    public int pageSize = 1024 * 1024;
    /**
     * compress type, default value is UNCOMPRESSED
     */
    public String compressName = "UNCOMPRESSED";
    // public CompressionTypeName compressName = CompressionTypeName.SNAPPY;
    /**
     * default frequency type if series writer hasn't set it, the value is
     * SINGLE_FREQ
     */
    public String defaultFreqType = "SINGLE_FREQ";
    /**
     * the default Endian value is LITTLE_ENDIAN
     */
    public String defaultEndian = "LITTLE_ENDIAN";

    // encoder configuration
    /**
     * the default time series value is TS_2DIFF
     */
    public String timeSeriesEncoder = "TS_2DIFF";
    // public TSEncoding timeSeriesEncoder = TSEncoding.RLE;
    /**
     * the default value series value is RLE
     */
    public String defaultSeriesEncoder = "RLE";
    /**
     * the default width of RLE encoding is 8
     */
    public int defaultRleBitWidth = 8;
    /**
     * the default block size of two-diff. delta encoding is 128
     */
    public int defaultDeltaBlockSize = 128;
    /**
     * the default PLA max error is 100
     */
    public double defaultPLAMaxError = 100;
    /**
     * the default SDT max error is 100
     */
    public double defaultSDTMaxError = 100;
    /**
     * the default point number is 2
     */
    public int defaultMaxPointNumber = 2;
    /**
     * the default point number is 10000
     */
    public int defaultDFTPackLength = 10000;
    /**
     * the default point number is 0.4
     */
    public double defaultDFTRate = 0.4;
    /**
     * the default DFTWriteMain is false
     */
    public boolean defaultDFTWriteMain = false;
    /**
     * the default DFTWriteEncoding is false
     */
    public boolean defaultDFTWriteEncoding = false;
    /**
     * the default overlap rate is 0
     */
    public float defaultDFTOverlapRate = 0;
    /**
     * the default main frequency number is 1
     */
    public int defaultDFTMainFreqNum = 2;

    public double dftSatisfyRate = 0.1;
    /**
     * the maximum number of writing instances existing in same time.
     */
    public int writeInstanceThreshold = 5;

    /**
     * data directory of Overflow data
     */
    public String overflowDataDir = "src/main/resources/data/overflow";
    /**
     * data directory of fileNode data
     */
    public String FileNodeDir = "src/main/resources/data/digest";
    /**
     * data directory of bufferWrite data
     */
    public String BufferWriteDir = "src/main/resources/data/delta";

    public String metadataDir = "src/main/resources/metadata";

    public String derbyHome = "src/main/resources/derby";

    /**
     * maximum concurrent thread number for merging overflow
     */
    public int mergeConcurrentThreadNum = 10;
    /**
     * the maximum number of concurrent file node instances
     */
    public int maxFileNodeNum = 1000;
    /**
     * the maximum number of concurrent overflow instances
     */
    public int maxOverflowNodeNum = 100;
    /**
     * the maximum number of concurrent buffer write instances
     */
    public int maxBufferWriteNodeNum = 50;
    public int defaultFetchSize = 5000;
    public String writeLogPath = "src/main/resources/writeLog.log";

    public final int RLE_MIN_REPEATED_NUM = 8;
    public final int RLE_MAX_REPEATED_NUM = 0x7FFFFF;
    public final int RLE_MAX_BIT_PACKED_NUM = 63;

    public final int BITMAP_BITWIDTH = 1;

    public TSFileConfig() {
	String home = System.getProperty(SystemConstant.TSFILE_HOME, "");
	if (!home.equals("")) {
	    overflowDataDir = home + "/data/overflow";
	    FileNodeDir = home + "/data/digest";
	    BufferWriteDir = home + "/data/delta";
	    writeLogPath = home + "/data/writeLog.log";
	    metadataDir = home + "/data/metadata";
	    derbyHome = home + "/data/derby";
	}
    }
}
