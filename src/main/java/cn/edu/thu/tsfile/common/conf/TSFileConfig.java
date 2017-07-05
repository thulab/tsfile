package cn.edu.thu.tsfile.common.conf;

/**
 * TSFileConfig is a configure class. Every variables is public and has default
 * value.
 *
 * @author kangrong
 */
public class TSFileConfig {
    // Memory configuration
    /**
     * The memory size threshold for flushing to disk or HDFS, default value is
     * 128 * 1024 * 1024
     */
    public int rowGroupSize = 128 * 1024 * 1024;
    /**
     * The memory size for each series writer to pack page, default value is
     * 1024 * 1024
     */
    public int pageSize = 1024 * 1024;
    /**
     * The maximum number of data points in a page, defalut 1024*1024
     */
    public int maxPointNumberInPage = 1024 * 1024;
    
    // Data type configuration
    /**
     * Default data type of time is LONG
     */
    public String timeDataType = "INT64";
    /**
     * Max length limitation of input string
     */
    public int maxStringLength = 128;    
    /**
     * Floating-point precision
     */
    public int floatPrecision = 2;
    
    // Encoder configuration
    /**
     * the default time series value is TS_2DIFF
     */
    public String timeSeriesEncoder = "TS_2DIFF";
    /**
     * the default value series value is RLE
     */
    public String valueSeriesEncoder = "RLE";
    
    // RLE configuration
    /**
     * Default bit width of RLE encoding is 8
     */
    public int rleBitWidth = 8;
    public final int RLE_MIN_REPEATED_NUM = 8;
    public final int RLE_MAX_REPEATED_NUM = 0x7FFFFF;
    public final int RLE_MAX_BIT_PACKED_NUM = 63;    
    
    // TS_2DIFF configuration
    /**
     * Default block size of two-diff. delta encoding is 128
     */
    public int deltaBlockSize = 128;    
    
    // Bitmap configuration
    public final int BITMAP_BITWIDTH = 1;
    
    // Freq encoder configuration
    /**
     * default frequency type if series writer hasn't set it, the value is
     * SINGLE_FREQ
     */
    public String freqType = "SINGLE_FREQ";
    /**
     * Default PLA max error is 100
     */
    public double plaMaxError = 100;
    /**
     * Default SDT max error is 100
     */
    public double sdtMaxError = 100;
    
    public double dftSatisfyRate = 0.1;

    // Compression configuration
    /**
     * Data compression method, TsFile supports UNCOMPRESSED or SNAPPY. 
     * Default value is UNCOMPRESSED which means no compression
     */
    public String compressor = "UNCOMPRESSED";
    
    // Don't change the following configuration
    
    /**
     * line count threshold for checking page memory occupied size
     */
    public int pageCheckSizeThreshold = 100;

    /**
     * current version is 0
     */
    public int currentVersion = 0;

    /**
     * query page data while writing tsfile
     */
    public boolean duplicateIncompletedPage = false;

    /**
     * the default Endian value is LITTLE_ENDIAN
     */
    public String endian = "LITTLE_ENDIAN";

    /**
     * String encoder with UTF-8 encodes a character to at most 4 bytes.
     */
    public static final int byteSizePerChar = 4;
    
    public static String CONFIG_DEFAULT_PATH = "src/test/resources/tsfile.properties";
    
    public TSFileConfig() {

    }
}
