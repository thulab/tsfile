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
     * the upper bound of line count maintained in a page
     */
    public int pageCountUpperBound = 1024 * 1024;
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

    public double dftSatisfyRate = 0.1;

    public final int RLE_MIN_REPEATED_NUM = 8;
    public final int RLE_MAX_REPEATED_NUM = 0x7FFFFF;
    public final int RLE_MAX_BIT_PACKED_NUM = 63;

    public final int BITMAP_BITWIDTH = 1;

    public TSFileConfig() {

    }
}
