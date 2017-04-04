package cn.edu.thu.tsfile.common.constant;

/***
 * this class define several constant string variables used in tsfile schema's
 * keys.
 * 
 * @author kangrong
 *
 */
public class JsonFormatConstant {
    public static final String JSON_SCHEMA = "schema";
    public static final String DELTA_TYPE = "delta_type";
    public static final String MEASUREMENT_UID = "measurement_id";
    public static final String DATA_TYPE = "data_type";
    public static final String MEASUREMENT_ENCODING = "encoding";
    public static final String ENUM_VALUES = "enum_values";
    public static final String ENUM_VALUES_SEPARATOR = ",";
    public static final String PLA_MAX_ERROR = "max_error";
    public static final String SDT_MAX_ERROR = "max_error";
    public static final String MAX_POINT_NUMBER = "max_point_number";
    public static final String COMPRESS_TYPE = "compressor";
    public static final String FreqType = "freq_type";
    public static final String TSRECORD_SEPARATOR = ",";
    public static final String OVERFLOW_RECORD_SEPARATOR = ",";
    public static final String FREQUENCY_ENCODING = "freq_encoding";
    public static final String DFT_PACK_LENGTH = "dft_pack_length";
    public static final String DFT_RATE = "dft_rate";
    public static final String DFT_WRITE_Main_FREQ = "write_main_freq";
    public static final String DFT_WRITE_ENCODING = "write_encoding";
    public static final String DFT_OVERLAP_RATE = "overlap_rate";
    public static final String DFT_MAIN_FREQ_NUM = "dft_main_freq_num";
    public static final String MAX_STRING_LENGTH = "max_string_length";

    public static final String ROW_GROUP_SIZE = "row_group_size";
    public static final String PAGE_SIZE = "page_size";

    public static final String defaultDeltaType = "default_delta_type";
}
