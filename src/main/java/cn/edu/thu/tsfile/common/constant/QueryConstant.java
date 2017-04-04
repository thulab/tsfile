package cn.edu.thu.tsfile.common.constant;

public class QueryConstant {
    // Read according to the HDFS partition
    public static final String READ_FROM_PARTITION = "read_from_partition";
    // The start offset for the partition
    public static final String PARTITION_START_OFFSET = "partition_start_offset";
    // The end offset for the partition
    public static final String PARTITION_END_OFFSET = "partition_end_offset";
    // all_devices in one partition
    public static final String ALL_DEVICES = "all_devices";

    public static final String TABLE_SCHEMA = "table_schema";
    public static final String UNION_TABLE = "union_table";
    public static final String LONG_TABLE = "long_table";
}
