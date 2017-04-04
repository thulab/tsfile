package cn.edu.thu.tsfile.common.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * logger for tsfile-framework to collect performance info
 * 
 * @author XuYi xuyi556677@163.com
 *
 */
public class ProfilingLog {
  public static Logger HEADER_LOG = LoggerFactory.getLogger("headerLog");
  public static Logger SUBTITLE_LOG = LoggerFactory.getLogger("subTitle");
  public static Logger WRITE_LINE_LOG = LoggerFactory.getLogger("writeLineLog");
  public static Logger WRITE_RESULT_LOG = LoggerFactory.getLogger("writeResultLog");
  public static Logger WRITE_ROWGROUP_LOG = LoggerFactory.getLogger("writeRowGroupLog");
  public static Logger READ_SUB_TITLE_LOG = LoggerFactory.getLogger("readSubTitleLog");

  public static Logger READ_SINGLE_WNF_LOG = LoggerFactory.getLogger("readSingleRowWithoutFilter");
  public static Logger READ_SINGLE_WF_LOG = LoggerFactory.getLogger("readSingleRowWithFilter");
  public static Logger READ_MUTIPLE_WNF_LOG = LoggerFactory.getLogger("readMutipleWithoutFilter");
  public static Logger READ_CROSS_QUERY_LOG = LoggerFactory.getLogger("readCrossQuery");

  public static Logger READ_PAGE_INFO_LOG = LoggerFactory.getLogger("readPageInfo");
  public static Logger READ_COLUMN_WNF_LOG = LoggerFactory.getLogger("readColumnWithoutFilter");
  public static Logger READ_COLUMN_WF_LOG = LoggerFactory.getLogger("readColumnWithFilter");

  public static Logger READ_CROSS_QUERY_STEP_LOG = LoggerFactory.getLogger("readCrossQueryStepLog");
}
