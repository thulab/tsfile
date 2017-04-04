package cn.edu.thu.tsfile.timeseries.write.record;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.utils.StringContainer;

/**
 * TSRecord is a kind of format that TSFile receives.TSRecord contains timestamp, deltaObjectId and
 * a list of data points.
 * 
 * @author kangrong
 *
 */
public class TSRecord {
    public long time;
    public String deltaObjectId;
    public List<DataPoint> dataPointList = new ArrayList<DataPoint>();

    public TSRecord(long timestamp, String deltaObjectId) {
        this.time = timestamp;
        this.deltaObjectId = deltaObjectId;
    }

    public void addTuple(DataPoint tuple) {
        this.dataPointList.add(tuple);
    }

    public String toString() {
        StringContainer sc = new StringContainer(" ");
        sc.addTail("{delta object id:", deltaObjectId, "time:", time, ",data:[");
        for (DataPoint tuple : dataPointList) {
            sc.addTail(tuple);
        }
        sc.addTail("]}");
        return sc.toString();
    }
}
