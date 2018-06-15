package cn.edu.tsinghua.ts2file.timesegment.write.record;

import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;
import java.util.ArrayList;
import java.util.List;


public class Ts2Record {
    public long startTime;
    public long endTime;
    public String deltaObjectId;
    public List<SegmentDataPoint> dataPointList = new ArrayList<>();

    public Ts2Record(long startTime, long endTime, String deltaObjectId) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.deltaObjectId = deltaObjectId;
    }

    public void addTuple(SegmentDataPoint tuple) {
        this.dataPointList.add(tuple);
    }

    public String toString() {
        StringContainer sc = new StringContainer(" ");
        sc.addTail("{delta object id:", deltaObjectId, "startTime:", startTime, "endTime:", endTime,
            ",data:[");
        for (SegmentDataPoint tuple : dataPointList) {
            sc.addTail(tuple);
        }
        sc.addTail("]}");
        return sc.toString();
    }
}
