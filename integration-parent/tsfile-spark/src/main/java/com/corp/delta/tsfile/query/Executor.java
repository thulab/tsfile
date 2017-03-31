package com.corp.delta.tsfile.query;

import com.corp.delta.tsfile.common.utils.TSRandomAccessFileReader;
import com.corp.delta.tsfile.read.query.QueryConfig;
import com.corp.delta.tsfile.read.query.QueryDataSet;
import com.corp.delta.tsfile.read.query.QueryEngine;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class used to execute Querys on TSFile
 */
public class Executor {
    public static List<QueryDataSet> query(TSRandomAccessFileReader in, List<QueryConfig> queryConfigs, HashMap<String, Long> parameters) {
        QueryEngine queryEngine;
        List<QueryDataSet> dataSets = new ArrayList<>();
        try {
            queryEngine = new QueryEngine(in);
            for(QueryConfig queryConfig: queryConfigs) {
                QueryDataSet queryDataSet = queryEngine.query(queryConfig, parameters);
                dataSets.add(queryDataSet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSets;
    }
}
