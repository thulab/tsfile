package cn.edu.tsinghua.tsfile.performance;

import java.util.concurrent.ConcurrentHashMap;

public class CostFramework {

    private CostFramework() {
    }

    private static class SingletonHolder {
        private static final CostFramework instance = new CostFramework();
    }

    public static CostFramework getInstance() {
        return SingletonHolder.instance;
    }

    public static boolean turnOn = true;

    // class + method, calc the execute time of given class and method
    private ConcurrentHashMap<String, Long> costs = new ConcurrentHashMap<>();

    // class + method, calc the invoke time of given class and method
    private ConcurrentHashMap<String, Integer> times = new ConcurrentHashMap<>();

    public void addCost(String className, String methodName, long timeCost) {
        if (!costs.containsKey(getKey(className, methodName))) {
            costs.put(getKey(className, methodName), timeCost);
        } else {
            costs.put(getKey(className, methodName), costs.get(getKey(className, methodName)) + timeCost);
        }

    }

    public void clear() {
        costs.clear();
        times.clear();
    }

    private String getKey(String className, String methodName) {
        return className + "#" + methodName;
    }

}
