package com.aliware.tianchi;

import org.apache.dubbo.common.URL;

/**
 * 根据阻塞时间调整峰值
 *
 * @author Zhao Qian
 * @date 2019/07/05
 */
public class CongestionHandler {
    // TODO delta可设置为不同机器配置性能 有不一样的数值
    private static final int DELTA = 60;  // 默认60
    private static final double FACTOR = 0.875;  // 默认0.875

    URL url;
    int oldActiveTask;
    int activeTask;

    double oldDelay;
    double delay;

    int maxActiveTask = 250;   // 可以修改，对性能有影响  默认200
    int minActiveTask = 1;

    public CongestionHandler(URL url) {
        oldActiveTask = minActiveTask;
        activeTask = minActiveTask;
        this.url = url;
    }

    private double computeNDG() {
        double abs = delay - oldDelay;
        if (abs < 0) {
            return abs;
        }
        if (abs < 2.5) {
            return -abs;
        } else {
            return abs;
        }
    }

    public void updateMaxActiveTask(int value) {
        maxActiveTask = value;
    }

    public int getMaxActiveTask() {
        return maxActiveTask;
    }

    public int updateActiveTask(double delay) {
        double NDG = computeNDG();
        this.delay = delay;
        int newActiveTask;
        //if (delay > 800) {
        //    newActiveTask = decrease(activeTask);
        //    oldActiveTask = activeTask;
        //    activeTask = newActiveTask;
        //    oldDelay = delay;
        //    return newActiveTask;
        //
        //}
        if (NDG > 0 || activeTask >= maxActiveTask) {
            newActiveTask = decrease(activeTask);
        } else if (NDG <= 0 || activeTask == minActiveTask) {
            newActiveTask = increase(activeTask);
        } else {
            throw new IllegalStateException("未考虑到该状况，需调试分析");
        }
        oldActiveTask = activeTask;
        activeTask = newActiveTask;
        oldDelay = delay;
        return newActiveTask;

    }

    private int increase(int activeTask) {
        return activeTask + DELTA;
    }

    private int increaseDouble(int activeTask) {
        return activeTask + DELTA * 2;
    }

    private int decrease(int activeTask) {
        return (int)Math.round(activeTask * FACTOR);
    }
}
