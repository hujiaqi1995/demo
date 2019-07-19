package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance {
    private static final int DEFUALT_WEIGHT = 100;
    public static volatile UserLoadBalance INSTANCE;
    public Map<URL, CongestionHandler> limitMap = new HashMap<>();
    private Map<URL, Integer> map1 = new HashMap<>();
    private Map<URL, Integer> map2 = new HashMap<>();

    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        int length = invokers.size();

        int[] weights = new int[length];
        int[] indexes = new int[length];
        int j = 0;
        int totalWeight = 0;

        for (int i = 0; i < length; i++, j++) {
            Invoker<T> invoker = invokers.get(i);
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();
            if(limitMap.get(invoker.getUrl()) != null) {
                int max = limitMap.get(invoker.getUrl()).getMaxActiveTask();
                // 如果active太大，放弃这个节点
                if(active > max * 0.95) {
                    continue;
                }
            }
            indexes[j] = i;
            Integer windowSize = map1.get(invoker.getUrl());
            //            active 越小，weight越大
            if(windowSize == null) {
                weights[j] = DEFUALT_WEIGHT;
            } else {
                // TODO 如果为负的怎么办
                weights[j] = windowSize - active;
            }
            totalWeight += weights[j];
        }

        if(totalWeight > 0) {
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < j; i++) {
                int weight = weights[i];
                offsetWeight -= weight;
                if (offsetWeight < 0) {
                    return invokers.get(indexes[i]);
                }
            }
        }

        return invokers.get(ThreadLocalRandom.current().nextInt(length));

    }
    public UserLoadBalance() {
        System.out.println("UserLoadBalance init!");
        if(INSTANCE == null) {
            // TOOD 研究下内存可见性问题，在构造函数中向外传递引用是否可能导致逃逸。
            INSTANCE = this;
        }
    }

    synchronized public void updateActiveLimit(Map<Invoker, Double> map) {
        map2.clear();
        map.forEach((invoker, delay) -> {
            CongestionHandler avoidence = limitMap.get(invoker.getUrl());
            if(avoidence == null) {
                limitMap.put(invoker.getUrl(), new CongestionHandler(invoker.getUrl()));
                avoidence = limitMap.get(invoker.getUrl());
            }
            int newLimit = avoidence.updateActiveTask(delay);
            map2.put(invoker.getUrl(), newLimit);

        });
        Map<URL, Integer> tmp = map1;
        map1 = map2;
        map2 = tmp;
    }
}
