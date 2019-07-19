package com.aliware.tianchi;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcStatus;

/**
 * @author daofeng.xjf
 *
 * 客户端过滤器
 * 可选接口
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = Constants.CONSUMER)
public class TestClientFilter implements Filter {

    private ConcurrentHashMap<Invoker, ConcurrentLinkedQueue<Integer>> timeQueue1;
    private ConcurrentHashMap<Invoker, ConcurrentLinkedQueue<Integer>> timeQueue2;
    private ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,new NamedThreadFactory("TestClientFilter-Config-Refresher"));


    public TestClientFilter() {
        timeQueue1 = new ConcurrentHashMap<>();
        timeQueue2 = new ConcurrentHashMap<>();
        executorService.scheduleAtFixedRate(new MonitorTask(),1000,200,TimeUnit.MILLISECONDS);
        updateMaxPool();
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        String methodName = invocation.getMethodName();
        int max = invoker.getUrl().getMethodParameter(methodName, Constants.ACTIVES_KEY, 0);
        RpcStatus.beginCount(url, methodName, max);

        long begin = System.currentTimeMillis();
        try {
            invocation.getAttachments().put("start", String.valueOf(begin));
            Result result = invoker.invoke(invocation);
            return result;
        } catch (RuntimeException e) {
            // 调用异常
            throw e;
        }

    }

    @Override
    public Result onResponse(Result result, Invoker<?> invoker, Invocation invocation) {

        URL url = invoker.getUrl();
        String methodName = invocation.getMethodName();
        boolean isSuccess = true;
        if (result.hasException()) {
            isSuccess = false;
        } else {
            Long start = Long.valueOf(invocation.getAttachment("start"));
            // 从队列中获取时间存储队列
            ConcurrentLinkedQueue<Integer> queue = timeQueue1.get(url.getHost());
            if (queue == null) {
                timeQueue1.putIfAbsent(invoker, new ConcurrentLinkedQueue<>());
                queue = timeQueue1.get(invoker);
            }
            queue.offer((int)(System.currentTimeMillis() - start));
        }
        RpcStatus.endCount(url, methodName, 0, isSuccess);
        return result;
    }

    public void updateMaxPool() {
        CallbackListenerImpl.sizeMap.forEach((str, value) -> {
            if (UserLoadBalance.INSTANCE != null) {
                for (URL url : UserLoadBalance.INSTANCE.limitMap.keySet()) {
                    if (url.toString().contains(str)) {
                        UserLoadBalance.INSTANCE.limitMap.get(url).updateMaxActiveTask(value);
                    }
                }
            }
        });
    }

    private class MonitorTask extends TimerTask {
        @Override
        public void run() {
            updateMaxPool();

            ConcurrentHashMap<Invoker, ConcurrentLinkedQueue<Integer>> tmp = timeQueue1;
            timeQueue1 = timeQueue2;
            timeQueue2 = tmp;
            //timeQueue2 = timeQueue1;
            //timeQueue1.clear();
            if (timeQueue2.isEmpty()) {
                return;
            }
            Map<String, Double> avgMap = new HashMap<>();
            Map<Invoker, Double> avgMap2 = new HashMap<>();
            for (Map.Entry<Invoker, ConcurrentLinkedQueue<Integer>> item : timeQueue2.entrySet()) {
                String key = item.getKey().getUrl().getHost();
                ConcurrentLinkedQueue<Integer> queue = item.getValue();
                int sum = 0;
                int count = 0;
                if (queue.isEmpty()) {
                    continue;
                }
                while (true) {
                    Integer t = queue.poll();
                    if (t == null) {
                        break;
                    }
                    sum += t;
                    count += 1;
                }
                avgMap.put(key, (double)sum / count);
                avgMap2.put(item.getKey(), (double)sum / count);
            }
            if (avgMap.isEmpty()) {
                return;
            }
            if (UserLoadBalance.INSTANCE != null) {
                UserLoadBalance.INSTANCE.updateActiveLimit(avgMap2);
            }
        }

        private String mapToString(Map<String, Double> map) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Double> item : map.entrySet()) {
                sb.append(String.format("%6s: %5.1fms ", item.getKey(),
                    item.getValue()));
            }
            return sb.toString();
        }
    }
}
