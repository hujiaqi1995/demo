package com.aliware.tianchi;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.store.DataStore;
import org.apache.dubbo.rpc.listener.CallbackListener;
import org.apache.dubbo.rpc.service.CallbackService;

import java.util.Date;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author daofeng.xjf
 * <p>
 * 服务端回调服务
 * 可选接口
 * 用户可以基于此服务，实现服务端向客户端动态推送的功能
 */
public class CallbackServiceImpl implements CallbackService {

    private static volatile CallbackServiceImpl INSTANCE = null;


    public static void sendServerMsg(String msg) {
        if(INSTANCE != null) {
            INSTANCE._sendServerMsg(msg);
        }
    }

    public CallbackServiceImpl() {

    }

    /**
     * key: listener type
     * value: callback listener
     */
    private final Map<String, CallbackListener> listeners = new ConcurrentHashMap<>();

    @Override
    public void addListener(String key, CallbackListener listener) {
        listeners.put(key, listener);
        String env = System.getProperty("quota");
        // 消息回调
        listener.receiveServerMsg(env + "\t" + key + "\t" + new Date().toString());
        if(INSTANCE == null) {
            DataStore dataStore = ExtensionLoader.getExtensionLoader(DataStore.class).getDefaultExtension();
            Map<String, Object> executors = dataStore.get(Constants.EXECUTOR_SERVICE_COMPONENT_KEY);
            executors.forEach((str, executor) -> {
                ThreadPoolExecutor executorService = (ThreadPoolExecutor)executor;
                int size = executorService.getMaximumPoolSize();
                this._sendServerMsg("PoolSize:" + env + ":" + size);
            });
            INSTANCE = this;
        }
    }

    private void _sendServerMsg(String msg) {
        if (!listeners.isEmpty()) {
            for (Map.Entry<String, CallbackListener> entry : listeners.entrySet()) {
                try {
                    entry.getValue().receiveServerMsg(msg);
                } catch (Exception t1) {
                }
            }
        }
    }
}
