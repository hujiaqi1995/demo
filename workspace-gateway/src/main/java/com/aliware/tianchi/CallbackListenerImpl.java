package com.aliware.tianchi;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.dubbo.rpc.listener.CallbackListener;

/**
 * @author daofeng.xjf
 *
 * 客户端监听器
 * 可选接口
 * 用户可以基于获取获取服务端的推送信息，与 CallbackService 搭配使用
 *
 */
public class CallbackListenerImpl implements CallbackListener {

    public static ConcurrentMap<String, Integer> sizeMap = new ConcurrentHashMap<>();

    @Override
    public void receiveServerMsg(String msg) {
        // 返回最大窗口
        if(msg.startsWith("PoolSize")) {
            String[] strs = msg.split(":");
            String name = strs[1];
            int maxPoolSize = Integer.valueOf(strs[2]);
            sizeMap.put(name, maxPoolSize);
        }
    }

}
