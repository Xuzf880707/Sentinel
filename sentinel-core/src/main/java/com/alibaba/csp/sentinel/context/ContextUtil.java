/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.context;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.EntryType;
import com.alibaba.csp.sentinel.SphO;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.node.EntranceNode;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slotchain.StringResourceWrapper;
import com.alibaba.csp.sentinel.slots.nodeselector.NodeSelectorSlot;

/**
 * Utility class to get or create {@link Context} in current thread.
 *
 * <p>
 * Each {@link SphU}#entry() or {@link SphO}#entry() should be in a {@link Context}.
 * If we don't invoke {@link ContextUtil}#enter() explicitly, DEFAULT context will be used.
 * </p>
 *
 * @author jialiang.linjl
 * @author leyou(lihao)
 * @author Eric Zhao
 */
public class ContextUtil {

    /**
     * Store the context in ThreadLocal for easy access.
     * 线程对应的ThreadLocal，可以获取当前线程中的ContextUtil，保证针对线程单例
     */
    private static ThreadLocal<Context> contextHolder = new ThreadLocal<>();

    /**
     * Holds all {@link EntranceNode}. Each {@link EntranceNode} is associated with a distinct context name.
     * 全局变量，持有所有的入口，每个入口节点都绑定一个唯一的上下文名称
     * key: context的名称
     * value：context对应的 DefaultNode
     */
    private static volatile Map<String, DefaultNode> contextNameNodeMap = new HashMap<>();

    private static final ReentrantLock LOCK = new ReentrantLock();
    //一个空的上下文
    private static final Context NULL_CONTEXT = new NullContext();

    static {
        // Cache the entrance node for default context.
        initDefaultContext();
    }

    /***
     * 初始化一个默认的上下文入口节点：sentinel_default_context。
     * 将默认的上下文入口节点sentinel_default_context存入全局变量contextNameNodeMap
     */
    private static void initDefaultContext() {
        String defaultContextName = Constants.CONTEXT_DEFAULT_NAME;
        EntranceNode node = new EntranceNode(new StringResourceWrapper(defaultContextName, EntryType.IN), null);
        Constants.ROOT.addChild(node);
        contextNameNodeMap.put(defaultContextName, node);
    }

    /**
     * Not thread-safe, only for test.
     */
    static void resetContextMap() {
        if (contextNameNodeMap != null) {
            RecordLog.warn("Context map cleared and reset to initial state");
            contextNameNodeMap.clear();
            initDefaultContext();
        }
    }

    /**
     * <p>
     *
     *
     * @param name   the context name
     * @param origin the origin of this invocation, usually the origin could be the Service
     *               Consumer's app name. The origin is useful when we want to control different
     *               invoker/consumer separately.
     * @return The invocation context of the current thread
     *
     *  1、进入调用上下文，它将作为被标记为某次调用链的入口，context被包装为一个ThreadLocal，这样可以保证每个线程都有它自己的context，实现线程隔离。
     *    如果当前线程里没有，则会新建一个。
     *  2、每个Context会绑定一个入口EntranceNode节点，作为调用链的统计节点，主要用来统计当前调用链的统计信息，所以如果相同相同的上下文，会共享这个统计节点。
     *    如果context不包含这样的入口节点，则会新建一个。
     *    处于不同的context中的同一个资源，会被分离成两个调用链：NodeSelectorSlot
     *  3、ClusterBuilderSlot 会帮我们创建一个集群节点，该节点主要是用于统计资源的统计信息，所以如果不同的上下文获取同样的资源的话，会共享这个统计点。
     *  4、ClusterBuilderSlot 会根据资源resourceName和调用来源orign创建一个statisticsNode，所以对于不同的调用来源orgin调用不同的资源的话，最多会创建amount * distinct个统计节点
     *      因此，当来源太多时，应仔细考虑内存占用。
     */
    public static Context enter(String name, String origin) {
        //上下文名称校验
        if (Constants.CONTEXT_DEFAULT_NAME.equals(name)) {
            throw new ContextNameDefineException(
                "The " + Constants.CONTEXT_DEFAULT_NAME + " can't be permit to defined!");
        }
        //name是上下文
        //origin是调用者
        return trueEnter(name, origin);
    }

    /***
     * 获取/创建一个上下文，并存放到ThreadLocal
     * @param name 上下文名称
     * @param origin 调用发起者
     * @return
     * 1、从ThreadLocal中获取Context(所以Context和Thread绑定)
     * 2、如果ThreadLocal里没有，则新建一个Context
     *      根据context名称创建一个 EntranceNode 节点，它也是一个 DefaultNode 节点，代表一个调用链。后面根据根据调用链进行限流
     *      将 EntranceNode 节点 添加到根节点，组成调用树。
     *
     */
    protected static Context trueEnter(String name, String origin) {
        //检查ThreadLocal里是否存在Context对象
        Context context = contextHolder.get();
        if (context == null) {
            //根据name从map中获取根节点，只要是相同的资源名，就能直接从map中获取到node
            //检查本地缓存，是否存在name对应的DefaultNode。
            // key：上下文名称，
            // value：一个入口节点EntranceNode
            Map<String, DefaultNode> localCacheNameMap = contextNameNodeMap;
            //判断当前上下文是否已创建好调用链节点：DefaultNode
            DefaultNode node = localCacheNameMap.get(name);
            if (node == null) {//第一次创建上下文name
                //缓存里不能有超过2000个调用链
                if (localCacheNameMap.size() > Constants.MAX_CONTEXT_NAME_SIZE) {
                    setNullContext();
                    return NULL_CONTEXT;
                } else {
                    try {
                        //双重检查
                        LOCK.lock();
                        node = contextNameNodeMap.get(name);//检查本地内存
                        if (node == null) {
                            if (contextNameNodeMap.size() > Constants.MAX_CONTEXT_NAME_SIZE) {
                                setNullContext();
                                return NULL_CONTEXT;
                            } else {
                                //根据上下文名称初始化一个入口节点EntranceNode，入口节点通过名称绑定Context
                                node = new EntranceNode(new StringResourceWrapper(name, EntryType.IN), null);//创建一个新的入口节点
                                // Add entrance node. 把EntranceNode设置为Root节点的子节点
                                Constants.ROOT.addChild(node);
                                //更新本地缓存
                                Map<String, DefaultNode> newMap = new HashMap<>(contextNameNodeMap.size() + 1);
                                newMap.putAll(contextNameNodeMap);
                                newMap.put(name, node);
                                contextNameNodeMap = newMap;
                            }
                        }
                    } finally {
                        LOCK.unlock();
                    }
                }
            }
            //创建一个新的Context，并设置Context的根节点，即设置EntranceNode
            context = new Context(node, name);
            context.setOrigin(origin);
            contextHolder.set(context);
        }

        return context;
    }

    private static boolean shouldWarn = true;

    /***
     * 设置当前线程的Context为空的上下文：NULL_CONTEXT
     */
    private static void setNullContext() {
        contextHolder.set(NULL_CONTEXT);
        // Don't need to be thread-safe.
        if (shouldWarn) {
            RecordLog.warn("[SentinelStatusChecker] WARN: Amount of context exceeds the threshold "
                + Constants.MAX_CONTEXT_NAME_SIZE + ". Entries in new contexts will NOT take effect!");
            shouldWarn = false;
        }
    }

    /**
     * <p>
     * Enter the invocation context, which marks as the entrance of an invocation chain.
     * The context is wrapped with {@code ThreadLocal}, meaning that each thread has it's own {@link Context}.
     * New context will be created if current thread doesn't have one.
     * </p>
     * <p>
     * A context will be bound with an {@link EntranceNode}, which represents the entrance statistic node
     * of the invocation chain. New {@link EntranceNode} will be created if
     * current context does't have one. Note that same context name will share
     * same {@link EntranceNode} globally.
     * </p>
     * <p>
     * Same resource in different context will count separately, see {@link NodeSelectorSlot}.
     * </p>
     *
     * @param name the context name
     * @return The invocation context of the current thread
     */
    public static Context enter(String name) {
        return enter(name, "");
    }

    /**
     * 移除当前线程ThreadLocal中绑定的Context，协助对象垃圾回收
     */
    public static void exit() {
        Context context = contextHolder.get();
        if (context != null && context.getCurEntry() == null) {
            contextHolder.set(null);
        }
    }

    /**
     * Get current size of context entrance node map.
     *
     * @return current size of context entrance node map
     * @since 0.2.0
     * 获得当前内存里调用链数量
     */
    public static int contextSize() {
        return contextNameNodeMap.size();
    }

    /**
     * 检查当前的context是否是默认的Context
     * @param context context to check
     * @return true if it is a default context, otherwise false
     * @since 0.2.
     *
     */
    public static boolean isDefaultContext(Context context) {
        if (context == null) {
            return false;
        }
        return Constants.CONTEXT_DEFAULT_NAME.equals(context.getName());
    }

    /**
     * Get {@link Context} of current thread.
     *
     * @return context of current thread. Null value will be return if current
     * thread does't have context.
     */
    public static Context getContext() {
        return contextHolder.get();
    }

    /**
     * <p>
     * Replace current context with the provided context.
     * This is mainly designed for context switching (e.g. in asynchronous invocation).
     * </p>
     * <p>
     * Note: When switching context manually, remember to restore the original context.
     * For common scenarios, you can use {@link #runOnContext(Context, Runnable)}.
     * </p>
     *
     * @param newContext new context to set
     * @return old context
     * @since 0.2.0
     *
     * 使用新的context来替换旧的context(该设计主要是为了在异步调用时希望目标线程和某个之前的资源跑在同一个context里)
     *      如果新的context不为空，则直接替换
     *      如果新的context为空，则直接移除旧的context就可以了
     */
    static Context replaceContext(Context newContext) {
        //获得旧的context
        Context backupContext = contextHolder.get();
        if (newContext == null) {
            //移除旧的context
            contextHolder.remove();
        } else {
            contextHolder.set(newContext);
        }
        return backupContext;
    }

    /**
     * Execute the code within provided context.
     * This is mainly designed for context switching (e.g. in asynchronous invocation).
     *
     * @param context the context
     * @param f       lambda to run within the context
     * @since 0.2.0
     * 让某个线程跑在指定的context上(可以让多个资源共享一个context)
     *      1、会用传入的context替换掉当前线程里现有的旧的context
     *      2、开始执行任务线程
     *      3、当线程在指定的目标context里执行完成后，要记得把当前线程绑定的context替换回来
     */
    public static void runOnContext(Context context, Runnable f) {
        Context curContext = replaceContext(context);
        try {
            f.run();
        } finally {
            replaceContext(curContext);
        }
    }
}
