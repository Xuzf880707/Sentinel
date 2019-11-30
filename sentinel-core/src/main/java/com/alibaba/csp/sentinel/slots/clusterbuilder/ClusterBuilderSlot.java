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
package com.alibaba.csp.sentinel.slots.clusterbuilder;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.csp.sentinel.EntryType;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.node.ClusterNode;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.node.IntervalProperty;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.node.SampleCountProperty;
import com.alibaba.csp.sentinel.slotchain.AbstractLinkedProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlotChain;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slotchain.StringResourceWrapper;

/**
 * <p>
 * This slot maintains resource running statistics (response time, qps, thread
 * count, exception), and a list of callers as well which is marked by
 * {@link ContextUtil#enter(String origin)}
 * </p>
 * <p>
 * One resource has only one cluster node, while one resource can have multiple
 * default nodes.
 * </p>
 *
 * @author jialiang.linjl
 * 则用于存储资源的统计信息以及调用者信息，例如该资源的 RT, QPS, thread count 等等，
 * 这些信息将用作为多维度限流，降级的依据；
 *
 *  ClusterBuilderSlot 主要用于维护资源的埋点信息 。同一个资源ResourceWrapper(即使是不同的context下)共享一个 ClusterNode节点。
 *  ClusterNode记录了在所有的context的调用链路下针对这个ResourceWrapper的调用的统计信息。每个调用链路节点DefaultNode也持有对ClusterNode的引用。
 *  ClusterNode针对资源主要有这些统计信息：响应时间、QPS、block 数目、线程数、异常数等，以及调用来源统计信息列表，调用来源的名称由 ContextUtil.enter(contextName，origin) 中的 origin 标记。
 *  思考下一问题，一个资源可能会存在多个DefaultNode。所以怎么更快的获得同一个资源的统计信息呢？
 *      答案是所有具有相同的资源名称的调用链路节点DefaultNode共享一个ClusterNode，它们都有一个引用指向ClusterNode。
 *
 */
public class ClusterBuilderSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    /**
     * <p>
     * Remember that same resource({@link ResourceWrapper#equals(Object)}) will share
     * the same {@link ProcessorSlotChain} globally, no matter in witch context. So if
     * code goes into {@link #entry(Context, ResourceWrapper, DefaultNode, int, boolean, Object...)},
     * the resource name must be same but context name may not.
     * </p>
     * <p>
     * To get total statistics of the same resource in different context, same resource
     * shares the same {@link ClusterNode} globally. All {@link ClusterNode}s are cached
     * in this map.
     * </p>
     * <p>
     * The longer the application runs, the more stable this mapping will
     * become. so we don't concurrent map but a lock. as this lock only happens
     * at the very beginning while concurrent map will hold the lock all the time.
     * </p>
     */
    /***
     *
     * key：资源名称。只跟资源名称有关，即使是不同的context下同一资源名称，也是共享同一个value
     * value：资源对应的集群节点。
     * 注意：这个 clusterNodeMap 和 NodeSelectorSlot 中map不一样，它是一个类变量，所以即使是来自于不同的 ProcessorSlotChain，它也是共享这一个map
     */
    private static volatile Map<ResourceWrapper, ClusterNode> clusterNodeMap = new HashMap<>();

    private static final Object lock = new Object();
    //这个是对象变量。我们知道 每个ResourceWrapper对应一个ProcessorSlotChain，也就是对应一个ClusterBuilderSlot。
    //所以这里对象变量clusterNode本身也跟资源一一对应
    private volatile ClusterNode clusterNode = null;

    /***
     *
     * @param context         当前上下文context
     * @param resourceWrapper 当前context的资源resource
     * @param node    对应的是当前context下某个资源resource的调用链路节点
     * @param count           需要的token数
     * @param prioritized     是否紧急
     * @param args            调用者参数 parameters of the original call
     * @throws Throwable
     * 1、检查resourceWrapper对应的clusterNode是否已初始化，如果未初始化则先初始化创建一个ClusterNode并放到本地内存里
     * 2、将clusterNode绑定DefaultNode（某个context下的当前resourceWrapper所对应的调用链路节点.
     *      这样可以在任何一个调用链路下，通过DefaultNode找到资源对应的总的统计信息节点clusterNode
     * 3、如果当前调用指定了调用来源，那么就要为cluterNode绑定用于统计来源于orgin的调用信息的节点：StatisticNode。
     *      注意：
     *          不同资源的StatisticNode并不共享。确定一个StatisticNode需要: resourceWrapper+ orgin
     *          理论上讲，clusterNode统计信息应该由所有的StatisticNode累加起来的
     *
     */
    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node, int count,
                      boolean prioritized, Object... args)
        throws Throwable {
        //因为这边的clusterNode本身就是跟资源是一一对应的，都对应某个ProcessorSlotChain，
        // 所以不需要从map里先get再判断，而是直接判断
        if (clusterNode == null) {//如果clusterNode还未初始化
            synchronized (lock) {
                if (clusterNode == null) {
                    // Create the cluster node.
                    clusterNode = new ClusterNode();
                    HashMap<ResourceWrapper, ClusterNode> newMap = new HashMap<>(Math.max(clusterNodeMap.size(), 16));
                    newMap.putAll(clusterNodeMap);
                    newMap.put(node.getId(), clusterNode);

                    clusterNodeMap = newMap;
                }
            }
        }
        //将clusterNode绑定DefaultNode（某个context下的当前resourceWrapper所对应的调用链路节点。
        //这样可以在任何一个调用链路下，通过DefaultNode找到资源对应的总的统计信息节点clusterNode
        node.setClusterNode(clusterNode);

        /*
         * if context origin is set, we should get or create a new {@link Node} of
         * the specific origin.
         */
        //如果此次调用指定了调用源，则根据resourceWrapper+ orgin创建一个StatisticNode，并绑定到clusterNode中
        if (!"".equals(context.getOrigin())) {//根据orign维护对应的OriginNode
            Node originNode = node.getClusterNode().getOrCreateOriginNode(context.getOrigin());
            context.getCurEntry().setOriginNode(originNode);
        }
        //交给下一个规则进行检测
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }

    /**
     * Get {@link ClusterNode} of the resource of the specific type.
     *
     * @param id   resource name.
     * @param type invoke type.
     * @return the {@link ClusterNode}
     */
    public static ClusterNode getClusterNode(String id, EntryType type) {
        return clusterNodeMap.get(new StringResourceWrapper(id, type));
    }

    /**
     * Get {@link ClusterNode} of the resource name.
     *
     * @param id resource name.
     * @return the {@link ClusterNode}.
     */
    public static ClusterNode getClusterNode(String id) {
        if (id == null) {
            return null;
        }
        ClusterNode clusterNode = null;

        for (EntryType nodeType : EntryType.values()) {//从全局变量clusterNodeMap根据资源名称获取
            clusterNode = clusterNodeMap.get(new StringResourceWrapper(id, nodeType));
            if (clusterNode != null) {
                break;
            }
        }

        return clusterNode;
    }

    /**
     * Get {@link ClusterNode}s map, this map holds all {@link ClusterNode}s, it's key is resource name,
     * value is the related {@link ClusterNode}. <br/>
     * DO NOT MODIFY the map returned.
     *
     * @return all {@link ClusterNode}s
     */
    public static Map<ResourceWrapper, ClusterNode> getClusterNodeMap() {
        return clusterNodeMap;
    }

    /**
     * Reset all {@link ClusterNode}s. Reset is needed when {@link IntervalProperty#INTERVAL} or
     * {@link SampleCountProperty#SAMPLE_COUNT} is changed.
     */
    public static void resetClusterNodes() {
        for (ClusterNode node : clusterNodeMap.values()) {
            node.reset();
        }
    }
}
