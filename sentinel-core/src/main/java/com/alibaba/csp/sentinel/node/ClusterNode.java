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
package com.alibaba.csp.sentinel.node;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.slots.block.BlockException;

/**
 * <p>
 *     这个类保存额资源的统计信息，相同的名称共享同一个ClusterNode
 * This class stores summary runtime statistics of the resource, including rt, thread count, qps
 * and so on. Same resource shares the same {@link ClusterNode} globally, no matter in which
 * {@link com.alibaba.csp.sentinel.context.Context}.
 * </p>
 * <p>
 * To distinguish invocation from different origin (declared in
 * {@link ContextUtil#enter(String name, String origin)}),
 * one {@link ClusterNode} holds an {@link #originCountMap}, this map holds {@link StatisticNode}
 * of different origin. Use {@link #getOrCreateOriginNode(String)} to get {@link Node} of the specific
 * origin.<br/>
 * Note that 'origin' usually is Service Consumer's app name.
 * </p>
 *
 * @author qinan.qn
 * @author jialiang.linjl
 * 该节点中保存了资源的总体的运行时统计信息，包括rt，线程数，qps等等，相同的资源会全局共享同一个ClusterNode，不管他属于哪个上下文
 */
public class ClusterNode extends StatisticNode {

    /**
     * <p>The origin map holds the pair: (origin, originNode) for one specific resource.</p>
     * <p>
     * The longer the application runs, the more stable this mapping will become.
     * So we didn't use concurrent map here, but a lock, as this lock only happens
     * at the very beginning while concurrent map will hold the lock all the time.
     * </p>
     * 当前资源节点下来源于各个调用者的统计信息，所以通过资源名称+orgin确定唯一的StatisticNode
     *      key：调用者orign名称
     *      value：调用者orign对应的StatisticNode
     */
    private Map<String, StatisticNode> originCountMap = new HashMap<String, StatisticNode>();

    private final ReentrantLock lock = new ReentrantLock();

    /**
     *
     * <p>Get {@link Node} of the specific origin. Usually the origin is the Service Consumer's app name.</p>
     * <p>If the origin node for given origin is absent, then a new {@link StatisticNode}
     * for the origin will be created and returned.</p>
     *
     * @param origin The caller's name, which is designated in the {@code parameter} parameter
     *               {@link ContextUtil#enter(String name, String origin)}.
     * @return the {@link Node} of the specific origin
     * 则用于存储资源的统计信息以及调用者信息，例如该资源的 RT, QPS, thread count 等等，这些信息将用作为多维度限流，降级的依据；
     */
    public Node getOrCreateOriginNode(String origin) {
        //如果clusterNode中未包含orign对应的statisticNode，则新建一个，并绑定到 clusterNode里
        StatisticNode statisticNode = originCountMap.get(origin);
        if (statisticNode == null) {
            try {
                lock.lock();
                statisticNode = originCountMap.get(origin);
                if (statisticNode == null) {//如果clusterNode的originCountMap不包含orgin，则新建一个
                    // The node is absent, create a new node for the origin.
                    statisticNode = new StatisticNode();
                    HashMap<String, StatisticNode> newMap = new HashMap<>(originCountMap.size() + 1);
                    newMap.putAll(originCountMap);
                    newMap.put(origin, statisticNode);
                    originCountMap = newMap;
                }
            } finally {
                lock.unlock();
            }
        }
        return statisticNode;
    }

    public synchronized Map<String, StatisticNode> getOriginCountMap() {
        return originCountMap;
    }

    /**
     * Add exception count only when given {@code throwable} is not a {@link BlockException}.
     *
     * @param throwable target exception
     * @param count     count to add
     */
    public void trace(Throwable throwable, int count) {
        if (count <= 0) {
            return;
        }
        if (!BlockException.isBlockException(throwable)) {
            this.increaseExceptionQps(count);
        }
    }
}
