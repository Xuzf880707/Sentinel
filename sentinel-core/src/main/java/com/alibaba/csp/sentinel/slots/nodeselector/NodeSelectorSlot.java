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
package com.alibaba.csp.sentinel.slots.nodeselector;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.node.ClusterNode;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.node.EntranceNode;
import com.alibaba.csp.sentinel.slotchain.AbstractLinkedProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;

/**
 * </p>
 * This class will try to build the calling traces via
 * <ol>
 * <li>adding a new {@link DefaultNode} if needed as the last child in the context.
 * The context's last node is the current node or the parent node of the context. </li>
 * <li>setting itself to the context current node.</li>
 * </ol>
 * </p>
 *
 * <p>It works as follow:</p>
 * <pre>
 * ContextUtil.enter("entrance1", "appA");
 * Entry nodeA = SphU.entry("nodeA");
 * if (nodeA != null) {
 *     nodeA.exit();
 * }
 * ContextUtil.exit();
 * </pre>
 *
 * Above code will generate the following invocation structure in memory:
 *
 * <pre>
 *
 *              machine-root
 *                  /
 *                 /
 *           EntranceNode1
 *               /
 *              /
 *        DefaultNode(nodeA)- - - - - -> ClusterNode(nodeA);
 * </pre>
 *
 * <p>
 * Here the {@link EntranceNode} represents "entrance1" given by
 * {@code ContextUtil.enter("entrance1", "appA")}.
 * </p>
 * <p>
 * Both DefaultNode(nodeA) and ClusterNode(nodeA) holds statistics of "nodeA", which is given
 * by {@code SphU.entry("nodeA")}
 * </p>
 * <p>
 * The {@link ClusterNode} is uniquely identified by the ResourceId; the {@link DefaultNode}
 * is identified by both the resource id and {@link Context}. In other words, one resource
 * id will generate multiple {@link DefaultNode} for each distinct context, but only one
 * {@link ClusterNode}.
 * </p>
 * <p>
 * the following code shows one resource id in two different context:
 * </p>
 *
 * <pre>
 *    ContextUtil.enter("entrance1", "appA");
 *    Entry nodeA = SphU.entry("nodeA");
 *    if (nodeA != null) {
 *        nodeA.exit();
 *    }
 *    ContextUtil.exit();
 *
 *    ContextUtil.enter("entrance2", "appA");
 *    nodeA = SphU.entry("nodeA");
 *    if (nodeA != null) {
 *        nodeA.exit();
 *    }
 *    ContextUtil.exit();
 * </pre>
 *
 * Above code will generate the following invocation structure in memory:
 *
 * <pre>
 *
 *                  machine-root
 *                  /         \
 *                 /           \
 *         EntranceNode1   EntranceNode2
 *               /               \
 *              /                 \
 *      DefaultNode(nodeA)   DefaultNode(nodeA)
 *             |                    |
 *             +- - - - - - - - - - +- - - - - - -> ClusterNode(nodeA);
 * </pre>
 *
 * <p>
 * As we can see, two {@link DefaultNode} are created for "nodeA" in two context, but only one
 * {@link ClusterNode} is created.
 * </p>
 *
 * <p>
 * We can also check this structure by calling: <br/>
 * {@code curl http://localhost:8719/tree?type=root}
 * </p>
 *
 * @author jialiang.linjl
 * @see EntranceNode
 * @see ContextUtil
 * 负责收集资源的路径，并将这些资源的调用路径，以树状结构存储起来，用于根据调用路径来限流降级
 */
public class NodeSelectorSlot extends AbstractLinkedProcessorSlot<Object> {

    /**
     * {@link DefaultNode}s of the same resource in different context.
     * key：context上下文名称。
     *      由于不同context中同名的资源名称会共享相同的规则链，所以在不同的context里可以尝试获得相同的资源。
     *      因此这里使用context作为key，这样就可以在不同的上下文里区分同一个资源。
     *      既然这样，同一个资源名称可能会为每一个context创建多个DefaultNode
     *
     */
    private volatile Map<String, DefaultNode> /**/map = new HashMap<String, DefaultNode>(10);
    //负责收集资源的路径，并将这些资源的调用路径，以树状结构存储起来，用于根据调用路径来限流降级；
    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, Object obj, int count, boolean prioritized, Object... args)
        throws Throwable {
        /* 有趣的是，我们采用context的名称而不是资源的名称来作为map的key
         * It's interesting that we use context name rather resource name as the map key.
         * 不同的上下文里相同名称的资源会共享相同的过滤链。所以如果代码进入entry方法里，那么资源名称肯定相同，
         * 但是context不一定相同
         * Remember that same resource({@link ResourceWrapper#equals(Object)}) will share
         * the same {@link ProcessorSlotChain} globally, no matter in which context. So if
         * code goes into {@link #entry(Context, ResourceWrapper, DefaultNode, int, Object...)},
         * the resource name must be same but context name may not.
         *  如果我们在不同的context使用entry(String resource)尝试获取相同的资源名称，如果使用context作为key的话可以
         *  区分出同名的资源名称。因此，可能为不同的context基于相同名称的资源建立多个DefaultNode
         * If we use {@link com.alibaba.csp.sentinel.SphU#entry(String resource)} to
         * enter same resource in different context, using context name as map key can
         * distinguish the same resource. In this case, multiple {@link DefaultNode}s will be created
         * of the same resource name, for every distinct context (different context name) each.
         *  另一问题，一个资源可能会存在多个DefaultNode。所以怎么更快的获得同一个资源的统计信息呢？
         *  答案是所有具有相同的资源名称的DefaultNode共享一个ClusterNode
         * Consider another question. One resource may have multiple {@link DefaultNode},
         * so what is the fastest way to get total statistics of the same resource?
         * The answer is all {@link DefaultNode}s with same resource name share one
         * {@link ClusterNode}. See {@link ClusterBuilderSlot} for detail.
         * 相同的资源共享同一个规则链，无论是在哪个context里
         */
        //根据context名称获得对应的DefaultNode
        // 根据「上下文」的名称获取DefaultNode
        // 多线程环境下，每个线程都会创建一个context，
        // 只要资源名相同(意味着同一个NodeSelectorSlot对象，因为map是对象变量)，且context的名称也相同，那么获取到的节点就相同
        DefaultNode node = map.get(context.getName());
        if (node == null) {//还未初始化context对应的DefaultNode
            synchronized (this) {
                node = map.get(context.getName());
                if (node == null) {// 如果当前资源在当前「上下文」中没有该节点
                    //根据资源名称为context创建一个DefaultNode
                    node = new DefaultNode(resourceWrapper, null);
                    HashMap<String, DefaultNode> cacheMap = new HashMap<String, DefaultNode>(map.size());
                    cacheMap.putAll(map);
                    cacheMap.put(context.getName(), node);
                    map = cacheMap;
                }
                // Build invocation tree 将新节点加入到parent节点的childList里
                ((DefaultNode)context.getLastNode()).addChild(node);
            }
        }
        // 将当前node作为「上下文」的最后一个节点的子节点添加进去
        // 如果context的curEntry.parent.curNode为null，则添加到entranceNode中去
        // 否则添加到context的curEntry.parent.curNode中去
        context.setCurNode(node);//将curEntry绑定defaultNode
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }
}
