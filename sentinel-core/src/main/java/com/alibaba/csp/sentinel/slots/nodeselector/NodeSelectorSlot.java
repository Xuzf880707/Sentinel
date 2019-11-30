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
 *      NodeSelectorSlot 负责收集资源的路径，并将这些资源的调用路径，以树状结构存储起来，用于根据调用路径来限流降级。
 *      每个context下的每个resourceWrapper都有一个独立的 DefaultNode。而这个DefaultNode下又都持有针对所有context下的同一个resourceWrapper的总的统计信息
 */
public class NodeSelectorSlot extends AbstractLinkedProcessorSlot<Object> {
    /***
     *{@link DefaultNode}s of the same resource in different context.
     *
     * key：context.name 也就是上下文名称
     * value：是在当前context下，请求的资源对象所代表的DefaultNode，比如 nodeA
     *      注意：我们知道每个资源resourceWrapper都会创建自己的ProcessorSlotChain，每个ProcessorSlotChain 中的NodeSelectorSlot都是独立并不共享
     *          所以不同的 resourceWrapper 是不同的 map；
     *          不同context中相同的resourceWrapper会共享这个map，但是他们的key是不一样的。也就是说，对于同一个资源，会为每一个context都创建一个DefaultNode
     * 这边为什么要用context作为key？
     *      a、这样就可以在不同的上下文里区分同名的资源。
     *      b、而且因为即使是不同的context，只要资源名称相同，那么就会共享同一条规则链 ProcessorSlotChain。但是在某些场景下，我们想统计某个资源在某条调用链路下的统计信息。
     *          通过context作为key，我们就可以在不同的context中获得这一个共享的资源的当前调用链的调用情况。
     *
     */
    private volatile Map<String, DefaultNode> /**/map = new HashMap<String, DefaultNode>(10);
    /***
     * 负责收集资源的路径，并将这些资源的调用路径，以树状结构存储起来，用于根据调用路径来限流降级；
     *      注意：每个DefaultNode都持有一个ClusterNode，这个ClusterNode节点是用来统计资源 resource总的统计信息(不区分调用链路)。所以通过DefaultNode可以很快的拿到对应的资源的总的统计信息。
     * @param context         current {@link Context}
     * @param resourceWrapper current resource
     * @param obj
     * @param count           tokens needed
     * @param prioritized     whether the entry is prioritized
     * @param args            parameters of the original call
     * @throws Throwable
     * 1、先检查下当前资源调用链维护的NodeSelectorSlot下是否存在对应context的调用链节点 DefaultNode 。
     * 2、如果没有的话，则根据资源对象 resourceWrapper 新建一个，并根据context-DefaultNode方式放到 NodeSelectorSlot下的对象变量map中。
     */
    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, Object obj, int count, boolean prioritized, Object... args)
        throws Throwable {
        /*
         * It's interesting that we use context name rather resource name as the map key.
         * Remember that same resource({@link ResourceWrapper#equals(Object)}) will share
         * the same {@link ProcessorSlotChain} globally, no matter in which context. So if
         * code goes into {@link #entry(Context, ResourceWrapper, DefaultNode, int, Object...)},
         * the resource name must be same but context name may not.
         *
         * If we use {@link com.alibaba.csp.sentinel.SphU#entry(String resource)} to
         * enter same resource in different context, using context name as map key can
         * distinguish the same resource. In this case, multiple {@link DefaultNode}s will be created
         * of the same resource name, for every distinct context (different context name) each.
         * Consider another question. One resource may have multiple {@link DefaultNode},
         * so what is the fastest way to get total statistics of the same resource?
         * The answer is all {@link DefaultNode}s with same resource name share one
         * {@link ClusterNode}. See {@link ClusterBuilderSlot} for detail.
         */
        //根据context名称获得对应的DefaultNode
        DefaultNode node = map.get(context.getName());
        if (node == null) {//还未初始化context对应的DefaultNode
            synchronized (this) {
                node = map.get(context.getName());
                if (node == null) {// 如果当前资源在当前「上下文」中没有该节点
                    //根据资源名称为context创建一个 DefaultNode，注意，新建的DefaultNode暂时还没有绑定资源对应的clusterNode上
                    node = new DefaultNode(resourceWrapper, null);
                    HashMap<String, DefaultNode> cacheMap = new HashMap<String, DefaultNode>(map.size());
                    cacheMap.putAll(map);
                    cacheMap.put(context.getName(), node);
                    map = cacheMap;
                }
                /**
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
                 * */
                // Build invocation tree 将新节点加入到parent节点的childList里，查看上面的调用链，这个时候是把DefaultNode(nodeA)添加到🌲里
                ((DefaultNode)context.getLastNode()).addChild(node);
            }
        }
        // 将当前node作为「上下文」的最后一个节点的子节点添加进去
        // 如果context的curEntry.parent.curNode为null，则添加到entranceNode中去
        // 否则添加到context的curEntry.parent.curNode中去
        context.setCurNode(node);//将curEntry绑定defaultNode
        //这边会触发 ClusterBuilderSlot 执行
        fireEntry(context, resourceWrapper, node, count, prioritized, args);
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        fireExit(context, resourceWrapper, count, args);
    }
}
