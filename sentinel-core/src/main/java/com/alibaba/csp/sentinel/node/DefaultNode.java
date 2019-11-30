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

import java.util.HashSet;
import java.util.Set;

import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.SphO;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.nodeselector.NodeSelectorSlot;

/**
 * <p>
 *     用于对一个context下的一个资源进行统计，每个context中的每一个资源都会相应的维护一个DefaultNode
 * A {@link Node} used to hold statistics for specific resource name in the specific context.
 * Each distinct resource in each distinct {@link Context} will corresponding to a {@link DefaultNode}.
 * </p>
 * <p>
 *  如果在同一个context中多次调用entry，则该节点下会有一个子节点列表
 * This class may have a list of sub {@link DefaultNode}s. Child nodes will be created when
 * calling {@link SphU}#entry() or {@link SphO}@entry() multiple times in the same {@link Context}.
 * </p>
 *
 * @author qinan.qn
 * @see NodeSelectorSlot
 *
 * 该节点持有指定上下文中指定资源的统计信息，当在同一个上下文中多次调用entry方法时，该节点下可能会创建有一系列的子节点。
 * 另外每个DefaultNode中会关联一个ClusterNode
 */
public class DefaultNode extends StatisticNode {

    /**
     * The resource associated with the node.
     * 资源的id
     */
    private ResourceWrapper id;

    /**
     * The list of all child nodes.
     */
    private volatile Set<Node> childList = new HashSet<>();

    /**
     * Associated cluster node.
     * 每个DefaultNode中会关联一个ClusterNode
     */
    private ClusterNode clusterNode;

    public DefaultNode(ResourceWrapper id, ClusterNode clusterNode) {
        this.id = id;
        this.clusterNode = clusterNode;
    }

    public ResourceWrapper getId() {
        return id;
    }

    public ClusterNode getClusterNode() {
        return clusterNode;
    }

    /***
     * 绑定对相应资源的总的统计信息
     * @param clusterNode
     */
    public void setClusterNode(ClusterNode clusterNode) {
        this.clusterNode = clusterNode;
    }

    /**
     * Add child node to current node.
     *
     * @param node valid child node
     */
    public void addChild(Node node) {
        if (node == null) {
            RecordLog.warn("Trying to add null child to node <{0}>, ignored", id.getName());
            return;
        }
        if (!childList.contains(node)) {
            synchronized (this) {
                if (!childList.contains(node)) {
                    Set<Node> newSet = new HashSet<>(childList.size() + 1);
                    newSet.addAll(childList);
                    newSet.add(node);
                    childList = newSet;
                }
            }
            RecordLog.info("Add child <{0}> to node <{1}>", ((DefaultNode)node).id.getName(), id.getName());
        }
    }

    /**
     * Reset the child node list.
     */
    public void removeChildList() {
        this.childList = new HashSet<>();
    }

    public Set<Node> getChildList() {
        return childList;
    }

    /***
     *
     * @param count
     * 1、添加当前DefaultNode节点Block Qps统计信息
     * 2、增加集群节点的Block Qps统计信息
     */
    @Override
    public void increaseBlockQps(int count) {
        super.increaseBlockQps(count);
        this.clusterNode.increaseBlockQps(count);
    }

    /***
     *
     * @param count
     * 1、添加当前DefaultNode节点 Exception Qps统计信息
     * 2、增加集群节点的 Exception Qps统计信息
     */
    @Override
    public void increaseExceptionQps(int count) {
        super.increaseExceptionQps(count);
        this.clusterNode.increaseExceptionQps(count);
    }

    /***
     *
     * @param rt
     * @param successCount
     * 1、添加当前DefaultNode节点'响应时间rt和成功调用数successCount'的统计信息
     * 2、增加集群节点的'响应时间rt和成功调用数successCount'统计信息
     */
    @Override
    public void addRtAndSuccess(long rt, int successCount) {
        super.addRtAndSuccess(rt, successCount);
        this.clusterNode.addRtAndSuccess(rt, successCount);
    }

    /***
     *
     * 1、增加当前DefaultNode节点'调用线程'的统计信息
     * 2、增加集群节点的'调用线程'的统计信息
     */
    @Override
    public void increaseThreadNum() {
        super.increaseThreadNum();
        this.clusterNode.increaseThreadNum();
    }
    /***
     *
     * 1、减少当前DefaultNode节点'调用线程'的统计信息
     * 2、减少集群节点的'调用线程'的统计信息
     */
    @Override
    public void decreaseThreadNum() {
        super.decreaseThreadNum();
        this.clusterNode.decreaseThreadNum();
    }

    /***
     * DefaultNode：保存着某个resource在某个context中的实时指标，每个DefaultNode都指向一个ClusterNode
     * ClusterNode：保存着某个resource在所有的context中实时指标的总和，同样的resource会共享同一个ClusterNode，不管他在哪个context中
     * 1、增加当前DefaultNode节点'请求通过'的统计信息
     * 2、增加集群节点的'请求通过'的统计信息
     */
    @Override
    public void addPassRequest(int count) {
        super.addPassRequest(count);
        this.clusterNode.addPassRequest(count);
    }

    public void printDefaultNode() {
        visitTree(0, this);
    }

    /**
     * 遍历调用链路🌲
     * @param level
     * @param node
     */
    private void visitTree(int level, DefaultNode node) {
        for (int i = 0; i < level; ++i) {
            System.out.print("-");
        }
        if (!(node instanceof EntranceNode)) {
            System.out.println(
                String.format("%s(thread:%s pq:%s bq:%s tq:%s rt:%s 1mp:%s 1mb:%s 1mt:%s)", node.id.getShowName(),
                    node.curThreadNum(), node.passQps(), node.blockQps(), node.totalQps(), node.avgRt(),
                    node.totalRequest() - node.blockRequest(), node.blockRequest(), node.totalRequest()));
        } else {
            System.out.println(
                String.format("Entry-%s(t:%s pq:%s bq:%s tq:%s rt:%s 1mp:%s 1mb:%s 1mt:%s)", node.id.getShowName(),
                    node.curThreadNum(), node.passQps(), node.blockQps(), node.totalQps(), node.avgRt(),
                    node.totalRequest() - node.blockRequest(), node.blockRequest(), node.totalRequest()));
        }
        for (Node n : node.getChildList()) {
            DefaultNode dn = (DefaultNode)n;
            visitTree(level + 1, dn);
        }
    }

}
