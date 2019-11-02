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

import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.nodeselector.NodeSelectorSlot;

/**
 * <p>
 *     一个代表调用树的入口的节点
 * A {@link Node} represents the entrance of the invocation tree.
 * </p>
 * <p>
 *    一个context会关联一个 EntranceNode，它代表每个调用树的入口。如果当前context不存在一个EntranceNode
 *    来代表该context的入口的话，则会创建一个新的。相同名字的context会共享一个EntranceNode
 * One {@link Context} will related to a {@link EntranceNode},
 * which represents the entrance of the invocation tree. New {@link EntranceNode} will be created if
 * current context does't have one. Note that same context name will share same {@link EntranceNode}
 * globally.
 * </p>
 *
 * @author qinan.qn
 * @see ContextUtil
 * @see ContextUtil#enter(String, String)
 * @see NodeSelectorSlot
 *
 * 该节点表示一棵调用链树的入口节点，通过他可以获取调用链树中所有的子节点
 */
public class EntranceNode extends DefaultNode {

    public EntranceNode(ResourceWrapper id, ClusterNode clusterNode) {
        super(id, clusterNode);
    }

    @Override
    public double avgRt() {
        double total = 0;
        double totalQps = 0;
        for (Node node : getChildList()) {
            total += node.avgRt() * node.passQps();
            totalQps += node.passQps();
        }
        return total / (totalQps == 0 ? 1 : totalQps);
    }

    @Override
    public double blockQps() {
        double blockQps = 0;
        for (Node node : getChildList()) {
            blockQps += node.blockQps();
        }
        return blockQps;
    }

    @Override
    public long blockRequest() {
        long r = 0;
        for (Node node : getChildList()) {
            r += node.blockRequest();
        }
        return r;
    }

    @Override
    public int curThreadNum() {
        int r = 0;
        for (Node node : getChildList()) {
            r += node.curThreadNum();
        }
        return r;
    }

    @Override
    public double totalQps() {
        double r = 0;
        for (Node node : getChildList()) {
            r += node.totalQps();
        }
        return r;
    }

    @Override
    public double successQps() {
        double r = 0;
        for (Node node : getChildList()) {
            r += node.successQps();
        }
        return r;
    }

    @Override
    public double passQps() {
        double r = 0;
        for (Node node : getChildList()) {
            r += node.passQps();
        }
        return r;
    }

    @Override
    public long totalRequest() {
        long r = 0;
        for (Node node : getChildList()) {
            r += node.totalRequest();
        }
        return r;
    }

    @Override
    public long totalPass() {
        long r = 0;
        for (Node node : getChildList()) {
            r += node.totalPass();
        }
        return r;
    }
}
