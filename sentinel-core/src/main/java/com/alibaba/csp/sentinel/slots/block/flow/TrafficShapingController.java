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
package com.alibaba.csp.sentinel.slots.block.flow;

import com.alibaba.csp.sentinel.node.Node;

/**
 * A universal interface for traffic shaping controller.
 *
 * @author jialiang.linjl
 */
public interface TrafficShapingController {

    /***
     * 当QPS超过任意规则的阈值后，新的请求就会被立即拒绝，拒绝方式为抛出FlowException
     * @param node resource node，也就是 clusterNode
     * @param acquireCount count to acquire
     * @param prioritized whether the request is prioritized 是否优先处理
     * @return
     */
    boolean canPass(Node node, int acquireCount, boolean prioritized);

    /***
     * 当QPS超过任意规则的阈值后，新的请求就会被立即拒绝，拒绝方式为抛出FlowException
     * @param node resource node
     * @param acquireCount count to acquire
     * @return
     */
    boolean canPass(Node node, int acquireCount);
}
