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
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.node.OccupyTimeoutProperty;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.PriorityWaitException;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;
import com.alibaba.csp.sentinel.util.TimeUtil;

/**
 * Default throttling controller (immediately reject strategy).
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 * 直接拒绝
 */
public class DefaultController implements TrafficShapingController {

    private static final int DEFAULT_AVG_USED_TOKENS = 0;
    /***
     * 限流阈值
     */
    private double count;
    /**
     * 限流类型 1-QPS 0-Thread
     */
    private int grade;

    public DefaultController(double count, int grade) {
        this.count = count;
        this.grade = grade;
    }
    /***
     * 当QPS超过任意规则的阈值后，新的请求就会被立即拒绝，拒绝方式为抛出FlowException
     * @param node resource node 也就是 clusterNode
     * @param acquireCount count to acquire
     * @returnargLine
     */
    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    /***
     * 当QPS超过任意规则的阈值后，新的请求就会被立即拒绝，拒绝方式为抛出FlowException
     * @param node resource node 也就是 clusterNode
     * @param acquireCount count to acquire
     * @param prioritized whether the request is prioritized 是否优先处理
     * @return
     * 1、获得已被获取的token数
     *    a、如果限流方式是Thread线程类型的，则从clusterNode中获得当前持有资源的线程数
     *    b、如果如果限流方式是QPS类型的，从clusterNode中获得当前持采样时间内的通过的qps
     * 2、如果是QPS流量限制
     */
    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        int curCount = avgUsedTokens(node);//返回当前滑动窗口的线程数或者QPS
        if (curCount + acquireCount > count) {//如果超过流量限制或者超过并发线程数
            if (prioritized && grade == RuleConstant.FLOW_GRADE_QPS) {
                long currentTime;
                long waitInMs;
                //获得当前时间
                currentTime = TimeUtil.currentTimeMillis();
                //获得收满acquireCount个token需要等待的时间
                waitInMs = node.tryOccupyNext(currentTime, acquireCount, count);
                //如果需要等待的时间小于设置的超时时间
                if (waitInMs < OccupyTimeoutProperty.getOccupyTimeout()) {
                    //设置等待重新发起请求占用
                    node.addWaitingRequest(currentTime + waitInMs, acquireCount);
                    //先登记占用的pass个数
                    node.addOccupiedPass(acquireCount);
                    sleep(waitInMs);
                    // PriorityWaitException indicates that the request will pass after waiting for {@link @waitInMs}.
                    //表示请求将被等待
                    throw new PriorityWaitException(waitInMs);
                }
            }
            return false;
        }
        return true;
    }

    /**
     * 获得已被获取的token数
     *      1、如果限流方式是Thread线程类型的，则从clusterNode中获得当前持有资源的线程数
     *      2、如果如果限流方式是QPS类型的，从clusterNode中获得当前持采样时间内的通过的qps
     * @param node
     * @return
     */
    private int avgUsedTokens(Node node) {
        if (node == null) {
            return DEFAULT_AVG_USED_TOKENS;
        }
        return grade == RuleConstant.FLOW_GRADE_THREAD ?
                node.curThreadNum() //从clusterNode中获得当前持有资源的线程数
                : (int)(node.passQps());//从clusterNode中获得当前持采样时间内的通过的qps
    }

    private void sleep(long timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }
}
