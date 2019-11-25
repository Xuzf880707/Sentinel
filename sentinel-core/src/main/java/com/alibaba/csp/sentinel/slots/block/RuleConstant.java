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
package com.alibaba.csp.sentinel.slots.block;

import com.alibaba.csp.sentinel.node.IntervalProperty;

/**
 * @author youji.zj
 * @author jialiang.linjl
 */
public final class RuleConstant {
    //限流方式：线程数限流
    public static final int FLOW_GRADE_THREAD = 0;
    //限流方式：QPS限流
    public static final int FLOW_GRADE_QPS = 1;
    //熔断方式：响应时间熔断
    public static final int DEGRADE_GRADE_RT = 0;
    /**
     * Degrade by biz exception ratio in the current {@link IntervalProperty#INTERVAL} second(s).
     */
    //熔断方式：异常比熔断
    public static final int DEGRADE_GRADE_EXCEPTION_RATIO = 1;
    /**
     * Degrade by biz exception count in the last 60 seconds.
     */
    //熔断方式：异常数熔断
    public static final int DEGRADE_GRADE_EXCEPTION_COUNT = 2;
    //白名单认证
    public static final int AUTHORITY_WHITE = 0;
    //黑名单认证
    public static final int AUTHORITY_BLACK = 1;
    /**
     * 基于调用关系的流量控制
     * STRATEGY_DIRECT：根据调用方进行限流。ContextUtil.enter(resourceName, origin) 方法中的 origin 参数标明了调用方的身份。
     *      如果 strategy 选择了DIRECT，则还需要根据限流规则中的 limitApp 字段根据调用方在不同的场景中进行流量控制，包括有：”所有调用方“、”特定调用方origin“、”除特定调用方origin之外的调用方“。
     *
     * STRATEGY_RELATE：根据关联流量限流。当两个资源之间具有资源争抢或者依赖关系的时候，这两个资源便具有了关联，可使用关联限流来避免具有关联关系的资源之间过度的争抢。
     *
     * STRATEGY_CHAIN：根据调用链路入口限流。假设来自入口 Entrance1 和 Entrance2 的请求都调用到了资源 NodeA，Sentinel 允许根据某个入口的统计信息对资源进行限流。
     *
     */
    public static final int STRATEGY_DIRECT = 0;
    public static final int STRATEGY_RELATE = 1;
    public static final int STRATEGY_CHAIN = 2;
    //该方式是默认的流量控制方式，当 qps 超过任意规则的阈值后，新的请求就会被立即拒绝，拒绝方式为抛出FlowException。
    // 这种方式适用于对系统处理能力确切已知的情况下，比如通过压测确定了系统的准确水位。
    public static final int CONTROL_BEHAVIOR_DEFAULT = 0;
    //排队等待 ，又称为 冷启动。该方式主要用于当系统长期处于低水位的情况下，流量突然增加时，直接把系统拉升到高水位可能瞬间把系统压垮。
    //通过"冷启动"，让通过的流量缓慢增加，在一定时间内逐渐增加到阈值上限，给冷系统一个预热的时间，避免冷系统被压垮的情况。
    public static final int CONTROL_BEHAVIOR_WARM_UP = 1;
    //慢启动，又称为 匀速器模式。这种方式严格控制了请求通过的间隔时间，也即是让请求以均匀的速度通过，对应的是漏桶算法
    //这种方式主要用于处理间隔性突发的流量，例如消息队列
    public static final int CONTROL_BEHAVIOR_RATE_LIMITER = 2;
    public static final int CONTROL_BEHAVIOR_WARM_UP_RATE_LIMITER = 3;

    public static final String LIMIT_APP_DEFAULT = "default";
    public static final String LIMIT_APP_OTHER = "other";

    public static final int DEFAULT_SAMPLE_COUNT = 2;
    public static final int DEFAULT_WINDOW_INTERVAL_MS = 1000;

    private RuleConstant() {}
}
