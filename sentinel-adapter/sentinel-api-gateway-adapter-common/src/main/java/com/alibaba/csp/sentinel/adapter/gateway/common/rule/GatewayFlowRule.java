/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.adapter.gateway.common.rule;

import java.util.Objects;

import com.alibaba.csp.sentinel.adapter.gateway.common.SentinelGatewayConstants;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;

/**
 * @author Eric Zhao
 * @since 1.6.0
 */
public class GatewayFlowRule {

    private String resource;//资源名称
    //规则是针对 API Gateway 的 route（RESOURCE_MODE_ROUTE_ID）
    // 还是用户在 Sentinel 中定义的 API 分组（RESOURCE_MODE_CUSTOM_API_NAME），默认是 route。
    private int resourceMode = SentinelGatewayConstants.RESOURCE_MODE_ROUTE_ID;
    //限流指标维度，同限流规则的 grade 字段。
    private int grade = RuleConstant.FLOW_GRADE_QPS;
    private double count;
    private long intervalSec = 1;//统计时间窗口，单位是秒，默认是 1 秒。
    //流量整形的控制效果，同限流规则的 controlBehavior 字段，目前支持快速失败和匀速排队两种模式，默认是快速失败。
    private int controlBehavior = RuleConstant.CONTROL_BEHAVIOR_DEFAULT;
    private int burst;//应对突发请求时额外允许的请求数目。
    /**
     * For throttle (rate limiting with queueing).
     * 匀速排队模式下的最长排队时间，单位是毫秒，仅在匀速排队模式下生效。
     */
    private int maxQueueingTimeoutMs = 500;

    /**
     * For parameter flow control. If not set, the gateway rule will be
     * converted to normal flow rule.
     * 参数限流配置。若不提供，则代表不针对参数进行限流，该网关规则将会被转换成普通流控规则；否则会转换成热点规则
     */
    private GatewayParamFlowItem paramItem;

    public GatewayFlowRule() {}

    public GatewayFlowRule(String resource) {
        this.resource = resource;
    }

    public String getResource() {
        return resource;
    }

    public GatewayFlowRule setResource(String resource) {
        this.resource = resource;
        return this;
    }

    public int getResourceMode() {
        return resourceMode;
    }

    public GatewayFlowRule setResourceMode(int resourceMode) {
        this.resourceMode = resourceMode;
        return this;
    }

    public int getGrade() {
        return grade;
    }

    public GatewayFlowRule setGrade(int grade) {
        this.grade = grade;
        return this;
    }

    public int getControlBehavior() {
        return controlBehavior;
    }

    public GatewayFlowRule setControlBehavior(int controlBehavior) {
        this.controlBehavior = controlBehavior;
        return this;
    }

    public double getCount() {
        return count;
    }

    public GatewayFlowRule setCount(double count) {
        this.count = count;
        return this;
    }

    public long getIntervalSec() {
        return intervalSec;
    }

    public GatewayFlowRule setIntervalSec(long intervalSec) {
        this.intervalSec = intervalSec;
        return this;
    }

    public int getBurst() {
        return burst;
    }

    public GatewayFlowRule setBurst(int burst) {
        this.burst = burst;
        return this;
    }

    public GatewayParamFlowItem getParamItem() {
        return paramItem;
    }

    public GatewayFlowRule setParamItem(GatewayParamFlowItem paramItem) {
        this.paramItem = paramItem;
        return this;
    }

    public int getMaxQueueingTimeoutMs() {
        return maxQueueingTimeoutMs;
    }

    public GatewayFlowRule setMaxQueueingTimeoutMs(int maxQueueingTimeoutMs) {
        this.maxQueueingTimeoutMs = maxQueueingTimeoutMs;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }

        GatewayFlowRule rule = (GatewayFlowRule)o;

        if (resourceMode != rule.resourceMode) { return false; }
        if (grade != rule.grade) { return false; }
        if (Double.compare(rule.count, count) != 0) { return false; }
        if (intervalSec != rule.intervalSec) { return false; }
        if (controlBehavior != rule.controlBehavior) { return false; }
        if (burst != rule.burst) { return false; }
        if (maxQueueingTimeoutMs != rule.maxQueueingTimeoutMs) { return false; }
        if (!Objects.equals(resource, rule.resource)) { return false; }
        return Objects.equals(paramItem, rule.paramItem);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = resource != null ? resource.hashCode() : 0;
        result = 31 * result + resourceMode;
        result = 31 * result + grade;
        temp = Double.doubleToLongBits(count);
        result = 31 * result + (int)(temp ^ (temp >>> 32));
        result = 31 * result + (int)(intervalSec ^ (intervalSec >>> 32));
        result = 31 * result + controlBehavior;
        result = 31 * result + burst;
        result = 31 * result + maxQueueingTimeoutMs;
        result = 31 * result + (paramItem != null ? paramItem.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GatewayFlowRule{" +
            "resource='" + resource + '\'' +
            ", resourceMode=" + resourceMode +
            ", grade=" + grade +
            ", count=" + count +
            ", intervalSec=" + intervalSec +
            ", controlBehavior=" + controlBehavior +
            ", burst=" + burst +
            ", maxQueueingTimeoutMs=" + maxQueueingTimeoutMs +
            ", paramItem=" + paramItem +
            '}';
    }
}
