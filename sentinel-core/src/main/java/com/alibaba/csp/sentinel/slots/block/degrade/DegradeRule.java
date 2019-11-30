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
package com.alibaba.csp.sentinel.slots.block.degrade;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.node.ClusterNode;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.slots.block.AbstractRule;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.clusterbuilder.ClusterBuilderSlot;

/**
 * <p>
 * Degrade is used when the resources are in an unstable state, these resources
 * will be degraded within the next defined time window. There are two ways to
 * measure whether a resource is stable or not:
 * </p>
 * <ul>
 * <li>
 * Average response time ({@code DEGRADE_GRADE_RT}): When
 * the average RT exceeds the threshold ('count' in 'DegradeRule', in milliseconds), the
 * resource enters a quasi-degraded state. If the RT of next coming 5
 * requests still exceed this threshold, this resource will be downgraded, which
 * means that in the next time window (defined in 'timeWindow', in seconds) all the
 * access to this resource will be blocked.
 * </li>
 * <li>
 * Exception ratio: When the ratio of exception count per second and the
 * success qps exceeds the threshold, access to the resource will be blocked in
 * the coming window.
 * </li>
 * </ul>
 *
 * @author jialiang.linjl
 *
 * 熔断是依赖于clusterNode，所以是以资源为粒度进行熔断
 */
public class DegradeRule extends AbstractRule {

    private static final int RT_MAX_EXCEED_N = 5;

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private static ScheduledExecutorService pool = Executors.newScheduledThreadPool(
        Runtime.getRuntime().availableProcessors(), new NamedThreadFactory("sentinel-degrade-reset-task", true));

    public DegradeRule() {}

    public DegradeRule(String resourceName) {
        setResource(resourceName);
    }

    /**
     * RT threshold or exception ratio threshold count.
     */
    private double count;

    /**
     * Degrade recover timeout (in seconds) when degradation occurs.
     */
    private int timeWindow;

    /**
     * Degrade strategy (0: average RT, 1: exception ratio).
     */
    private int grade = RuleConstant.DEGRADE_GRADE_RT;

    private final AtomicBoolean cut = new AtomicBoolean(false);

    public int getGrade() {
        return grade;
    }

    public DegradeRule setGrade(int grade) {
        this.grade = grade;
        return this;
    }

    private AtomicLong passCount = new AtomicLong(0);

    public double getCount() {
        return count;
    }

    public DegradeRule setCount(double count) {
        this.count = count;
        return this;
    }

    private boolean isCut() {
        return cut.get();
    }

    private void setCut(boolean cut) {
        this.cut.set(cut);
    }

    public AtomicLong getPassCount() {
        return passCount;
    }

    public int getTimeWindow() {
        return timeWindow;
    }

    public DegradeRule setTimeWindow(int timeWindow) {
        this.timeWindow = timeWindow;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DegradeRule)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        DegradeRule that = (DegradeRule)o;

        if (count != that.count) {
            return false;
        }
        if (timeWindow != that.timeWindow) {
            return false;
        }
        if (grade != that.grade) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + new Double(count).hashCode();
        result = 31 * result + timeWindow;
        result = 31 * result + grade;
        return result;
    }

    /***
     *
     * @param context current {@link Context}
     * @param node    current {@link com.alibaba.csp.sentinel.node.Node}
     * @param acquireCount
     * @param args    arguments of the original invocation.
     * @return
     * 1、判断熔断开关是否打开：如果打开，则直接拒绝放行
     * 2、获得资源对应的ClusterNode统计节点
     * 3、检查熔断规则
     *      a、DEGRADE_GRADE_RT：根据响应时间RT进行熔断：
     *          根据采样时间，计算采样时间段内的平均rt=采样时间段内（包含两个滑动窗口）所有成功请求总的响应时间/采样时间段内（包含两个滑动窗口）的所有成功的请求数。
     *          如果平均响应时间小于阈值，则放行。
     *          如果平均响应时间大于阈值，则如果判断已放行的超过阈值的请求数是否小于5，如果是的话，则继续放行，否则打开开关熔断
     *          注意：这里只统计业务处理请求成功的。
     *      b、DEGRADE_GRADE_EXCEPTION_RATIO：根据异常比熔断：注意，这里的异常是业务异常
     *          根据采样时间，，获得当前的异常数和成功请求数。注意，这里成功的请求数里只包括抛出业务异常的请求。
     *          计算 exception/success，如果小于阈值，则放行。
     *          如果比例大于阈值，则打开开关熔断
     *
     *      c、DEGRADE_GRADE_EXCEPTION_COUNT：直接根据异常数熔断，直接判断异常数是否超过阈值，如果是，则打开熔断开关
     * 4、如果满足熔断开关，则打开熔断开关。同时新建一个定时任务
     *      定时任务是负责在指定的窗口时间timeWindow后，将熔断开关关闭，并且对通过的请求数进行清零，
     *
     */
    @Override
    public boolean passCheck(Context context, DefaultNode node, int acquireCount, Object... args) {
        if (cut.get()) {//如果熔断开关打开的话，则直接拒绝
            return false;
        }
        //获得当前资源的统计信息ClusterNode
        ClusterNode clusterNode = ClusterBuilderSlot.getClusterNode(this.getResource());
        if (clusterNode == null) {
            return true;
        }
        //如果是根据响应设计RT
        if (grade == RuleConstant.DEGRADE_GRADE_RT) {
            //这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除
            double rt = clusterNode.avgRt();//获得采样时间段内的平均响应时间
            if (rt < this.count) {//如果平均响应时间<阈值，则成功放行
                passCount.set(0);
                return true;
            }
            //走到这边说明平均的响应时间超过阈值了，则通过的请求数是否达到统计的数量(统计请求数要有个阈值，不然很可能因为网络抖动误杀)
            // Sentinel will degrade the service only if count exceeds.
            if (passCount.incrementAndGet() < RT_MAX_EXCEED_N) {
                return true;
            }
        } else if (grade == RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO) {//如果根据错误比
            double exception = clusterNode.exceptionQps();//当前采样时间内的两个滑动窗口的异常的qps
            // 2、success=2
            double success = clusterNode.successQps();//前一秒的每秒成功数（包括拿到token但是抛出业务异常的请求） 2
            //3、total=5
            double total = clusterNode.totalQps();//前一秒总的请求数 5
            // if total qps less than RT_MAX_EXCEED_N, pass.（包括：获得token但是业务失败+获得token且业务成功+没获得token）
            if (total < RT_MAX_EXCEED_N) {//如果总的查询还未超过默认的阈值，则直接反省
                return true;
            }
            //计算真正的成功请求数，也就是 获得token且业务成功=获得token成功数-异常数
            double realSuccess = success - exception; //2-1=0，
            if (realSuccess <= 0//如果我的realSuccess>0,就不用判断exception的最小值是否满足阈值
                    && exception < RT_MAX_EXCEED_N//获得token但是业务失败
                    ) {
                return true;
            }
            //如果失败数/成功数<count，也就是小于阈值，则放行，不然打开熔断开关
            if (exception / success < count) {//1/2=50%
                return true;
            }
        } else if (grade == RuleConstant.DEGRADE_GRADE_EXCEPTION_COUNT) {//如果根据异常数
            double exception = clusterNode.totalException();//直接判断异常数是否超过阈值
            if (exception < count) {
                return true;
            }
        }
        //如果原来cut是false，就是未打开熔断开关，则开启熔断开关
        if (cut.compareAndSet(false, true)) {
            ResetTask resetTask = new ResetTask(this);
            pool.schedule(resetTask, timeWindow, TimeUnit.SECONDS);
        }

        return false;
    }

    @Override
    public String toString() {
        return "DegradeRule{" +
            "resource=" + getResource() +
            ", grade=" + grade +
            ", count=" + count +
            ", limitApp=" + getLimitApp() +
            ", timeWindow=" + timeWindow +
            "}";
    }

    private static final class ResetTask implements Runnable {

        private DegradeRule rule;

        ResetTask(DegradeRule rule) {
            this.rule = rule;
        }

        @Override
        public void run() {
            rule.getPassCount().set(0);
            rule.cut.set(false);
        }
    }
}

