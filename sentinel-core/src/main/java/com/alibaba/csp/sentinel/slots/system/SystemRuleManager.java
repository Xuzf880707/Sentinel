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
package com.alibaba.csp.sentinel.slots.system;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.EntryType;
import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.property.DynamicSentinelProperty;
import com.alibaba.csp.sentinel.property.SentinelProperty;
import com.alibaba.csp.sentinel.property.SimplePropertyListener;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.BlockException;

/**
 * <p>
 * Sentinel System Rule makes the inbound traffic and capacity meet. It takes
 * average rt, qps, thread count of incoming requests into account. And it also
 * provides a measurement of system's load, but only available on Linux.
 * </p>
 * <p>
 * rt, qps, thread count is easy to understand. If the incoming requests'
 * rt,qps, thread count exceeds its threshold, the requests will be
 * rejected.however, we use a different method to calculate the load.
 * </p>
 * <p>
 * Consider the system as a pipeline，transitions between constraints result in
 * three different regions (traffic-limited, capacity-limited and danger area)
 * with qualitatively different behavior. When there isn’t enough request in
 * flight to fill the pipe, RTprop determines behavior; otherwise, the system
 * capacity dominates. Constraint lines intersect at inflight = Capacity ×
 * RTprop. Since the pipe is full past this point, the inflight –capacity excess
 * creates a queue, which results in the linear dependence of RTT on inflight
 * traffic and an increase in system load.In danger area, system will stop
 * responding.<br/>
 * Referring to BBR algorithm to learn more.
 * </p>
 * <p>
 * Note that {@link SystemRule} only effect on inbound requests, outbound traffic
 * will not limit by {@link SystemRule}
 * </p>
 *
 * @author jialiang.linjl
 * @author leyou
 */
public class SystemRuleManager {
    //当系统 load1 超过阈值，且系统当前的并发线程数超过系统容量时才会触发系统保护。
    // 系统容量由系统的 maxQps * minRt 计算得出。设定参考值一般是 CPU cores * 2.5。
    private static volatile double highestSystemLoad = Double.MAX_VALUE;
    /**
     * cpu usage, between [0, 1]
     */
    //当系统 CPU 使用率超过阈值即触发系统保护（取值范围 0.0-1.0）。
    private static volatile double highestCpuUsage = Double.MAX_VALUE;
    //当单台机器上所有入口流量的 QPS 达到阈值即触发系统保护。
    private static volatile double qps = Double.MAX_VALUE;
    //当单台机器上所有入口流量的平均 RT 达到阈值即触发系统保护，单位是毫秒。
    private static volatile long maxRt = Long.MAX_VALUE;
    //当单台机器上所有入口流量的并发线程数达到阈值即触发系统保护。
    private static volatile long maxThread = Long.MAX_VALUE;
    /**
     * mark whether the threshold are set by user.
     */
    private static volatile boolean highestSystemLoadIsSet = false;
    private static volatile boolean highestCpuUsageIsSet = false;
    private static volatile boolean qpsIsSet = false;
    private static volatile boolean maxRtIsSet = false;
    private static volatile boolean maxThreadIsSet = false;

    private static AtomicBoolean checkSystemStatus = new AtomicBoolean(false);

    private static SystemStatusListener statusListener = null;
    private final static SystemPropertyListener listener = new SystemPropertyListener();
    private static SentinelProperty<List<SystemRule>> currentProperty = new DynamicSentinelProperty<List<SystemRule>>();

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("sentinel-system-status-record-task", true));

    static {
        checkSystemStatus.set(false);
        statusListener = new SystemStatusListener();
        scheduler.scheduleAtFixedRate(statusListener, 5, 1, TimeUnit.SECONDS);
        currentProperty.addListener(listener);
    }

    /**
     * Listen to the {@link SentinelProperty} for {@link SystemRule}s. The property is the source
     * of {@link SystemRule}s. System rules can also be set by {@link #loadRules(List)} directly.
     *
     * @param property the property to listen.
     */
    public static void register2Property(SentinelProperty<List<SystemRule>> property) {
        synchronized (listener) {
            currentProperty.removeListener(listener);
            property.addListener(listener);
            currentProperty = property;
        }
    }

    /**
     * Load {@link SystemRule}s, former rules will be replaced.
     *
     * @param rules new rules to load.
     */
    public static void loadRules(List<SystemRule> rules) {
        currentProperty.updateValue(rules);
    }

    /**
     * Get a copy of the rules.
     *
     * @return a new copy of the rules.
     */
    public static List<SystemRule> getRules() {

        List<SystemRule> result = new ArrayList<SystemRule>();
        if (!checkSystemStatus.get()) {
            return result;
        }

        if (highestSystemLoadIsSet) {
            SystemRule loadRule = new SystemRule();
            loadRule.setHighestSystemLoad(highestSystemLoad);
            result.add(loadRule);
        }

        if (highestCpuUsageIsSet) {
            SystemRule rule = new SystemRule();
            rule.setHighestCpuUsage(highestCpuUsage);
            result.add(rule);
        }

        if (maxRtIsSet) {
            SystemRule rtRule = new SystemRule();
            rtRule.setAvgRt(maxRt);
            result.add(rtRule);
        }

        if (maxThreadIsSet) {
            SystemRule threadRule = new SystemRule();
            threadRule.setMaxThread(maxThread);
            result.add(threadRule);
        }

        if (qpsIsSet) {
            SystemRule qpsRule = new SystemRule();
            qpsRule.setQps(qps);
            result.add(qpsRule);
        }

        return result;
    }

    public static double getQps() {
        return qps;
    }

    public static void setQps(double qps) {
        SystemRuleManager.qps = qps;
    }

    public static long getMaxRt() {
        return maxRt;
    }

    public static long getMaxThread() {
        return maxThread;
    }

    static class SystemPropertyListener extends SimplePropertyListener<List<SystemRule>> {

        @Override
        public void configUpdate(List<SystemRule> rules) {
            //初始化各变量值
            restoreSetting();
            // systemRules = rules;
            //遍历系统规则
            if (rules != null && rules.size() >= 1) {
                for (SystemRule rule : rules) {
                    //加载系统规则配置
                    loadSystemConf(rule);
                }
            } else {
                checkSystemStatus.set(false);
            }

            RecordLog.info(String.format("[SystemRuleManager] Current system check status: %s, "
                    + "highestSystemLoad: %e, "
                    + "highestCpuUsage: %e, "
                    + "maxRt: %d, "
                    + "maxThread: %d, "
                    + "maxQps: %e",
                checkSystemStatus.get(),
                highestSystemLoad,
                highestCpuUsage,
                maxRt,
                maxThread,
                qps));
        }

        protected void restoreSetting() {
            checkSystemStatus.set(false);

            // should restore changes
            highestSystemLoad = Double.MAX_VALUE;
            highestCpuUsage = Double.MAX_VALUE;
            maxRt = Long.MAX_VALUE;
            maxThread = Long.MAX_VALUE;
            qps = Double.MAX_VALUE;

            highestSystemLoadIsSet = false;
            highestCpuUsageIsSet = false;
            maxRtIsSet = false;
            maxThreadIsSet = false;
            qpsIsSet = false;
        }

    }

    public static Boolean getCheckSystemStatus() {
        return checkSystemStatus.get();
    }

    public static double getHighestSystemLoad() {
        return highestSystemLoad;
    }

    public static void setHighestSystemLoad(double highestSystemLoad) {
        SystemRuleManager.highestSystemLoad = highestSystemLoad;
    }
    
    public static double getCpuUsageThreshold() {
        return highestCpuUsage;
    }

    public static void loadSystemConf(SystemRule rule) {
        boolean checkStatus = false;
        // Check if it's valid.
        //如果配置了限制的最高的系统加载，则开启检查系统负载的开关
        if (rule.getHighestSystemLoad() >= 0) {
            highestSystemLoad = Math.min(highestSystemLoad, rule.getHighestSystemLoad());
            highestSystemLoadIsSet = true;
            checkStatus = true;
        }
        //如果配置了限制的 CPU 使用率，则开启检查系统 CPU 使用率校验的开关
        if (rule.getHighestCpuUsage() >= 0) {
            highestCpuUsage = Math.min(highestCpuUsage, rule.getHighestCpuUsage());
            highestCpuUsageIsSet = true;
            checkStatus = true;
        }
        //如果配置了限制的 平均响应时间RT，则开启检查系统平均响应RT的开关
        if (rule.getAvgRt() >= 0) {
            maxRt = Math.min(maxRt, rule.getAvgRt());
            maxRtIsSet = true;
            checkStatus = true;
        }
        //如果配置了限制的 系统的最大线程数，则开启检查系统当前线程数的开关
        if (rule.getMaxThread() >= 0) {
            maxThread = Math.min(maxThread, rule.getMaxThread());
            maxThreadIsSet = true;
            checkStatus = true;
        }
        //如果配置了限制的 系统的QPS，则开启检查系统QPS的开关
        if (rule.getQps() >= 0) {
            qps = Math.min(qps, rule.getQps());
            qpsIsSet = true;
            checkStatus = true;
        }
        //开启检查系统状态的开关
        checkSystemStatus.set(checkStatus);

    }

    /**
     * Apply {@link SystemRule} to the resource. Only inbound traffic will be checked.
     *
     * @param resourceWrapper the resource.
     * @throws BlockException when any system rule's threshold is exceeded.
     * 我们每次在请求时都会维护一个全局的统计节点ENTRY_NODE，里面包含了整个实例的各种信息的统计：
     *  1、检查系统状态的开关必须打开(前面我们看到只要配置了规则，就会打开)
     *  2、资源类型必须是流入：EntryType.IN
     *  3、检查全局的系统统计节点ENTRY_NODE的QPS，是否超过限制。
     *  4、检查全局的系统统计节点ENTRY_NODE的maxThread，是否超过限制。
     *  5、检查全局的系统统计节点ENTRY_NODE的maxRt，是否超过限制。
     *  6、获得系统的当前的负载信息，判断是否超过限制。
     *  7、获得系统的当前的CPU使用情况，判断是否超过限制。
     */
    public static void checkSystem(ResourceWrapper resourceWrapper) throws BlockException {
        // Ensure the checking switch is on.
        if (!checkSystemStatus.get()) {
            return;
        }

        // for inbound traffic only
        if (resourceWrapper.getType() != EntryType.IN) {
            return;
        }

        // total qps
        double currentQps = Constants.ENTRY_NODE == null ? 0.0 : Constants.ENTRY_NODE.successQps();
        if (currentQps > qps) {
            throw new SystemBlockException(resourceWrapper.getName(), "qps");
        }

        // total thread
        int currentThread = Constants.ENTRY_NODE == null ? 0 : Constants.ENTRY_NODE.curThreadNum();
        if (currentThread > maxThread) {
            throw new SystemBlockException(resourceWrapper.getName(), "thread");
        }

        double rt = Constants.ENTRY_NODE == null ? 0 : Constants.ENTRY_NODE.avgRt();
        if (rt > maxRt) {
            throw new SystemBlockException(resourceWrapper.getName(), "rt");
        }

        // load. BBR algorithm.
        if (highestSystemLoadIsSet && getCurrentSystemAvgLoad() > highestSystemLoad) {
            if (!checkBbr(currentThread)) {
                throw new SystemBlockException(resourceWrapper.getName(), "load");
            }
        }

        // cpu usage
        if (highestCpuUsageIsSet && getCurrentCpuUsage() > highestCpuUsage) {
            if (!checkBbr(currentThread)) {
                throw new SystemBlockException(resourceWrapper.getName(), "cpu");
            }
        }
    }

    private static boolean checkBbr(int currentThread) {
        if (currentThread > 1 &&
            currentThread > Constants.ENTRY_NODE.maxSuccessQps() * Constants.ENTRY_NODE.minRt() / 1000) {
            return false;
        }
        return true;
    }

    public static double getCurrentSystemAvgLoad() {
        return statusListener.getSystemAverageLoad();
    }

    public static double getCurrentCpuUsage() {
        return statusListener.getCpuUsage();
    }
}