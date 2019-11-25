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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.alibaba.csp.sentinel.node.metric.MetricTimerListener;
import com.alibaba.csp.sentinel.property.DynamicSentinelProperty;
import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.csp.sentinel.property.SentinelProperty;

/**
 * <p>
 * One resources can have multiple rules. And these rules take effects in the following order:
 * <ol>
 * <li>requests from specified caller</li>
 * <li>no specified caller</li>
 * </ol>
 * </p>
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 *
 * 这是一个限流规则的管理类
 */
public class FlowRuleManager {
    /***
     * 通过一个本地全局变量来维护限流规则列表：
     *      key： 资源名称
     *      value： 一个资源可以配置多个限流规则
     */
    private static final Map<String, List<FlowRule>> flowRules = new ConcurrentHashMap<String, List<FlowRule>>();

    private static final FlowPropertyListener LISTENER = new FlowPropertyListener();
    /**
     * currentProperty，默认是实现类：DynamicSentinelProperty
     *      包含一组监听规则变化的监听器：Set<PropertyListener<T>> listeners。
     *      这个属性会通过updateValues(List<FlowRule> rules)，将一组rules交给监听器列表listeners来处理。listeners会遍历监听器，然后由监听器来更新FlowRuleManager的全局变量flowRules
     *
     *  目前默认的监听器实现：FlowPropertyListener
     *  用户可自己实现动态加载规则，并交给FlowRuleManager.register2Property注册
     */
    private static SentinelProperty<List<FlowRule>> currentProperty = new DynamicSentinelProperty<List<FlowRule>>();

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1,
        new NamedThreadFactory("sentinel-metrics-record-task", true));

    static {
        currentProperty.addListener(LISTENER);
        //每分钟统计一次各个资源的分钟级的埋点信息
        SCHEDULER.scheduleAtFixedRate(new MetricTimerListener(), 0, 1, TimeUnit.SECONDS);
    }

    /**
     * Listen to the {@link SentinelProperty} for {@link FlowRule}s. The property is the source of {@link FlowRule}s.
     * Flow rules can also be set by {@link #loadRules(List)} directly.
     *
     * @param property the property to listen.
     * 为property绑定一个监听器，这样后面调用property.updateValue的时候就可以通过LISTENER来更新全局的限流规则
     *   (自定义的持久化的datasource，就是通过绑定一个SentinelProperty，后面监听规则变化，如果规则变化的话。
     *     就会调用SentinelProperty.updateValue方法从而更新全局变量FlowRuleManager.flowRules属性)
     */
    public static void register2Property(SentinelProperty<List<FlowRule>> property) {
        AssertUtil.notNull(property, "property cannot be null");
        synchronized (LISTENER) {
            RecordLog.info("[FlowRuleManager] Registering new property to flow rule manager");
            currentProperty.removeListener(LISTENER);
            property.addListener(LISTENER);
            currentProperty = property;
        }
    }

    /**
     * Get a copy of the rules.
     *
     * @return a new copy of the rules.
     * 遍历获得全部限流规则
     */
    public static List<FlowRule> getRules() {
        List<FlowRule> rules = new ArrayList<FlowRule>();
        for (Map.Entry<String, List<FlowRule>> entry : flowRules.entrySet()) {
            rules.addAll(entry.getValue());
        }
        return rules;
    }

    /**
     * Load {@link FlowRule}s, former rules will be replaced.
     * 更新 flowRules，按照<resourceName,List<FlowRule> rules>的map,key是资源名称
     * @param rules new rules to load.
     */
    public static void loadRules(List<FlowRule> rules) {
        currentProperty.updateValue(rules);//更新限流规则
    }

    static Map<String, List<FlowRule>> getFlowRuleMap() {
        return flowRules;
    }

    /***
     * 判断是否为某个资源配置了限流规则
     * @param resource
     * @return
     */
    public static boolean hasConfig(String resource) {
        return flowRules.containsKey(resource);
    }

    /**
     * 判断调用来源
     * @param origin 调用来源
     * @param resourceName 资源名称
     * @return
     */
    public static boolean isOtherOrigin(String origin, String resourceName) {
        //如果来源信息是空的，直接返回false
        if (StringUtil.isEmpty(origin)) {
            return false;
        }
        //根据资源名称获得所有的限流规则
        List<FlowRule> rules = flowRules.get(resourceName);
        //如果某个限流规则限制的app的确和目标origin一致，说明 other的调用源
        if (rules != null) {
            for (FlowRule rule : rules) {
                if (origin.equals(rule.getLimitApp())) {
                    return false;
                }
            }
        }

        return true;
    }

    /***
     * 负责监听并更新全局的限流规则
     */
    private static final class FlowPropertyListener implements PropertyListener<List<FlowRule>> {
        //给监听器分配规则
        @Override
        public void configUpdate(List<FlowRule> value) {
            //整理规则列表，key为资源名称，value为资源对应的规则列表
            Map<String, List<FlowRule>> rules = FlowRuleUtil.buildFlowRuleMap(value);
            if (rules != null) {
                flowRules.clear();
                flowRules.putAll(rules);
            }
            RecordLog.info("[FlowRuleManager] Flow rules received: " + flowRules);
        }

        @Override
        public void configLoad(List<FlowRule> conf) {
            Map<String, List<FlowRule>> rules = FlowRuleUtil.buildFlowRuleMap(conf);
            if (rules != null) {
                flowRules.clear();
                flowRules.putAll(rules);
            }
            RecordLog.info("[FlowRuleManager] Flow rules loaded: " + flowRules);
        }
    }

}
