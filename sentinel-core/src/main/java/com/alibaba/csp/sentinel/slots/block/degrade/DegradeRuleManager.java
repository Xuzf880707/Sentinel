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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.property.DynamicSentinelProperty;
import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.csp.sentinel.property.SentinelProperty;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.StringUtil;

/**
 * @author youji.zj
 * @author jialiang.linjl
 * @author Eric Zhao
 */
public final class DegradeRuleManager {

    private static final Map<String, Set<DegradeRule>> degradeRules = new ConcurrentHashMap<>();

    private static final RulePropertyListener LISTENER = new RulePropertyListener();
    private static SentinelProperty<List<DegradeRule>> currentProperty
        = new DynamicSentinelProperty<>();

    static {
        currentProperty.addListener(LISTENER);
    }

    /**
     * Listen to the {@link SentinelProperty} for {@link DegradeRule}s. The property is the source
     * of {@link DegradeRule}s. Degrade rules can also be set by {@link #loadRules(List)} directly.
     *
     * @param property the property to listen.
     *  为property绑定一个监听器LISTENER，这样在规则变更的时候，可以利用LISTENER实时更新内存中的规则
     */
    public static void register2Property(SentinelProperty<List<DegradeRule>> property) {
        AssertUtil.notNull(property, "property cannot be null");
        synchronized (LISTENER) {
            RecordLog.info("[DegradeRuleManager] Registering new property to degrade rule manager");
            currentProperty.removeListener(LISTENER);
            property.addListener(LISTENER);
            currentProperty = property;
        }
    }
    //熔断降级检查

    /***
     *
     * @param resource
     * @param context
     * @param node
     * @param count
     * @throws BlockException
     * 1、根据资源名称获得熔断规则
     * 2、遍历熔断规则进行熔断检查，任何一个没过，则直接退出
     *
     */
    public static void checkDegrade(ResourceWrapper resource, Context context, DefaultNode node, int count)
        throws BlockException {
        //根据资源名称获得配置的熔断规则
        Set<DegradeRule> rules = degradeRules.get(resource.getName());
        if (rules == null) {
            return;
        }
        //循环熔断规则，只要有一个无法通过，则就熔断
        for (DegradeRule rule : rules) {
            if (!rule.passCheck(context, node, count)) {
                throw new DegradeException(rule.getLimitApp(), rule);
            }
        }
    }

    public static boolean hasConfig(String resource) {
        if (resource == null) {
            return false;
        }
        return degradeRules.containsKey(resource);
    }

    /**
     * Get a copy of the rules.
     *
     * @return a new copy of the rules.
     */
    public static List<DegradeRule> getRules() {
        List<DegradeRule> rules = new ArrayList<>();
        for (Map.Entry<String, Set<DegradeRule>> entry : degradeRules.entrySet()) {
            rules.addAll(entry.getValue());
        }
        return rules;
    }

    /**
     * Load {@link DegradeRule}s, former rules will be replaced.
     *
     * @param rules new rules to load.
     */
    public static void loadRules(List<DegradeRule> rules) {
        try {
            currentProperty.updateValue(rules);
        } catch (Throwable e) {
            RecordLog.warn("[DegradeRuleManager] Unexpected error when loading degrade rules", e);
        }
    }

    /**
     * Set degrade rules for provided resource. Former rules of the resource will be replaced.
     *
     * @param resourceName valid resource name
     * @param rules        new rule set to load
     * @return whether the rules has actually been updated
     * @since 1.5.0
     */
    public static boolean setRulesForResource(String resourceName, Set<DegradeRule> rules) {
        AssertUtil.notEmpty(resourceName, "resourceName cannot be empty");
        try {
            Map<String, Set<DegradeRule>> newRuleMap = new HashMap<>(degradeRules);
            if (rules == null) {
                newRuleMap.remove(resourceName);
            } else {
                Set<DegradeRule> newSet = new HashSet<>();
                for (DegradeRule rule : rules) {
                    if (isValidRule(rule) && resourceName.equals(rule.getResource())) {
                        newSet.add(rule);
                    }
                }
                newRuleMap.put(resourceName, newSet);
            }
            List<DegradeRule> allRules = new ArrayList<>();
            for (Set<DegradeRule> set : newRuleMap.values()) {
                allRules.addAll(set);
            }
            return currentProperty.updateValue(allRules);
        } catch (Throwable e) {
            RecordLog.warn(
                "[DegradeRuleManager] Unexpected error when setting degrade rules for resource: " + resourceName, e);
            return false;
        }
    }

    /****
     * 主要是用于监听熔断规则的变化
     */
    private static class RulePropertyListener implements PropertyListener<List<DegradeRule>> {

        @Override
        public void configUpdate(List<DegradeRule> conf) {
            Map<String, Set<DegradeRule>> rules = loadDegradeConf(conf);
            if (rules != null) {
                degradeRules.clear();
                degradeRules.putAll(rules);
            }
            RecordLog.info("[DegradeRuleManager] Degrade rules received: " + degradeRules);
        }

        @Override
        public void configLoad(List<DegradeRule> conf) {
            Map<String, Set<DegradeRule>> rules = loadDegradeConf(conf);
            if (rules != null) {
                degradeRules.clear();
                degradeRules.putAll(rules);
            }
            RecordLog.info("[DegradeRuleManager] Degrade rules loaded: " + degradeRules);
        }

        /***
         * 更新熔断规则
         * @param list
         * @return
         */
        private Map<String, Set<DegradeRule>> loadDegradeConf(List<DegradeRule> list) {
            Map<String, Set<DegradeRule>> newRuleMap = new ConcurrentHashMap<>();

            if (list == null || list.isEmpty()) {
                return newRuleMap;
            }

            for (DegradeRule rule : list) {
                if (!isValidRule(rule)) {
                    RecordLog.warn(
                        "[DegradeRuleManager] Ignoring invalid degrade rule when loading new rules: " + rule);
                    continue;
                }

                if (StringUtil.isBlank(rule.getLimitApp())) {
                    rule.setLimitApp(RuleConstant.LIMIT_APP_DEFAULT);
                }

                String identity = rule.getResource();
                Set<DegradeRule> ruleSet = newRuleMap.get(identity);
                if (ruleSet == null) {
                    ruleSet = new HashSet<>();
                    newRuleMap.put(identity, ruleSet);
                }
                ruleSet.add(rule);
            }

            return newRuleMap;
        }
    }

    /***
     * 判断规则是否有效
     * @param rule
     * @return
     */
    public static boolean isValidRule(DegradeRule rule) {
        //如果资源名称为空，或者熔断的窗口时间验证不通过，则忽略此规则
        boolean baseValid = rule != null && !StringUtil.isBlank(rule.getResource())
            && rule.getCount() >= 0 && rule.getTimeWindow() > 0;
        if (!baseValid) {
            return false;
        }
        // Warn for RT mode that exceeds the {@code TIME_DROP_VALVE}.
        //获得配置属性：csp.sentinel.statistic.max.rt
        int maxAllowedRt = Constants.TIME_DROP_VALVE;
        //如果是按照响应时间熔断，且配置的阀值时间超过csp.sentinel.statistic.max.rt，则打印日志
        if (rule.getGrade() == RuleConstant.DEGRADE_GRADE_RT && rule.getCount() > maxAllowedRt) {
            RecordLog.warn(String.format("[DegradeRuleManager] WARN: setting large RT threshold (%.1f ms) in RT mode"
                    + " will not take effect since it exceeds the max allowed value (%d ms)", rule.getCount(),
                maxAllowedRt));
        }
        // Check exception ratio mode.
        //如果按照百分比，但是配置的阀值>1,则也不生效
        if (rule.getGrade() == RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO && rule.getCount() > 1) {
            return false;
        }
        return true;
    }
}
