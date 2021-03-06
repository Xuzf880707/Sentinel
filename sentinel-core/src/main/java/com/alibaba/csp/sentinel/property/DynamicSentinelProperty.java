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
package com.alibaba.csp.sentinel.property;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.alibaba.csp.sentinel.log.RecordLog;

public class DynamicSentinelProperty<T> implements SentinelProperty<T> {

    protected Set<PropertyListener<T>> listeners = Collections.synchronizedSet(new HashSet<PropertyListener<T>>());
    private T value = null;

    public DynamicSentinelProperty() {
    }

    public DynamicSentinelProperty(T value) {
        super();
        this.value = value;
    }

    @Override
    public void addListener(PropertyListener<T> listener) {
        listeners.add(listener);
        listener.configLoad(value);
    }

    @Override
    public void removeListener(PropertyListener<T> listener) {
        listeners.remove(listener);
    }

    /***
     * 更新FlowRuleManager中的rule
     * @param newValue the new value.
     * @return
     * 会遍历当前DynamicSentinelProperty绑定的监听器列表，然后由监听器PropertyListener来更新全局限流规则：
     *      FlowRuleManager.flowRules
     *
     * 自定义持久化的存储源(RedisDataSource\NacosDataSource\ZookeeperDataSource\ApolloDataSource)都是利用自己监听底层存储的变化，
     * 进而主动通过它们本身持有的DynamicSentinelProperty引用的updateValue方法，从而通过DynamicSentinelProperty绑定的listner来更新
     * FlowRuleManager.flowRules，从而实现规则的动态持久化
     */
    @Override
    public boolean updateValue(T newValue) {
        if (isEqual(value, newValue)) {//如果旧的规则和新的规则一样，则不更新，返回false
            return false;
        }
        RecordLog.info("[DynamicSentinelProperty] Config will be updated to: " + newValue);

        value = newValue;//替换掉旧的规则
        //遍历规则监听器，并更新监听器中绑定的规则
        for (PropertyListener<T> listener : listeners) {//执行FlowPropertyListener等监听器进行监听，自己也可实现PropertyListener
            listener.configUpdate(newValue);
        }
        return true;
    }

    private boolean isEqual(T oldValue, T newValue) {
        if (oldValue == null && newValue == null) {
            return true;
        }

        if (oldValue == null) {
            return false;
        }

        return oldValue.equals(newValue);
    }

    public void close() {
        listeners.clear();
    }
}
