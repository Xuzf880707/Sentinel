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
package com.alibaba.csp.sentinel.node.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.config.SentinelConfig;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.node.ClusterNode;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.clusterbuilder.ClusterBuilderSlot;

/**
 * @author jialiang.linjl
 */
public class MetricTimerListener implements Runnable {

    private static final MetricWriter metricWriter = new MetricWriter(SentinelConfig.singleMetricFileSize(),
        SentinelConfig.totalMetricFileCount());

    /***
     *定时的统计资源的埋点信息，每分钟统计一次
     */
    @Override
    public void run() {
        /***
         * key：时间
         * values：一个列表，列表元素是各个资源的统计信息
         */
        Map<Long, List<MetricNode>> maps = new TreeMap<Long, List<MetricNode>>();
        /***
         * 从每个资源对应的集群节点上，获取所有的时间周期的埋点信息
         */
        for (Entry<ResourceWrapper, ClusterNode> e : ClusterBuilderSlot.getClusterNodeMap().entrySet()) {
            String name = e.getKey().getName();//获得资源名称
            ClusterNode node = e.getValue();//获得资源名称对应的集群节点
            Map<Long, MetricNode> metrics = node.metrics();//每个集群节点上获得资源的metric信息
            aggregate(maps, metrics, name);//统计
        }
        //统计各个时间点资源的统计信息
        aggregate(maps, Constants.ENTRY_NODE.metrics(), Constants.TOTAL_IN_RESOURCE_NAME);
        if (!maps.isEmpty()) {
            for (Entry<Long, List<MetricNode>> entry : maps.entrySet()) {
                try {
                    metricWriter.write(entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    RecordLog.warn("[MetricTimerListener] Write metric error", e);
                }
            }
        }
    }

    private void aggregate(Map<Long, List<MetricNode>> maps, Map<Long, MetricNode> metrics, String resourceName) {
        for (Entry<Long, MetricNode> entry : metrics.entrySet()) {
            long time = entry.getKey();//资源时间点
            MetricNode metricNode = entry.getValue();
            metricNode.setResource(resourceName);
            if (maps.get(time) == null) {
                maps.put(time, new ArrayList<MetricNode>());
            }
            List<MetricNode> nodes = maps.get(time);
            nodes.add(entry.getValue());
        }
    }

}
