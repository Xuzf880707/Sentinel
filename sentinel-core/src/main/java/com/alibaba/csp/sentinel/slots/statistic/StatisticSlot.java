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
package com.alibaba.csp.sentinel.slots.statistic;

import java.util.Collection;

import com.alibaba.csp.sentinel.slotchain.ProcessorSlotEntryCallback;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlotExitCallback;
import com.alibaba.csp.sentinel.slots.block.flow.PriorityWaitException;
import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.EntryType;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.node.ClusterNode;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.slotchain.AbstractLinkedProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.BlockException;

/**
 * <p>
 * A processor slot that dedicates to real time statistics.
 * When entering this slot, we need to separately count the following
 * information:
 * <ul>
 * <li>{@link ClusterNode}: total statistics of a cluster node of the resource ID.</li>
 * <li>Origin node: statistics of a cluster node from different callers/origins.</li>
 * <li>{@link DefaultNode}: statistics for specific resource name in the specific context.</li>
 * <li>Finally, the sum statistics of all entrances.</li>
 * </ul>
 * </p>
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 *  则用于记录，统计不同纬度的 runtime 信息
 * StatisticSlot 是 Sentinel 的核心功能插槽之一，用于统计实时的调用数据。
 *      clusterNode：资源唯一标识的 ClusterNode 的 runtime 统计
 *      origin：根据来自不同调用者的统计信息
 *      defaultnode: 根据上下文条目名称和资源 ID 的 runtime 统计
 * Sentinel 底层采用高性能的滑动窗口数据结构 LeapArray 来统计实时的秒级指标数据，可以很好地支撑写多于读的高并发场景。
 *
 * StatisticSlot主要是根据 ProcessorSlotChain 的过滤结果进行统计信息。它主要是触发DefaultNode、的统计功能：
 *      pass：如果通过ProcessorSlotChain过滤，并成功获得信号量，则记录并统计响应的信息，这些信息包括：
 *          DefaultNode.ThreadNum(线程数+1)、DefaultNode.pass(请求通过数+count)、OriginNode.ThreadNum(线程数+1)、OriginNode.pass(请求通过数+count)
 *
 *
 */
public class StatisticSlot extends AbstractLinkedProcessorSlot<DefaultNode> {
    /***
     *
     * @param context         current {@link Context}
     * @param resourceWrapper current resource
     * @param node
     * @param count           tokens needed
     * @param prioritized     whether the entry is prioritized
     * @param args            parameters of the original call
     * @throws Throwable
     * 1、打标记并放行
     * 2、根据资源请求状态开始记录统计信息
     *      pass：如果通过ProcessorSlotChain过滤，并成功获得信号量，则记录并统计响应的信息，这些信息包括：
     *          DefaultNode：ThreadNum(线程数+1)、pass(请求通过数+count)、
     *          OriginNode(如果有指定调用者的话)：ThreadNum(线程数+1)、pass(请求通过数+count)
     *          ENTRY_NODE(这是一个对全局所有的资源信息的输入的统计节点)：ThreadNum(线程数+1)、pass(请求通过数+count)
     *          判断是否是用了 StatisticSlotCallbackRegistry 注册了请求资源成功的回调函数了没，有的话执行回调函数。
     *      blockException：如果被限流或者降级，则记录并统计阻塞或限流的统计信息，这些信息包括：
     *          DefaultNode：blockQps(阻塞数+1)
     *          OriginNode(如果有指定调用者的话)：blockQps(阻塞数+1)
     *          ENTRY_NODE(这是一个对全局所有的资源信息的输入的统计节点)：blockQps(阻塞数+1)
     *          判断是否是用了 StatisticSlotCallbackRegistry 注册了请求资源阻塞的回调函数了没，有的话执行回调函数。
     *      PriorityWaitException：如果因为优先级排队异常，则记录并统计当前的统计信息：
     *          DefaultNode：blockQps(线程数+1)
     *          OriginNode(如果有指定调用者的话)：blockQps(线程数+1)
     *          ENTRY_NODE(这是一个对全局所有的资源信息的输入的统计节点)：threadNum(线程数+1)
     *          判断是否是用了 StatisticSlotCallbackRegistry 注册了请求资源阻塞的回调函数了没，有的话执行回调函数。
     *      其它异常：记录并统计异常的统计信息：
     *          DefaultNode：blockQps(异常数+1)
     *          OriginNode(如果有指定调用者的话)：exceptiionQps(异常数+1)
     *          ENTRY_NODE(这是一个对全局所有的资源信息的输入的统计节点)：blockQps(异常数+1)
     *
     */
    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode node, int count,
                      boolean prioritized, Object... args) throws Throwable {
        try {
            //触发下一个Slot的entry方法
            fireEntry(context, resourceWrapper, node, count, prioritized, args);
            //如果能通过SlotChain中后面的Slot的entry方法，说明没有被限流或降级
            // Request passed, add thread count and pass count.
            //增加访问资源的并发线程数
            node.increaseThreadNum();//线程数+1
            // 再增加当前秒钟pass的请求数
            node.addPassRequest(count);//通过的请求数+count
            // 如果在调用entry之前指定了调用的origin，即调用方
            if (context.getCurEntry().getOriginNode() != null) {
                // 则会有一个originNode，我们也需要做上面两个增加操作
                context.getCurEntry().getOriginNode().increaseThreadNum();//访问orgin的线程数+1
                context.getCurEntry().getOriginNode().addPassRequest(count);//访问orgin的通过的请求数+count
            }

            if (resourceWrapper.getType() == EntryType.IN) {
                // Add count for global inbound entry node for global statistics.
                Constants.ENTRY_NODE.increaseThreadNum();
                Constants.ENTRY_NODE.addPassRequest(count);
            }

            // Handle pass event with registered entry callback handlers.
            //
            for (ProcessorSlotEntryCallback<DefaultNode> handler : StatisticSlotCallbackRegistry.getEntryCallbacks()) {
                handler.onPass(context, resourceWrapper, node, count, args);
            }
        } catch (PriorityWaitException ex) {
            node.increaseThreadNum();
            if (context.getCurEntry().getOriginNode() != null) {
                // Add count for origin node.
                context.getCurEntry().getOriginNode().increaseThreadNum();
            }

            if (resourceWrapper.getType() == EntryType.IN) {
                // Add count for global inbound entry node for global statistics.
                Constants.ENTRY_NODE.increaseThreadNum();
            }
            // Handle pass event with registered entry callback handlers.
            for (ProcessorSlotEntryCallback<DefaultNode> handler : StatisticSlotCallbackRegistry.getEntryCallbacks()) {
                handler.onPass(context, resourceWrapper, node, count, args);
            }
        } catch (BlockException e) {
            // Blocked, set block exception to current entry.
            context.getCurEntry().setError(e);

            // 如果触发了BlockException，则说明获取token失败，被限流
            // 因此增加当前秒Block的请求数
            node.increaseBlockQps(count);
            //这里是针对调用方origin的统计
            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().increaseBlockQps(count);
            }

            if (resourceWrapper.getType() == EntryType.IN) {
                // Add count for global inbound entry node for global statistics.
                Constants.ENTRY_NODE.increaseBlockQps(count);
            }

            // Handle block event with registered entry callback handlers.
            for (ProcessorSlotEntryCallback<DefaultNode> handler : StatisticSlotCallbackRegistry.getEntryCallbacks()) {
                handler.onBlocked(e, context, resourceWrapper, node, count, args);
            }

            throw e;
        } catch (Throwable e) {
            // Unexpected error, set error to current entry.
            context.getCurEntry().setError(e);

            // This should not happen.
            //更新node异常的统计信息
            node.increaseExceptionQps(count);
            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().increaseExceptionQps(count);
            }

            if (resourceWrapper.getType() == EntryType.IN) {
                Constants.ENTRY_NODE.increaseExceptionQps(count);
            }
            throw e;
        }
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        DefaultNode node = (DefaultNode)context.getCurNode();
        //如果是正常退出该entry,表示本次请求成功
        if (context.getCurEntry().getError() == null) {
            // Calculate response time (max RT is TIME_DROP_VALVE).
            //获得响应时间：entry.enter-entry.exit
            long rt = TimeUtil.currentTimeMillis() - context.getCurEntry().getCreateTime();//响应时间=exit时间-创建时间
            if (rt > Constants.TIME_DROP_VALVE) {//最大响应时间不超过4900
                rt = Constants.TIME_DROP_VALVE;
            }

            // Record response time and success count.
            node.addRtAndSuccess(rt, count);//统计rt和 执行成功的次数（分钟级和秒级）
            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().addRtAndSuccess(rt, count);
            }

            node.decreaseThreadNum();//持有资源的线程数-1

            if (context.getCurEntry().getOriginNode() != null) {
                context.getCurEntry().getOriginNode().decreaseThreadNum();
            }

            if (resourceWrapper.getType() == EntryType.IN) {
                Constants.ENTRY_NODE.addRtAndSuccess(rt, count);
                Constants.ENTRY_NODE.decreaseThreadNum();
            }
        } else {
            // Error may happen.
        }

        // Handle exit event with registered exit callback handlers.
        Collection<ProcessorSlotExitCallback> exitCallbacks = StatisticSlotCallbackRegistry.getExitCallbacks();
        for (ProcessorSlotExitCallback handler : exitCallbacks) {
            handler.onExit(context, resourceWrapper, count, args);
        }

        fireExit(context, resourceWrapper, count);
    }
}
