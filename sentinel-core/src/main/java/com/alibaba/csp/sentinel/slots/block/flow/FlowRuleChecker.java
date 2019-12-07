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

import java.util.Collection;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.server.EmbeddedClusterTokenServerProvider;
import com.alibaba.csp.sentinel.cluster.client.TokenClientProvider;
import com.alibaba.csp.sentinel.cluster.TokenResultStatus;
import com.alibaba.csp.sentinel.cluster.TokenResult;
import com.alibaba.csp.sentinel.cluster.TokenService;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.node.DefaultNode;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.clusterbuilder.ClusterBuilderSlot;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.alibaba.csp.sentinel.util.function.Function;

/**
 * Rule checker for flow control rules.
 *
 * @author Eric Zhao
 */
public class FlowRuleChecker {
    /***
     * 流量检查
     * @param ruleProvider  用于根据resource获得 Map<String, List<FlowRule>> flowRules
     * @param resource 资源名称
     * @param context 上下文
     * @param node defaultNode
     * @param count
     * @param prioritized
     * @throws BlockException
     * 1、判断限流规则的提供者不能为空，且资源也不能为空
     * 2、根据资源名称从FlowRuleManager.flowRules集合中获取资源对应的限流规则列表
     * 3、遍历资源列表，一个个检查，只要任何一个规则不符合，则拒绝
     */
    public void checkFlow(Function<String, Collection<FlowRule>> ruleProvider, ResourceWrapper resource,
                          Context context, DefaultNode node, int count, boolean prioritized) throws BlockException {
        if (ruleProvider == null || resource == null) {
            return;
        }//根据资源名称获得资源对应的流量规则，从Map<String, List<FlowRule>> flowRules中获得资源对应的限流规则列表
        Collection<FlowRule> rules = ruleProvider.apply(resource.getName());
        if (rules != null) {//如果资源配置了限流规则，则遍历检查限流规则
            for (FlowRule rule : rules) {
                if (!canPassCheck(rule, context, node, count, prioritized)) {
                    throw new FlowException(rule.getLimitApp(), rule);
                }
            }
        }
    }

    public boolean canPassCheck(/*@NonNull*/ FlowRule rule, Context context, DefaultNode node,
                                                    int acquireCount) {
        return canPassCheck(rule, context, node, acquireCount, false);
    }

    /***
     * 根据限流规则，判断是否放行
     * @param rule 限流规则
     * @param context 上下文信息
     * @param node defaultNode
     * @param acquireCount 请求获取的资源个数
     * @param prioritized 默认false
     * @return
     * 1、检查规则的limitApp，不能为空
     * 2、根据集群或本地模式进行规则检查
     */
    public boolean canPassCheck(/*@NonNull*/ FlowRule rule, Context context, DefaultNode node, int acquireCount,
                                                    boolean prioritized) {
        String limitApp = rule.getLimitApp();//默认资源调用者，默认是default
        if (limitApp == null) {
            return true;
        }
        //标识是否为集群限流配置
        if (rule.isClusterMode()) {
            return passClusterCheck(rule, context, node, acquireCount, prioritized);
        }
        //本地规则检查
        return passLocalCheck(rule, context, node, acquireCount, prioritized);
    }

    /***
     *
     * @param rule
     * @param context
     * @param node
     * @param acquireCount
     * @param prioritized
     * @return
     *  1、查询用于统计的node
     *  2、调用相应的Controller进行控制检查
     */
    private static boolean passLocalCheck(FlowRule rule, Context context, DefaultNode node, int acquireCount,
                                          boolean prioritized) {
        //根据资源名称和规则获得相应的节点Node，通常返回的是OrginNode，调用者节点
        Node selectedNode = selectNodeByRequesterAndStrategy(rule, context, node);//获得clusterNode
        if (selectedNode == null) {
            return true;
        }
        //交给behaviorController来判断是否放行
        return rule.getRater().canPass(selectedNode, acquireCount, prioritized);
    }

    /***
     *
     * @param rule 限流规则
     * @param context 上下文
     * @param node DefaultNode
     * @return
     * 1、获得关联的资源
     * 2、
     */
    static Node selectReferenceNode(FlowRule rule, Context context, DefaultNode node) {
        //获得请求资源
        String refResource = rule.getRefResource();
        int strategy = rule.getStrategy();

        if (StringUtil.isEmpty(refResource)) {
            return null;
        }
        //限流方式是：根据关联流量限流
        if (strategy == RuleConstant.STRATEGY_RELATE) {
            //返回集群关联的资源
            return ClusterBuilderSlot.getClusterNode(refResource);
        }
        //如果是根据调用链路入口限流
        if (strategy == RuleConstant.STRATEGY_CHAIN) {
            if (!refResource.equals(context.getName())) {
                return null;
            }
            return node;
        }
        // No node.
        return null;
    }

    private static boolean filterOrigin(String origin) {
        // 如果调用者名称不是default和other,则返回true
        return !RuleConstant.LIMIT_APP_DEFAULT.equals(origin) && !RuleConstant.LIMIT_APP_OTHER.equals(origin);
    }

    /****
     *
     * @param rule 规则对象
     * @param context
     * @param node
     * @return
     * 1、获得规则限制的app，默认是default
     * 2、获得限流策略
     * 3、从上下文中获得档案调用者的节点
     *      如果限制的app和当前的调用者equals，则返回调用这节点：OriginNode
     *      如果限制的app是default，且strategy=STRATEGY_DIRECT，则直接返回clusterNode
     */
    static Node selectNodeByRequesterAndStrategy(/*@NonNull*/ FlowRule rule, Context context, DefaultNode node) {
        // The limit app should not be empty.
        String limitApp = rule.getLimitApp();//获得app，默认是default
        int strategy = rule.getStrategy();//获得限流策略：0-QPS或1-线程
        String origin = context.getOrigin();//获得调用者名称
        //如果limitApp=调用者名称且调用者名称不是 default 和 other,则返回true
        if (limitApp.equals(origin) //如果规则针对的就是orign调用者
                && filterOrigin(origin)//如果调用者名称不是 default 和 other
                ) {
            if (strategy == RuleConstant.STRATEGY_DIRECT) {//如果是直接拒绝策略，则返回调用者节点orignNode
                // Matches limit origin, return origin statistic node.
                return context.getOriginNode();
            }
            //非直接拒绝策略的话，则返回关联的节点
            return selectReferenceNode(rule, context, node);
        } else if (RuleConstant.LIMIT_APP_DEFAULT.equals(limitApp)) {//如果limitApp是default
            if (strategy == RuleConstant.STRATEGY_DIRECT) {
                // Return the cluster node.
                return node.getClusterNode();
            }

            return selectReferenceNode(rule, context, node);
        } else if (
                RuleConstant.LIMIT_APP_OTHER.equals(limitApp)//如果是other的调用者
            && FlowRuleManager.isOtherOrigin(origin, rule.getResource())//存在同时针对资源和other的限流规则
                ) {
            if (strategy == RuleConstant.STRATEGY_DIRECT) {
                return context.getOriginNode();
            }

            return selectReferenceNode(rule, context, node);
        }

        return null;
    }

    /***
     * 如果从集群中获得token失败，则回退化为本地获得token
     * @param rule
     * @param context
     * @param node
     * @param acquireCount
     * @param prioritized
     * @return
     * 1、获得当前节点所持有的 TokenService ，TokenService是规则判断的具体实现。
     *      作为内置的 token server 与服务在同一进程中启动，则返回 DefaultEmbeddedTokenServer
     *      如果本token client节点不是token server,则返回 DefaultClusterTokenClient
     * 2、如果无法通过集群获得TokenService，则降级为本地校验
     * 3、根据 flowId 和 acquireCount向 token服务端发起请求资源。
     *      flowId：是全局唯一的
     * 4、处理请求响应的结果
     */
    private static boolean passClusterCheck(FlowRule rule, Context context, DefaultNode node, int acquireCount,
                                            boolean prioritized) {
        try {
            //请求获得 TokenService（DefaultClusterTokenClient或DefaultEmbeddedTokenServer）
            TokenService clusterService = pickClusterService();
            //如果没有配置集群配置，则蜕化为本地模式
            if (clusterService == null) {
                return fallbackToLocalOrPass(rule, context, node, acquireCount, prioritized);
            }
            //（必需）全局唯一的规则 ID，由集群限流管控端分配.
            long flowId = rule.getClusterConfig().getFlowId();
            //根据 flowId 和 acquireCount向 token服务端发起请求资源
            TokenResult result = clusterService.requestToken(flowId, acquireCount, prioritized);
            //对请求资源进行处理
            return applyTokenResult(result, rule, context, node, acquireCount, prioritized);
            // If client is absent, then fallback to local mode.
        } catch (Throwable ex) {
            RecordLog.warn("[FlowRuleChecker] Request cluster token unexpected failed", ex);
        }
        // Fallback to local flow control when token client or server for this rule is not available.
        // If fallback is not enabled, then directly pass.
        //如果从集群中获得token失败，则回退化为本地获得token
        return fallbackToLocalOrPass(rule, context, node, acquireCount, prioritized);
    }

    private static boolean fallbackToLocalOrPass(FlowRule rule, Context context, DefaultNode node, int acquireCount,
                                                 boolean prioritized) {
        if (rule.getClusterConfig().isFallbackToLocalWhenFail()) {
            return passLocalCheck(rule, context, node, acquireCount, prioritized);
        } else {
            // The rule won't be activated, just pass.
            return true;
        }
    }

    /***
     * 获得当前接待你所持有的 TokenService
     *    如果本token client节点同时也是token server,则返回 DefaultEmbeddedTokenServer
     *    如果本token client节点不是token server,则返回 DefaultClusterTokenClient
     *    Token Client：集群流控客户端，用于向所属 Token Server 通信请求 token。集群限流服务端会返回给客户端结果，决定是否限流。
     *    Token Server：即集群流控服务端，处理来自 Token Client 的请求，根据配置的集群规则判断是否应该发放 token（是否允许通过）。
     * @return
     */
    private static TokenService pickClusterService() {
        //如果当前节点是客户端模式，则返回 DefaultClusterTokenClient
        if (ClusterStateManager.isClient()) {
            return TokenClientProvider.getClient();
        }
        //如果当前节点是服务端模式，则返回 DefaultEmbeddedTokenServer
        if (ClusterStateManager.isServer()) {
            return EmbeddedClusterTokenServerProvider.getServer();
        }
        return null;
    }

    /****
     *  除了请求响应ok以及请求阻塞之外，其它情况下都会降级为本地模式
     * @param result 向token server发起请求token后的响应结果
     * @param rule 限流规则
     * @param context 上下文
     * @param node DefaultNode
     * @param acquireCount 请求的资源数
     * @param prioritized
     * @return
     * 1、如果请求响应ok或者SHOULD_WAIT，则返回获得资源成功。
     * 2、如果请求响应返回blocked，则返回获取资源失败。
     * 3、其它情况下，弱化为本地模式
     */
    private static boolean applyTokenResult(/*@NonNull*/ TokenResult result, FlowRule rule, Context context,
                                                         DefaultNode node,
                                                         int acquireCount, boolean prioritized) {
        switch (result.getStatus()) {
            //如果请求响应ok或者SHOULD_WAIT，则返回获得资源成功。
            case TokenResultStatus.OK:
                return true;
            case TokenResultStatus.SHOULD_WAIT:
                // Wait for next tick.
                try {
                    Thread.sleep(result.getWaitInMs());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return true;
            case TokenResultStatus.NO_RULE_EXISTS:
            case TokenResultStatus.BAD_REQUEST:
            case TokenResultStatus.FAIL:
            case TokenResultStatus.TOO_MANY_REQUEST:
                //弱化为本地模式
                return fallbackToLocalOrPass(rule, context, node, acquireCount, prioritized);
            //如果请求响应返回blocked，则返回获取资源失败。
            case TokenResultStatus.BLOCKED:
            default:
                return false;
        }
    }
}