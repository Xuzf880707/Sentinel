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
package com.alibaba.csp.sentinel.cluster.flow;

import java.util.Collection;

import com.alibaba.csp.sentinel.cluster.TokenResultStatus;
import com.alibaba.csp.sentinel.cluster.TokenResult;
import com.alibaba.csp.sentinel.cluster.TokenService;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterParamFlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;

/**
 * Default implementation for cluster {@link TokenService}.
 *
 * @author Eric Zhao
 * @since 1.4.0
 */
public class DefaultTokenService implements TokenService {
    /***
     *
     * @param ruleId the unique rule ID 唯一的规则id
     * @param acquireCount token count to acquire
     * @param prioritized whether the request is prioritized
     * @return
     * 1、根据ruleid 查找对应的FlowRule
     * 2、使用FlowRule进行限流校验
     */
    @Override
    public TokenResult requestToken(Long ruleId, int acquireCount, boolean prioritized) {
        if (notValidRequest(ruleId, acquireCount)) {
            return badRequest();
        }
        // The rule should be valid.
        FlowRule rule = ClusterFlowRuleManager.getFlowRuleById(ruleId);
        if (rule == null) {
            return new TokenResult(TokenResultStatus.NO_RULE_EXISTS);
        }
        //根据
        return ClusterFlowChecker.acquireClusterToken(rule, acquireCount, prioritized);
    }

    @Override
    public TokenResult requestParamToken(Long ruleId, int acquireCount, Collection<Object> params) {
        if (notValidRequest(ruleId, acquireCount) || params == null || params.isEmpty()) {
            return badRequest();
        }
        // The rule should be valid.
        ParamFlowRule rule = ClusterParamFlowRuleManager.getParamRuleById(ruleId);
        if (rule == null) {
            return new TokenResult(TokenResultStatus.NO_RULE_EXISTS);
        }

        return ClusterParamFlowChecker.acquireClusterToken(rule, acquireCount, params);
    }

    private boolean notValidRequest(Long id, int count) {
        return id == null || id <= 0 || count <= 0;
    }

    private TokenResult badRequest() {
        return new TokenResult(TokenResultStatus.BAD_REQUEST);
    }
}
