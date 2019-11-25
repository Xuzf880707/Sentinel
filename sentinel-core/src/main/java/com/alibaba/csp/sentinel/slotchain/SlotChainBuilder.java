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
package com.alibaba.csp.sentinel.slotchain;

/**
 * The builder for processor slot chain.
 *
 * @author qinan.qn
 * @author leyou
 * @author Eric Zhao
 * SlotChainBuilder提供了SPI功能
 * 所以用户可以自己实现该接口，并重写build方法，重定义处理链的顺序
 */
public interface SlotChainBuilder {

    /**
     * Build the processor slot chain.
     *
     * @return a processor slot that chain some slots together
     * 构建一个处理链：ProcessorSlotChain
     */
    ProcessorSlotChain build();
}
