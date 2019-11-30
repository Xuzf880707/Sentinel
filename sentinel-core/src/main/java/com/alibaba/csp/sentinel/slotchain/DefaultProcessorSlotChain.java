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

import com.alibaba.csp.sentinel.context.Context;

/**
 * @author qinan.qn
 * @author jialiang.linjl
 */
public class DefaultProcessorSlotChain extends ProcessorSlotChain {
    //初始化一个head节点 不做任何逻辑，只是负责发起事件传播
    AbstractLinkedProcessorSlot<?> first = new AbstractLinkedProcessorSlot<Object>() {

        @Override
        public void entry(Context context, ResourceWrapper resourceWrapper, Object t, int count, boolean prioritized, Object... args)
            throws Throwable {
            //传递执行entry
            super.fireEntry(context, resourceWrapper, t, count, prioritized, args);
        }

        @Override
        public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
            //传递退出entry
            super.fireExit(context, resourceWrapper, count, args);
        }

    };
    //链表的尾结点
    AbstractLinkedProcessorSlot<?> end = first;

    /***
     * 1、将原来first的后面节点加到目标节点的下一个节点
     * 2、将当前节点设置为fast节点的下一个节点
     * 3、如果一开始end指针指向的head节点，则要修改end指针的引用
     *      注意：
     *          first的指针指向head节点，不会变。因为head节点只是负责发起传播事件
     *          end的指针在第一个节点加入的时候，修改引用。后续不会再变了
     * eg:
     *      before：head。（frist和end都指向head节点）
     *      before: head->c。（frist指向head节点，end指向c节点）
     *      after：head->b->c（frist指向head节点，end指向c节点）
     *      after: head->a->b->c（frist指向head节点，end指向c节点）
     * @param protocolProcessor 待添加的节点
     */
    @Override
    public void addFirst(AbstractLinkedProcessorSlot<?> protocolProcessor) {
        protocolProcessor.setNext(first.getNext());
        first.setNext(protocolProcessor);
        if (end == first) {
            end = protocolProcessor;
        }
    }

    /***
     * 1、将新节点添加到末尾，并修改end的指针的引用
     *      注意：每次新节点加入的时候，end指针都指向新的节点
     * @param protocolProcessor 待添加的节点
     */
    @Override
    public void addLast(AbstractLinkedProcessorSlot<?> protocolProcessor) {
        end.setNext(protocolProcessor);
        end = protocolProcessor;
    }

    /**
     * Same as {@link #addLast(AbstractLinkedProcessorSlot)}.
     *
     * @param next processor to be added.
     */
    @Override
    public void setNext(AbstractLinkedProcessorSlot<?> next) {
        addLast(next);
    }

    @Override
    public AbstractLinkedProcessorSlot<?> getNext() {
        return first.getNext();
    }

    /***
     *
     * @param context         current {@link Context} 线程上下文
     * @param resourceWrapper current resource 资源
     * @param t
     * @param count           tokens needed 要获得的资源个数
     * @param prioritized     whether the entry is prioritized
     * @param args            parameters of the original call
     * @throws Throwable
     *  触发head节点开始传播事件
     */
    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, Object t, int count, boolean prioritized, Object... args)
        throws Throwable {
        first.transformEntry(context, resourceWrapper, t, count, prioritized, args);
    }

    /***
     * 触发head节点开始传播跳出事件
     * @param context         current {@link Context}
     * @param resourceWrapper current resource
     * @param count           tokens needed
     * @param args            parameters of the original call
     */
    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        first.exit(context, resourceWrapper, count, args);
    }

}
