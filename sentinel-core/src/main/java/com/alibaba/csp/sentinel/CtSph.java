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
package com.alibaba.csp.sentinel;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.context.Context;
import com.alibaba.csp.sentinel.context.ContextUtil;
import com.alibaba.csp.sentinel.context.NullContext;
import com.alibaba.csp.sentinel.slotchain.MethodResourceWrapper;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlot;
import com.alibaba.csp.sentinel.slotchain.ProcessorSlotChain;
import com.alibaba.csp.sentinel.slotchain.ResourceWrapper;
import com.alibaba.csp.sentinel.slotchain.SlotChainProvider;
import com.alibaba.csp.sentinel.slotchain.StringResourceWrapper;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.Rule;

/**
 * {@inheritDoc}
 *
 * @author jialiang.linjl
 * @author leyou(lihao)
 * @author Eric Zhao
 * @see Sph
 */
public class CtSph implements Sph {

    private static final Object[] OBJECTS0 = new Object[0];

    /**
     * Same resource({@link ResourceWrapper#equals(Object)}) will share the same
     * {@link ProcessorSlotChain}, no matter in which {@link Context}.
     * 一个资源对应个 ProcessorSlotChain，不同context的同一资源共享 ProcessorSlotChain
     */
    private static volatile Map<ResourceWrapper, ProcessorSlotChain> chainMap
        = new HashMap<ResourceWrapper, ProcessorSlotChain>();

    private static final Object LOCK = new Object();

    private AsyncEntry asyncEntryWithNoChain(ResourceWrapper resourceWrapper, Context context) {
        AsyncEntry entry = new AsyncEntry(resourceWrapper, null, context);
        entry.initAsyncContext();
        // The async entry will be removed from current context as soon as it has been created.
        entry.cleanCurrentEntryInLocal();
        return entry;
    }

    private AsyncEntry asyncEntryWithPriorityInternal(ResourceWrapper resourceWrapper, int count, boolean prioritized,
                                                      Object... args) throws BlockException {
        Context context = ContextUtil.getContext();
        if (context instanceof NullContext) {
            // The {@link NullContext} indicates that the amount of context has exceeded the threshold,
            // so here init the entry only. No rule checking will be done.
            return asyncEntryWithNoChain(resourceWrapper, context);
        }
        if (context == null) {
            // Using default context.
            context = MyContextUtil.myEnter(Constants.CONTEXT_DEFAULT_NAME, "", resourceWrapper.getType());
        }

        // Global switch is turned off, so no rule checking will be done.
        if (!Constants.ON) {
            return asyncEntryWithNoChain(resourceWrapper, context);
        }

        ProcessorSlot<Object> chain = lookProcessChain(resourceWrapper);

        // Means processor cache size exceeds {@link Constants.MAX_SLOT_CHAIN_SIZE}, so no rule checking will be done.
        if (chain == null) {
            return asyncEntryWithNoChain(resourceWrapper, context);
        }

        AsyncEntry asyncEntry = new AsyncEntry(resourceWrapper, chain, context);
        try {
            chain.entry(context, resourceWrapper, null, count, prioritized, args);
            // Initiate the async context only when the entry successfully passed the slot chain.
            asyncEntry.initAsyncContext();
            // The asynchronous call may take time in background, and current context should not be hanged on it.
            // So we need to remove current async entry from current context.
            asyncEntry.cleanCurrentEntryInLocal();
        } catch (BlockException e1) {
            // When blocked, the async entry will be exited on current context.
            // The async context will not be initialized.
            asyncEntry.exitForContext(context, count, args);
            throw e1;
        } catch (Throwable e1) {
            // This should not happen, unless there are errors existing in Sentinel internal.
            // When this happens, async context is not initialized.
            RecordLog.warn("Sentinel unexpected exception in asyncEntryInternal", e1);

            asyncEntry.cleanCurrentEntryInLocal();
        }
        return asyncEntry;
    }

    private AsyncEntry asyncEntryInternal(ResourceWrapper resourceWrapper, int count, Object... args)
        throws BlockException {
        return asyncEntryWithPriorityInternal(resourceWrapper, count, false, args);
    }

    /***
     *
     * @param resourceWrapper 资源对象
     * @param count 需要的信号量数
     * @param prioritized  是否优先处理
     * @param args 附加参数，也可以被热点参数规则统计
     * @return
     * @throws BlockException
     *
     * 1、检查 ThreadLocal中的context=NullContext：
     *      如果是一个NullContext，表明context数量超过限制了，则这里只是初始化entry，不需要做规则检查
     *      如果context==null，则返回一个默认的name=sentinel_default_context的 CtEntry
     *      如果规则校验全局开关是关闭，则直接返回一个不需要做任何检查的CtEntry
     * 2、为资源ResourceWrapper绑定一个过滤链ProcessorSlot，一个资源对应一个过滤链。后续沿着链路做验证。
     *      如果本地内存里CtSph.chainMap已存在一个对应的过滤链，则直接返回，如果chainMap中的元素超过6000个，则直接返回空
     * 3、创建一个资源对象entry，当前资源对应的Entry添加到context的curEntry
     * 4、处理链ProcessorSlot 根据限流等规则对资源进行检查校验，判断是否匹配所有的规则(限流、熔断、降级等)
     */
    private Entry entryWithPriority(ResourceWrapper resourceWrapper, int count, boolean prioritized, Object... args)
        throws BlockException {
        Context context = ContextUtil.getContext();//从线程中获得上下文
        if (context instanceof NullContext) {//表明context要获得资源数量超过限制了
            //如果是一个NullContext，表明context数量超过限制了，则这里只是初始化entry，不需要做规则检查
            return new CtEntry(resourceWrapper, null, context);
        }
        //获取默认的资源
        if (context == null) {
            // Using default context.
            context = MyContextUtil.myEnter(Constants.CONTEXT_DEFAULT_NAME, "", resourceWrapper.getType());
        }

        //如果 全局开关关闭，则无需用rule进行检查
        if (!Constants.ON) {
            return new CtEntry(resourceWrapper, null, context);
        }
        //将该资源绑定ProcessorSlotChain链，后续在获得一个资源的时候，需要沿着链路做验证
        //如果本地内存里CtSph.chainMap已存在一个对应的过滤链，则直接返回，如果chainMap中的元素超过6000个，则直接返回空
        ProcessorSlot<Object> chain = lookProcessChain(resourceWrapper);
        /*
         * Means amount of resources (slot chain) exceeds {@link Constants.MAX_SLOT_CHAIN_SIZE},
         * so no rule checking will be done.
         */
        if (chain == null) {
            return new CtEntry(resourceWrapper, null, context);
        }
        //检查是否分配资源 将entry绑定到Context上
        Entry e = new CtEntry(resourceWrapper, chain, context);
        try {//开始遍历责任chain链，检查entry是否满足所有配置的规则
            //如果SlotChain的entry方法抛出了BlockException，则将该异常继续向上抛出
            //.如果SlotChain的entry方法正常执行了，则最后会将该entry对象返回
            chain.entry(context, resourceWrapper, null, count, prioritized, args);
        } catch (BlockException e1) {
            e.exit(count, args);
            throw e1;
        } catch (Throwable e1) {
            // This should not happen, unless there are errors existing in Sentinel internal.
            RecordLog.info("Sentinel unexpected exception", e1);
        }
        return e;
    }

    /**
     * Do all {@link Rule}s checking about the resource.
     *
     * <p>Each distinct resource will use a {@link ProcessorSlot} to do rules checking. Same resource will use
     * same {@link ProcessorSlot} globally. </p>
     *
     * <p>Note that total {@link ProcessorSlot} count must not exceed {@link Constants#MAX_SLOT_CHAIN_SIZE},
     * otherwise no rules checking will do. In this condition, all requests will pass directly, with no checking
     * or exception.</p>
     *
     * @param resourceWrapper resource name 资源名称
     * @param count           tokens needed 需要的token数，也就是类似信号量数
     * @param args             附加参数，也可以被热点参数规则统计
     * @return {@link Entry} represents this call
     * @throws BlockException if any rule's threshold is exceeded
     */
    public Entry entry(ResourceWrapper resourceWrapper, int count, Object... args) throws BlockException {
        return entryWithPriority(resourceWrapper, count, false, args);
    }
    /***
     *
     * @param resourceWrapper
     * @return
     * 根据资源获得规则处理链，如果当前资源未绑定一个处理链的话，则新建一个，该方法使用了一个HashMap做了缓存，key是资源对象
     *      如果本地内存里CtSph.chainMap已存在一个对应的过滤链，则直接返回，如果chainMap中的元素超过6000个，则直接返回空
     *      相同的资源会共享全部的规则链，无论是在哪一个context里
     * 1、使用 SlotChainProvider 实现类创建一个处理链
     */
    ProcessorSlot<Object> lookProcessChain(ResourceWrapper resourceWrapper) {
        ProcessorSlotChain chain = chainMap.get(resourceWrapper);
        if (chain == null) {
            synchronized (LOCK) {
                chain = chainMap.get(resourceWrapper);
                if (chain == null) {
                    // Entry size limit.
                    if (chainMap.size() >= Constants.MAX_SLOT_CHAIN_SIZE) {
                        return null;
                    }
                    //通过spi加载一个规则处理链ProcessorSlotChain
                    chain = SlotChainProvider.newSlotChain();
                    //把resourceWrapper->chain 更新到本地内存里，下次同一资源可直接获取
                    Map<ResourceWrapper, ProcessorSlotChain> newMap = new HashMap<ResourceWrapper, ProcessorSlotChain>(
                        chainMap.size() + 1);
                    newMap.putAll(chainMap);
                    newMap.put(resourceWrapper, chain);
                    chainMap = newMap;
                }
            }
        }
        return chain;
    }

    /**
     * Get current size of created slot chains.
     *
     * @return size of created slot chains
     * @since 0.2.0
     */
    public static int entrySize() {
        return chainMap.size();
    }

    /**
     * Reset the slot chain map. Only for internal test.
     *
     * @since 0.2.0
     */
    static void resetChainMap() {
        chainMap.clear();
    }

    /**
     * Only for internal test.
     *
     * @since 0.2.0
     */
    static Map<ResourceWrapper, ProcessorSlotChain> getChainMap() {
        return chainMap;
    }

    /**
     * This class is used for skip context name checking.
     */
    private final static class MyContextUtil extends ContextUtil {
        static Context myEnter(String name, String origin, EntryType type) {
            return trueEnter(name, origin);
        }
    }

    @Override
    public Entry entry(String name) throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, EntryType.OUT);
        return entry(resource, 1, OBJECTS0);
    }

    @Override
    public Entry entry(Method method) throws BlockException {
        MethodResourceWrapper resource = new MethodResourceWrapper(method, EntryType.OUT);
        return entry(resource, 1, OBJECTS0);
    }

    @Override
    public Entry entry(Method method, EntryType type) throws BlockException {
        MethodResourceWrapper resource = new MethodResourceWrapper(method, type);
        return entry(resource, 1, OBJECTS0);
    }

    @Override
    public Entry entry(String name, EntryType type) throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, type);
        return entry(resource, 1, OBJECTS0);
    }

    @Override
    public Entry entry(Method method, EntryType type, int count) throws BlockException {
        MethodResourceWrapper resource = new MethodResourceWrapper(method, type);
        return entry(resource, count, OBJECTS0);
    }

    @Override
    public Entry entry(String name, EntryType type, int count) throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, type);
        return entry(resource, count, OBJECTS0);
    }

    @Override
    public Entry entry(Method method, int count) throws BlockException {
        MethodResourceWrapper resource = new MethodResourceWrapper(method, EntryType.OUT);
        return entry(resource, count, OBJECTS0);
    }

    @Override
    public Entry entry(String name, int count) throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, EntryType.OUT);
        return entry(resource, count, OBJECTS0);
    }

    @Override
    public Entry entry(Method method, EntryType type, int count, Object... args) throws BlockException {
        MethodResourceWrapper resource = new MethodResourceWrapper(method, type);
        return entry(resource, count, args);
    }

    /***
     *
     * @param name  受保护的资源名称，全局唯一
     * @param type  资源类型，用来区分是流入还是流出的，标记当系统不稳定时，是否可以被阻塞
     * @param count 尝试获取的资源数量
     * @param args  附加参数，也可以被热点参数规则统计
     * @return
     * @throws BlockException
     */
    @Override
    public Entry entry(String name, EntryType type, int count, Object... args) throws BlockException {
        //根据资源名称获得一个资源包装对象StringResourceWrapper
        StringResourceWrapper resource = new StringResourceWrapper(name, type);
        //尝试获得count个资源
        return entry(resource, count, args);
    }

    @Override
    public AsyncEntry asyncEntry(String name, EntryType type, int count, Object... args) throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, type);
        return asyncEntryInternal(resource, count, args);//异步参与内部统计
    }

    @Override
    public Entry entryWithPriority(String name, EntryType type, int count, boolean prioritized) throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, type);
        return entryWithPriority(resource, count, prioritized);
    }

    @Override
    public Entry entryWithPriority(String name, EntryType type, int count, boolean prioritized, Object... args)
        throws BlockException {
        StringResourceWrapper resource = new StringResourceWrapper(name, type);
        return entryWithPriority(resource, count, prioritized, args);
    }
}
