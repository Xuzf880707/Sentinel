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
package com.alibaba.csp.sentinel.slots.block.flow.controller;

import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.Node;
import com.alibaba.csp.sentinel.slots.block.flow.TrafficShapingController;

/**
 * <p>
 * The principle idea comes from Guava. However, the calculation of Guava is
 * rate-based, which means that we need to translate rate to QPS.
 * </p>
 *
 * <p>
 * Requests arriving at the pulse may drag down long idle systems even though it
 * has a much larger handling capability in stable period. It usually happens in
 * scenarios that require extra time for initialization, e.g. DB establishes a connection,
 * connects to a remote service, and so on. That’s why we need “warm up”.
 * </p>
 *
 * <p>
 * Sentinel's "warm-up" implementation is based on the Guava's algorithm.
 * However, Guava’s implementation focuses on adjusting the request interval,
 * which is similar to leaky bucket. Sentinel pays more attention to
 * controlling the count of incoming requests per second without calculating its interval,
 * which resembles token bucket algorithm.
 * </p>
 *
 * <p>
 * The remaining tokens in the bucket is used to measure the system utility.
 * Suppose a system can handle b requests per second. Every second b tokens will
 * be added into the bucket until the bucket is full. And when system processes
 * a request, it takes a token from the bucket. The more tokens left in the
 * bucket, the lower the utilization of the system; when the token in the token
 * bucket is above a certain threshold, we call it in a "saturation" state.
 * </p>
 *
 * <p>
 * Base on Guava’s theory, there is a linear equation we can write this in the
 * form y = m * x + b where y (a.k.a y(x)), or qps(q)), is our expected QPS
 * given a saturated period (e.g. 3 minutes in), m is the rate of change from
 * our cold (minimum) rate to our stable (maximum) rate, x (or q) is the
 * occupied token.
 * </p>
 *
 * @author jialiang.linjl
 */
public class WarmUpController implements TrafficShapingController {

    protected double count;//限流时候，每秒限制的流量
    private int coldFactor;//默认是3 每秒流量增加的个数
    protected int warningToken = 0;
    private int maxToken;
    protected double slope;

    protected AtomicLong storedTokens = new AtomicLong(0);
    protected AtomicLong lastFilledTime = new AtomicLong(0);

    public WarmUpController(double count, int warmUpPeriodInSec, int coldFactor) {
        construct(count, warmUpPeriodInSec, coldFactor);
    }

    public WarmUpController(double count, int warmUpPeriodInSec) {
        construct(count, warmUpPeriodInSec, 3);
    }

    private void construct(double count, int warmUpPeriodInSec, int coldFactor) {

        if (coldFactor <= 1) {
            throw new IllegalArgumentException("Cold factor should be larger than 1");
        }
        //每秒最大允许通过的QPS
        this.count = count;
        //冷却因子 默认是3
        this.coldFactor = coldFactor;
        //警戒令牌=(int)(warmUpPeriodInSec * count) / (coldFactor - 1)；
        //每秒count个令牌*热身秒数，即热身期间所有令牌数/冷却因子-1，计算得出警戒值。
        // thresholdPermits = 0.5 * warmupPeriod / stableInterval.
        // warningToken = 100;
        warningToken = (int)(warmUpPeriodInSec * count) / (coldFactor - 1);
        // / maxPermits = thresholdPermits + 2 * warmupPeriod /
        // (stableInterval + coldInterval)
        // maxToken = 200 最大令牌=warningToken + (int)(2 * warmUpPeriodInSec * count / (1.0 + coldFactor))
        //由于默认coldFactor=3，所以默认：maxToken=2*warningToken
        maxToken = warningToken + (int)(2 * warmUpPeriodInSec * count / (1.0 + coldFactor));

        // slope 斜率=(coldFactor - 1.0) / count / (maxToken - warningToken)
        // slope = (coldIntervalMicros - stableIntervalMicros) / (maxPermits
        // - thresholdPermits);
        slope = (coldFactor - 1.0) / count / (maxToken - warningToken);
        //默认：slope=(coldFactor - 1)^2/(warmUpPeriodInSec*count^2)

    }

    @Override
    public boolean canPass(Node node, int acquireCount) {
        return canPass(node, acquireCount, false);
    }

    /***
     * 1、获得当前通过的QPS
     * 2、获得前一个窗口的QPS
     * 3、重新计算新的冷却令牌，并同步更新storedTokens（桶里剩余的令牌）
     * 4、如果storedTokens超过警戒线，也就是利用率很低。说明令牌桶饱和了，我们要冷却
     *      则根据斜率重新计算新的警戒线
     *    如果当前storedTokens小于警戒令牌，则直接判断QPS是否
     * @param node resource node
     * @param acquireCount count to acquire
     * @param prioritized whether the request is prioritized
     * @return
     *
     * 主要是可能由于令牌桶每秒生成的令牌数超过令牌的消耗速度，所以会导致桶中的累积的越来越多。
     * 这里会前一秒的QPS和当前累积的令牌桶，重新计算斜率，也就是生产令牌的速度
     */
    @Override
    public boolean canPass(Node node, int acquireCount, boolean prioritized) {
        //获得通过资源的当前时间的qps
        long passQps = (long) node.passQps();
        //获得当前时间前滑动窗口的的qps
        long previousQps = (long) node.previousPassQps();
        //重新计算新的冷却令牌，并同步更新storedTokens
        syncToken(previousQps);

        // 开始计算它的斜率
        // 如果进入了警戒线，开始调整他的qps
        long restToken = storedTokens.get();//从令牌桶storeTokens中获取令牌
        if (restToken >= warningToken) {//如果当前令牌大于等于警戒令牌
            long aboveToken = restToken - warningToken;//当前令牌减去警戒令牌（超出警戒令牌部分）aboveToken
            // 消耗的速度要比warning快，但是要比慢
            // current interval = restToken*slope+1/count
            double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));//根据斜率计算警戒qps
            if (passQps + acquireCount <= warningQps) {//当前qps+申请制acquireCount<=警戒qps；返回通过true，否则返回拒绝false
                return true;
            }
        } else {//如果当前令牌小于警戒令牌
            if (passQps + acquireCount <= count) {//当前qps+申请值acquireCount<=count（阈值count）返回通过true，否则返回拒绝false
                return true;
            }
        }

        return false;
    }

    /***
     * 获得桶里剩余的
     * 1、获得当前时间(毫秒转成秒)
     * 2、获得上一次填充的时间，默认是0
     * 3、如果当前时间<=上次填充令牌桶的时间，则直接返回
     * 4、获得storedTokens的值
     * 5、获得新的冷却令牌，并同步更新storedTokens
     * @param passQps
     */
    protected void syncToken(long passQps) {
        //当前时间
        long currentTime = TimeUtil.currentTimeMillis();
        //当前时间：秒
        currentTime = currentTime - currentTime % 1000;
        //获取上次填充token时间，默认是0
        long oldLastFillTime = lastFilledTime.get();
        if (currentTime <= oldLastFillTime) {//如果处理过的当前时间小于等于上次填充时间直接返回（因为抹掉了秒之后的所有位的数取整，即每秒填充一次）
            return;
        }
        //获取上次填充值storedTokens.get()
        long oldValue = storedTokens.get();
        //计算当前令牌桶中冷却的令牌数量
        long newValue = coolDownTokens(currentTime, passQps);//冷却令牌
        //设置新令牌如果成功则将上个时间窗通过的qps减去，如果减去之后小于0则设置为0
        if (storedTokens.compareAndSet(oldValue, newValue)) {
            //减去上一个的QPS=令牌桶剩余的令牌数(表示生产令牌过快，因此会作为是否需要冷却的依据)
            long currentValue = storedTokens.addAndGet(0 - passQps);
            if (currentValue < 0) {
                storedTokens.set(0L);
            }
            lastFilledTime.set(currentTime);//设置填充时间
        }

    }

    /***
     * 根据前一秒的QPS，来计算当前桶中剩余的令牌数，最多不能超过最大的剩余数maxToken
     * 1、获得storedTokens的值
     * 2、如果storedTokens<警戒值
     *      新令牌=((当前时间-上次填充时间)/1000(即转为秒))*count（阈值count）+老令牌
     *    如果storedTokens>警戒值
     *        a、上个时间窗的qps小于(count/冷却因子) ，新令牌=同样按照小于警戒令牌的算法计算。
     *        b、如果大于等于则不再增加新令牌
     * @param currentTime 当前时间
     * @param passQps 前一个窗口的QPS
     * @return
     */
    private long coolDownTokens(long currentTime, long passQps) {
        long oldValue = storedTokens.get();
        long newValue = oldValue;

        // 添加令牌的判断前提条件:
        // 当令牌的消耗程度远远低于警戒线的时候
        if (oldValue < warningToken) {//如果老令牌小于警戒令牌，新令牌=(当前时间-上次填充时间/1000(即转为秒))*count（阈值count）+老令牌
            newValue = (long)(oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
        } else if (oldValue > warningToken) {//如果老令牌大于等于警戒令牌，上个时间窗的qps如果小于(count/冷却因子)。新令牌=同样按照小于警戒令牌的算法计算。如果大于等于则不再增加新令牌
            if (passQps < (int)count / coldFactor) {
                newValue = (long)(oldValue + (currentTime - lastFilledTime.get()) * count / 1000);
            }
        }
        return Math.min(newValue, maxToken);//返回新令牌与最大令牌两者之间的最小值
    }

}
