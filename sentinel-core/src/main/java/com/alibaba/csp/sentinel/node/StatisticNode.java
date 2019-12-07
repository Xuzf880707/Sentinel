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
package com.alibaba.csp.sentinel.node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.csp.sentinel.util.TimeUtil;
import com.alibaba.csp.sentinel.node.metric.MetricNode;
import com.alibaba.csp.sentinel.slots.statistic.metric.ArrayMetric;
import com.alibaba.csp.sentinel.slots.statistic.metric.Metric;

/**
 * 三种实时统计指标
 * <p>The statistic node keep three kinds of real-time statistics metrics:</p>
 * <ol>
 * <li>metrics in second level ({@code rollingCounterInSecond})</li> 秒级滑动窗口
 * <li>metrics in minute level ({@code rollingCounterInMinute})</li> 分钟级滑动窗口
 * <li>thread count</li> 线程数
 * </ol>
 *
 * <p>
 * Sentinel use sliding window to record and count the resource statistics in real-time.
 * The sliding window infrastructure behind the {@link ArrayMetric} is {@code LeapArray}.
 * </p>
 *
 * <p>
 *     当地一个请求过来的适合，Sentinel会闯建一个新的一个时间跨度内窗口用于存储运行时的统计信息，比如响应时间、QPS、阻塞请求等
 * case 1: When the first request comes in, Sentinel will create a new window bucket of
 * a specified time-span to store running statics, such as total response time(rt),
 * incoming request(QPS), block request(bq), etc. And the time-span is defined by sample count.
 * </p>
 * <pre>
 * 	0      100ms
 *  +-------+--→ Sliding Windows
 * 	    ^
 * 	    |
 * 	  request
 * </pre>
 * <p>
 * Sentinel use the statics of the valid buckets to decide whether this request can be passed.
 * For example, if a rule defines that only 100 requests can be passed,
 * it will sum all qps in valid buckets, and compare it to the threshold defined in rule.
 * </p>
 *
 * <p>case 2: continuous requests</p>
 * <pre>
 *  0    100ms    200ms    300ms
 *  +-------+-------+-------+-----→ Sliding Windows
 *                      ^
 *                      |
 *                   request
 * </pre>
 *
 * <p>case 3: requests keeps coming, and previous buckets become invalid</p>
 * <pre>
 *  0    100ms    200ms	  800ms	   900ms  1000ms    1300ms
 *  +-------+-------+ ...... +-------+-------+ ...... +-------+-----→ Sliding Windows
 *                                                      ^
 *                                                      |
 *                                                    request
 * </pre>
 *
 * <p>The sliding window should become:</p>
 * <pre>
 * 300ms     800ms  900ms  1000ms  1300ms
 *  + ...... +-------+ ...... +-------+-----→ Sliding Windows
 *                                                      ^
 *                                                      |
 *                                                    request
 * </pre>
 *
 * @author qinan.qn
 * @author jialiang.linjl
 * 所有统计节点的父类，用于执行具体的资源统计操作
 */
public class StatisticNode implements Node {

    /**
     * Holds statistics of the recent {@code INTERVAL} seconds. The {@code INTERVAL} is divided into time spans
     * by given {@code sampleCount}.
     * 秒级
     */
    private transient volatile Metric rollingCounterInSecond = new ArrayMetric(SampleCountProperty.SAMPLE_COUNT,
        IntervalProperty.INTERVAL);

    /**
     * Holds statistics of the recent 60 seconds. The windowLengthInMs is deliberately set to 1000 milliseconds,
     * meaning each bucket per second, in this way we can get accurate statistics of each second.
     * 创建一个分钟级的时间窗口数组，每个时间窗口长度是1s(注意，如果因为是1s,所以可以从分钟级里获取当前系统的前1s的各种统计信息)
     */
    private transient Metric rollingCounterInMinute = new ArrayMetric(60, 60 * 1000, false);

    /**
     * The counter for thread count.
     */
    private AtomicInteger curThreadNum = new AtomicInteger(0);

    /**
     * The last timestamp when metrics were fetched.
     */
    private long lastFetchTime = -1;

    @Override
    public Map<Long, MetricNode> metrics() {
        // The fetch operation is thread-safe under a single-thread scheduler pool.
        long currentTime = TimeUtil.currentTimeMillis();
        currentTime = currentTime - currentTime % 1000;
        Map<Long, MetricNode> metrics = new ConcurrentHashMap<>();
        List<MetricNode> nodesOfEverySecond = rollingCounterInMinute.details();
        long newLastFetchTime = lastFetchTime;
        // Iterate metrics of all resources, filter valid metrics (not-empty and up-to-date).
        for (MetricNode node : nodesOfEverySecond) {
            if (isNodeInTime(node, currentTime) && isValidMetricNode(node)) {
                metrics.put(node.getTimestamp(), node);
                newLastFetchTime = Math.max(newLastFetchTime, node.getTimestamp());
            }
        }
        lastFetchTime = newLastFetchTime;

        return metrics;
    }

    private boolean isNodeInTime(MetricNode node, long currentTime) {
        return node.getTimestamp() > lastFetchTime && node.getTimestamp() < currentTime;
    }

    private boolean isValidMetricNode(MetricNode node) {
        return node.getPassQps() > 0 || node.getBlockQps() > 0 || node.getSuccessQps() > 0
            || node.getExceptionQps() > 0 || node.getRt() > 0 || node.getOccupiedPassQps() > 0;
    }

    @Override
    public void reset() {
        rollingCounterInSecond = new ArrayMetric(SampleCountProperty.SAMPLE_COUNT, IntervalProperty.INTERVAL);
    }

    /***
     * 分钟级统计当前滑动窗口中总的请求数=通过的pass+阻塞的block
     * @return
     */
    @Override
    public long totalRequest() {
        long totalRequest = rollingCounterInMinute.pass() + rollingCounterInMinute.block();
        return totalRequest;
    }
    /***
     * 分钟级统计当前滑动窗口中总的阻塞数=阻塞的block
     * @return
     */
    @Override
    public long blockRequest() {
        return rollingCounterInMinute.block();
    }
    /***
     * 秒级统计当前滑动窗口中总的阻塞QPS=阻塞的block数/滑动窗口的大小(默认是1000ms)
     * @return
     */
    @Override
    public double blockQps() {
        return rollingCounterInSecond.block() / rollingCounterInSecond.getWindowIntervalInSec();
    }
    /***
     * 获得前1s的QPS(我们知道分钟级的rollingCounterInMinute的每个窗口大小是1s，所以我们只要根据当前时间在rollingCounterInMinute中获得前一个窗口的统计信息即可)
     *      1、如果根据rollingCounterInMinute获得当前时间对应的前1s时间窗口已经不存在，则返回0
     *      2、如果根据rollingCounterInMinute获得当前时间对应的前1s时间窗口存在，则直接获得前一个时间窗口的阻塞数
     *      注意：
     *              rollingCounterInMinute默认每一个窗口数组长度是1s,所以前一秒的通过的个数就是Block QPS，整个窗口数组里面有60个时间窗口。
     *              rollingCounterInSecond默认每一个窗口数组长度是500ms,里面有2个时间窗口
     * @return
     */
    @Override
    public double previousBlockQps() {
        return this.rollingCounterInMinute.previousWindowBlock();
    }
    /***
     * 获得前1s的QPS(我们知道分钟级的rollingCounterInMinute的每个窗口大小是1s，所以我们只要根据当前时间在rollingCounterInMinute中获得前一个窗口的统计信息即可)
     *      1、如果根据rollingCounterInMinute获得当前时间对应的前1s时间窗口已经不存在，则返回0
     *      2、如果根据rollingCounterInMinute获得当前时间对应的前1s时间窗口存在，则直接获得前一个时间窗口的通过的个数
     *      注意：
     *              rollingCounterInMinute默认每一个窗口数组长度是1s,所以前一秒的通过的个数就是Pass QPS，整个窗口数组里面有60个时间窗口。
     *              rollingCounterInSecond默认每一个窗口数组长度是500ms,里面有2个时间窗口
     * @return
     */
    @Override
    public double previousPassQps() {
        return this.rollingCounterInMinute.previousWindowPass();
    }

    @Override
    public double totalQps() {
        return passQps() + blockQps();
    }
    /***
     * 获得分钟级内总的成功数
     * 根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计，不会统计其中和当前时间跨度超过一整个时间窗口数组的跨度的旧的窗口
     * @return
     */
    @Override
    public long totalSuccess() {
        return rollingCounterInMinute.success();
    }
    /***
     *
     * @return
     *      从秒级的metric中获取两个指标：
     *          当前时间所处的采样时间段(默认是跨度两个滑动窗口）的统计值(默认每个滑动窗口是500ms)
     *          采样的时间跨度(单位秒，这里默认是1s)
     *     这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除
     */
    @Override
    public double exceptionQps() {
        return rollingCounterInSecond.exception() / rollingCounterInSecond.getWindowIntervalInSec();
    }
    /***
     * 获得分钟级内总的异常数
     * 根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计，不会统计其中和当前时间跨度超过一整个时间窗口数组的跨度的旧的窗口
     * @return
     */
    @Override
    public long totalException() {
        return rollingCounterInMinute.exception();
    }

    /***
     *
     * @return
     *      从秒级的metric中获取两个指标：
     *          当前时间所处的采样时间段(默认是跨度两个滑动窗口）的统计值(默认每个滑动窗口是500ms)
     *          采样的时间跨度(单位秒，这里默认是1s)
     *     这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除
     */
    @Override
    public double passQps() {
        //rollingCounterInSecond.pass()：当前时间所处的采样时间的的psss 数的统计值(默认是2个滑动窗口是1000ms)
        //rollingCounterInSecond.getWindowIntervalInSec():每个采样时间的跨度，默认是1s
        return rollingCounterInSecond.pass() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    /***
     * 获得分钟级内总的通过的请求数
     * 根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计，不会统计其中和当前时间跨度超过一整个时间窗口数组的跨度的旧的窗口
     * @return
     */
    @Override
    public long totalPass() {
        return rollingCounterInMinute.pass();
    }
    /***
     *
     * @return
     *      从秒级的metric中获取两个指标：
     *          当前时间所处的采样时间段(默认是跨度两个滑动窗口）的统计值(默认每个滑动窗口是500ms)
     *          采样的时间跨度(单位秒，这里默认是1s)
     *     这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除
     */
    @Override
    public double successQps() {
        /***
         * rollingCounterInSecond.success()：当前时间所处的滑动窗口的psss 数的统计值(默认滑动窗口是500ms)
         * rollingCounterInSecond.getWindowIntervalInSec():每个滑动时间窗的时间跨度，默认是0.5s
         */
        return rollingCounterInSecond.success() / rollingCounterInSecond.getWindowIntervalInSec();
    }
    /***
     *
     * @return
     *      从秒级的metric中获取两个指标：
     *          当前时间所处的采样时间段(默认是跨度两个滑动窗口）的统计值(默认每个滑动窗口是500ms)
     *          采样的时间跨度(单位秒，这里默认是1s)
     *     这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除
     */
    @Override
    public double maxSuccessQps() {
        /***
         * rollingCounterInSecond.success()：单个每个采样时间中请求成功的最大值的滑动窗口(默认一个采样时间是2个滑动窗口，取较大那个)
         * rollingCounterInSecond.getSampleCount(): 秒级metric的窗口数
         */
        return rollingCounterInSecond.maxSuccess() * rollingCounterInSecond.getSampleCount();
    }
    /***
     *
     * @return
     *      从秒级的metric中获取两个指标：
     *          当前时间所处的采样时间段(默认是跨度两个滑动窗口）的统计值(默认每个滑动窗口是500ms)
     *          采样的时间跨度(单位秒，这里默认是1s)
     *     这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除
     */
    @Override
    public double occupiedPassQps() {
        return rollingCounterInSecond.occupiedPass() / rollingCounterInSecond.getWindowIntervalInSec();
    }

    /****
     * 获得平均响应时间=整个滑动窗口数组中所有元素的的rt总和/成功数总和
     * （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public double avgRt() {
        long successCount = rollingCounterInSecond.success();
        if (successCount == 0) {
            return 0;
        }

        return rollingCounterInSecond.rt() * 1.0 / successCount;
    }

    @Override
    public double minRt() {
        return rollingCounterInSecond.minRt();
    }

    @Override
    public int curThreadNum() {
        return curThreadNum.get();
    }

    @Override
    public void addPassRequest(int count) {
        rollingCounterInSecond.addPass(count);
        rollingCounterInMinute.addPass(count);
    }

    @Override
    public void addRtAndSuccess(long rt, int successCount) {
        rollingCounterInSecond.addSuccess(successCount);//记录秒级的执行成功exit的数量
        rollingCounterInSecond.addRT(rt);//记录同一秒内的请求数的总的响应时间

        rollingCounterInMinute.addSuccess(successCount);
        rollingCounterInMinute.addRT(rt);
    }

    /***
     * 维护秒级滑动窗口统计的'block'的数量+count
     * 维护分钟级滑动窗口统计的'block'的数量+count
     * @param count count to add
     */
    @Override
    public void increaseBlockQps(int count) {
        rollingCounterInSecond.addBlock(count);
        rollingCounterInMinute.addBlock(count);
    }
    /***
     * 维护秒级滑动窗口统计的'Exception'的数量+count
     * 维护分钟级滑动窗口统计的'Exception'的数量+count
     * @param count count to add
     */
    @Override
    public void increaseExceptionQps(int count) {
        rollingCounterInSecond.addException(count);
        rollingCounterInMinute.addException(count);
    }
    /***
     * 维护秒级滑动窗口统计的'Thread'的数量+1
     * 维护分钟级滑动窗口统计的'Thread'的数量+1
     */
    @Override
    public void increaseThreadNum() {
        curThreadNum.incrementAndGet();
    }
    /***
     * 维护秒级滑动窗口统计的'Thread'的数量-1
     * 维护分钟级滑动窗口统计的'Thread'的数量-1
     */
    @Override
    public void decreaseThreadNum() {
        curThreadNum.decrementAndGet();
    }

    @Override
    public void debug() {
        rollingCounterInSecond.debug();
    }

    /***
     *
     *  返回需要等待的窗口数，才能凑够需要的 acquireCount，一个窗口一个窗口的累加，一直累加到够的情况下。否则就等待配置的时间
     * @param currentTime  current time millis. 当前时间
     * @param acquireCount tokens count to acquire. 需要获取的token数
     * @param threshold    qps threshold. 当前限流规则的流量阈值
     * @return
     */
    @Override
    public long tryOccupyNext(long currentTime, int acquireCount, double threshold) {
        //maxCount = qps阈值 * 采样时间(获得一个采样窗口里所能容纳的最大的查询请求资源数)
        double maxCount = threshold * IntervalProperty.INTERVAL / 1000;//10*1000/1000
        //获得当前当前采样时间中正在等待的请求数
        long currentBorrow = rollingCounterInSecond.waiting();//0
        //如果正在等待的请求数已经超过整个滑动窗口的阈值，则直接拒绝当前请求
        if (currentBorrow >= maxCount) {//返回超时等待的时间
            return OccupyTimeoutProperty.getOccupyTimeout();
        }
        //当前采样时间内每个窗口的时间长度
        int windowLength = IntervalProperty.INTERVAL / SampleCountProperty.SAMPLE_COUNT;//默认是500ms
        //整个方法最终结果是想得到一个等待的时间，但是该等待时间也是有最大限制的。
        //earliestTime=当前时间所处的窗口的结束时间-采样时间。相当于当前窗口作为某个采样时间的的最后一个窗口了，则这个采样的时间开始时间是earliestTime
        //currentTime： 1575266347363
        //earliestTime：1575266346500
        //currentTime-IntervalProperty.INTERVAL=（earliestTime+currentTime % windowLength-currentTime - windowLength）
        long earliestTime = (currentTime - currentTime % windowLength + windowLength) - IntervalProperty.INTERVAL;//

        int idx = 0;
        /*
         * Note: here {@code currentPass} may be less than it really is NOW, because time difference
         * since call rollingCounterInSecond.pass(). So in high concurrency, the following code may
         * lead more tokens be borrowed.
         */
        //当前采样时间已通过的请求数
        long currentPass = rollingCounterInSecond.pass();
        while (earliestTime < currentTime) {
            long waitInMs = idx * windowLength + windowLength - currentTime % windowLength;//137
            if (waitInMs >= OccupyTimeoutProperty.getOccupyTimeout()) {
                break;
            }//计算earliestTime所在窗口的通过数（默认是上一个窗口的pass）
            long windowPass = rollingCounterInSecond.getWindowPass(earliestTime);
            //当前采样时间通过的请求数+当前采样时间等待的请求数+本次请求数-上一次窗口的请求=当前窗口的请求数<采样窗口的请求上限
            if (currentPass + currentBorrow + acquireCount - windowPass <= maxCount) {//如果当前窗口通过的数目+当前请求的数目-上一个窗口的请求数
                return waitInMs;
            }//继续往后推
            earliestTime += windowLength;
            currentPass -= windowPass;
            idx++;
        }

        return OccupyTimeoutProperty.getOccupyTimeout();
    }

    @Override
    public long waiting() {
        return rollingCounterInSecond.waiting();
    }

    @Override
    public void addWaitingRequest(long futureTime, int acquireCount) {
        rollingCounterInSecond.addWaiting(futureTime, acquireCount);
    }

    @Override
    public void addOccupiedPass(int acquireCount) {
        rollingCounterInMinute.addOccupiedPass(acquireCount);
        rollingCounterInMinute.addPass(acquireCount);
    }
}
