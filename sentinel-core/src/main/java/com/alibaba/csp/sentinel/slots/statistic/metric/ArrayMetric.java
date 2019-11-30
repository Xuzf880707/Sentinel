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
package com.alibaba.csp.sentinel.slots.statistic.metric;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.csp.sentinel.Constants;
import com.alibaba.csp.sentinel.node.metric.MetricNode;
import com.alibaba.csp.sentinel.slots.statistic.MetricEvent;
import com.alibaba.csp.sentinel.slots.statistic.base.LeapArray;
import com.alibaba.csp.sentinel.slots.statistic.data.MetricBucket;
import com.alibaba.csp.sentinel.slots.statistic.base.WindowWrap;
import com.alibaba.csp.sentinel.slots.statistic.metric.occupy.OccupiableBucketLeapArray;

/**
 * The basic metric class in Sentinel using a {@link BucketLeapArray} internal.
 *
 * @author jialiang.linjl
 * @author Eric Zhao
 */
public class ArrayMetric implements Metric {

    private final LeapArray<MetricBucket> data;

    public ArrayMetric(int sampleCount, int intervalInMs) {
        this.data = new OccupiableBucketLeapArray(sampleCount, intervalInMs);
    }

    public ArrayMetric(int sampleCount, int intervalInMs, boolean enableOccupy) {
        if (enableOccupy) {
            this.data = new OccupiableBucketLeapArray(sampleCount, intervalInMs);
        } else {
            this.data = new BucketLeapArray(sampleCount, intervalInMs);
        }
    }

    /**
     * For unit test.
     */
    public ArrayMetric(LeapArray<MetricBucket> array) {
        this.data = array;
    }
    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口（也就是采样时间，默认是1s）的值并记录下单个窗口最大的成功数
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long success() {
        data.currentWindow();
        long success = 0;

        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            success += window.success();
        }
        return success;
    }
    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计（也就是采样时间，默认是1s）
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long maxSuccess() {
        data.currentWindow();
        long success = 0;

        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            if (window.success() > success) {
                success = window.success();
            }
        }
        return Math.max(success, 1);
    }
    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计（也就是采样时间，默认是1s）
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long exception() {
        data.currentWindow();
        long exception = 0;
        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            exception += window.exception();
        }
        return exception;
    }

    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计（也就是采样时间，默认是1s）
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long block() {
        data.currentWindow();
        long block = 0;
        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            block += window.block();
        }
        return block;
    }

    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计（也就是采样时间，默认是1s）
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long pass() {
        //TODO ?这一步只是为了重置更新当前时间对应的窗口，然后避免拿到旧的值吗？但是data.values()已经做了同样的功能了啊？
        //所以这一步真正目的是否是为了更新当前对应的时间窗口？
        data.currentWindow();
        long pass = 0;
        List<MetricBucket> list = data.values();

        for (MetricBucket window : list) {
            pass += window.pass();
        }
        return pass;
    }
    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计（也就是采样时间，默认是1s）
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long occupiedPass() {
        data.currentWindow();
        long pass = 0;
        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            pass += window.occupiedPass();
        }
        return pass;
    }
    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口的值的累计（也就是采样时间，默认是1s）
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long rt() {
        data.currentWindow();
        long rt = 0;
        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            rt += window.rt();
        }
        return rt;
    }
    /***
     * 1、更新滑动窗口的最新信息，这里会根据当前时间，丢弃秒级metric中过期的时间窗口
     *          (通过当前时间计算在窗口数组中所处的索引，然后检查索引对应的窗口，判断当前时间是否在该窗口时间区间，如果不是则表示旧的窗口，直接丢弃该窗口，并维护一个新的时间窗口)
     * 2、根据当前获得当前整个滑动窗口数组中的所有的窗口（也就是采样时间，默认是1s）的平均响应时间最小的窗口，并获得最小的响应时间
     *      （这里面会剔除过期的滑动窗口，也就是那些开始时间距离当前时间点超过整个滑动窗口数组的采样时间intervalInMs的滑动窗口会被排除）
     * @return
     */
    @Override
    public long minRt() {
        data.currentWindow();
        long rt = Constants.TIME_DROP_VALVE;
        List<MetricBucket> list = data.values();
        for (MetricBucket window : list) {
            if (window.minRt() < rt) {
                rt = window.minRt();
            }
        }

        return Math.max(1, rt);
    }

    @Override
    public List<MetricNode> details() {
        List<MetricNode> details = new ArrayList<MetricNode>();
        data.currentWindow();
        List<WindowWrap<MetricBucket>> list = data.list();
        for (WindowWrap<MetricBucket> window : list) {
            if (window == null) {
                continue;
            }
            MetricNode node = new MetricNode();
            node.setBlockQps(window.value().block());
            node.setExceptionQps(window.value().exception());
            node.setPassQps(window.value().pass());
            long successQps = window.value().success();
            node.setSuccessQps(successQps);
            if (successQps != 0) {
                node.setRt(window.value().rt() / successQps);
            } else {
                node.setRt(window.value().rt());
            }
            node.setTimestamp(window.windowStart());
            node.setOccupiedPassQps(window.value().occupiedPass());

            details.add(node);
        }

        return details;
    }

    @Override
    public MetricBucket[] windows() {
        data.currentWindow();
        return data.values().toArray(new MetricBucket[0]);
    }

    @Override
    public void addException(int count) {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        wrap.value().addException(count);
    }

    @Override
    public void addBlock(int count) {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        wrap.value().addBlock(count);
    }

    @Override
    public void addWaiting(long time, int acquireCount) {
        data.addWaiting(time, acquireCount);
    }

    @Override
    public void addOccupiedPass(int acquireCount) {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        wrap.value().addOccupiedPass(acquireCount);
    }

    @Override
    public void addSuccess(int count) {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        wrap.value().addSuccess(count);
    }

    @Override
    public void addPass(int count) {
        //将LeapArray的当前滑动窗口包装成一个WindowWrap
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        wrap.value().addPass(count);//执行包装的滑动窗口WindowWrap的addPass方法
    }

    @Override
    public void addRT(long rt) {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        wrap.value().addRT(rt);
    }

    @Override
    public void debug() {
        data.debug(System.currentTimeMillis());
    }

    /***
     * 获得前一个时间窗口的统计信息
     *      1、如果当前时间对应的前一个时间窗口已经不存在，则返回0
     *      2、获得前一个时间窗口的阻塞数
     *      注意：
     *          默认的对于分钟级的metric，每个窗口跨度是1s。
     *          默认的对于秒级的metric，每个窗口跨度是500ms
     * @return
     */
    @Override
    public long previousWindowBlock() {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        //根据当前时间获得前一个窗口的统计信息
        //  默认的对于分钟级的metric，每个窗口跨度是1s。
        //  默认的对于秒级的metric，每个窗口跨度是500ms
        wrap = data.getPreviousWindow();
        //如果前一个时间窗口不存在，则返回0
        if (wrap == null) {
            return 0;
        }
        return wrap.value().block();
    }

    @Override
    public long previousWindowPass() {
        WindowWrap<MetricBucket> wrap = data.currentWindow();
        //根据当前时间获得前一个窗口的统计信息
        //  默认的对于分钟级的metric，每个窗口跨度是1s。
        //  默认的对于秒级的metric，每个窗口跨度是500ms
        wrap = data.getPreviousWindow();
        if (wrap == null) {
            return 0;
        }
        return wrap.value().pass();
    }

    public void add(MetricEvent event, long count) {
        data.currentWindow().value().add(event, count);
    }

    public long getCurrentCount(MetricEvent event) {
        return data.currentWindow().value().get(event);
    }

    /**
     * Get total sum for provided event in {@code intervalInSec}.
     *
     * @param event event to calculate
     * @return total sum for event
     */
    public long getSum(MetricEvent event) {
        data.currentWindow();
        long sum = 0;

        List<MetricBucket> buckets = data.values();
        for (MetricBucket bucket : buckets) {
            sum += bucket.get(event);
        }
        return sum;
    }

    /**
     * Get average count for provided event per second.
     *
     * @param event event to calculate
     * @return average count per second for event
     */
    public double getAvg(MetricEvent event) {
        return getSum(event) / data.getIntervalInSecond();
    }

    @Override
    public long getWindowPass(long timeMillis) {
        MetricBucket bucket = data.getWindowValue(timeMillis);
        if (bucket == null) {
            return 0L;
        }
        return bucket.pass();
    }

    @Override
    public long waiting() {
        return data.currentWaiting();
    }

    @Override
    public double getWindowIntervalInSec() {
        return data.getIntervalInSecond();
    }

    @Override
    public int getSampleCount() {
        return data.getSampleCount();
    }
}
