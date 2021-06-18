package com.stillcoolme.flink.hotitem;


import com.stillcoolme.flink.hotitem.entity.hotpage.ApacheLogEvent;
import com.stillcoolme.flink.hotitem.entity.hotpage.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.net.URL;
import java.text.SimpleDateFormat;

public class HotPages {

    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {  // 设置可容忍一分钟延迟
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getTimestamp();
            }
        });

        // 分组开窗
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(data -> "GET".equals(data.getMethod()))
                .keyBy(ApacheLogEvent::getUrl)
                .timeWindow(Time.hours(1), Time.minutes(5))
                .allowedLateness(Time.minutes(1))
                .aggregate(new PageCountAgg(), new PageCountRsult());

        // 收集同一窗口数据
        SingleOutputStreamOperator hotPageResult = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPage(5));

        env.execute("hot page job");
    }

    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a1, Long a2) {
            return a1 + a2;
        }
    }
}
