package com.stillcoolme.flink.orderdetect;

import com.stillcoolme.flink.orderdetect.entity.OrderEvent;
import com.stillcoolme.flink.orderdetect.entity.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 * @author stillcoolme
 * @version 1.0.0
 * @createTime 2021-09-08 19:39:00
 * @Description
 *  之前是按照CEP的步骤：先定义Pattern，然后将Pattern应用到流上去，然后select方法去提取，如果有超时时间再定义一个侧输出流标签。
 *
 *  这次我们就用一个逻辑判断：定义状态，再加个定时器如果到点了pay还没来就设置为超时，，用到 状态 和 定时器 那么就用 自定义函数KeyedProcessFunction
 *  来了一个crate事件，就注册一个15分钟的定时器，到点了还没有定时器就超时
 */
public class OrderPayTimeoutWithoutCEP {
    // 定义超时事件的侧输出流标签
    private static final OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 读取数据并转换成POJO类型
        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        DataStream<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(new Long(fields[0]), fields[1], fields[2], new Long(fields[3]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });


        // 自定义处理函数，主流输出正常匹配订单事件，测流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream
                .keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());


        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");

        env.execute("order timeout detect without cep job");

    }

    // 实现自定义KeyedProcessFunction
    private static class OrderPayMatchDetect extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        // 来了一个pay订单事件，需要记住之前有没有create订单状态
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;
        // 定义状态，保存定时器时间戳，用来定时器完成了要用来删除定时器
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed", Boolean.class, false));
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-create", Boolean.class, false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            // 判断当前事件类型
            if ("create".equals(value.getEventType())) {
                // 1. 如果来的是create，要判断是否支付过。这种是乱序数据，pay先来的
                if (isPayed) {
                    // 1.1 如果已经正常支付，输出正常匹配结果
                    out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    // 清空状态，删除定时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 1.2 如果没有支付过，注册15分钟后的定时器，开始等待支付事件
                    Long ts = (value.getTimestamp() + 15 * 60) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    // 更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);
                }
            } else if ("pay".equals(value.getEventType())) {
                // 2. 如果来的是pay，要判断是否有下单事件来过
                if (isCreated) {
                    // 2.1 已经有过下单事件，要继续判断支付的时间戳是否超过15分钟
                    if (value.getTimestamp() * 1000L < timerTs) {
                        // 2.1.1 在15分钟内，没有超时，正常匹配输出
                        out.collect(new OrderResult(value.getOrderId(), "payed successfully"));
                    } else {
                        // 2.1.2 已经超时，输出侧输出流报警
                        ctx.output(orderTimeoutTag, new OrderResult(value.getOrderId(), "payed but already timeout"));
                    }
                    // 统一清空状态
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                } else {
                    // 2.2 没有下单事件，乱序数据，注册一个定时器，然后等待下单事件
                    ctx.timerService().registerEventTimeTimer(value.getTimestamp() * 1000L);
                    // 更新状态
                    timerTsState.update(value.getTimestamp() * 1000L);
                    isPayedState.update(true);
                }
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            // 定时器触发，说明一定有一个事件没来
            if (isPayedState.value()) {
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "payed but not found created"));
            } else {
                // 如果pay没来，支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(), "timeout"));
            }
            // 清空状态
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();

        }


    }
}
