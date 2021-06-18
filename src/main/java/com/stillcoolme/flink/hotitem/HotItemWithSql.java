package com.stillcoolme.flink.hotitem;

import com.stillcoolme.flink.hotitem.entity.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemWithSql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建表执行环境，用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> inputStream = env.readTextFile("/Users/stillcoolme/Documents/ideaProject/myself/flink-uba/src/main/resources/UserBehavior.csv");
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // ==============  table api 来玩   ===============
        // 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");
        // 分组开窗
        Table windowAggTable = dataTable
                .filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId, w")
                .select("itemId, w.end as windowEnd, itemId.count as cnt");

        // 利用开窗函数，对count值进行排序并获取Row number，得到Top N
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg", aggStream, "itemId, windowEnd, cnt");
        String hotItemSql = "select * from" +
                "(select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num from agg) temp " +
                "where row_num <= 5";
        Table resultTable = tableEnv.sqlQuery(hotItemSql);


        // ============== sql执行 ================
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");
        hotItemSql = "select * from (" +
                "select *, ROW_NUMBER() OVER (partition by windowEnd order by cnt desc) as row_num from" +
                "(" +
                    "select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd from data_table" +
                    " where behavior = 'pv' group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                ") temp1" +
                ") where row_num <= 5";
        Table resultSqlTable = tableEnv.sqlQuery(hotItemSql);


        // 这里用了个 toRetractStream
//        tableEnv.toRetractStream(resultTable, Row.class).print();
        tableEnv.toRetractStream(resultSqlTable, Row.class).print();
        env.execute("hot items with sql job");

    }
}
