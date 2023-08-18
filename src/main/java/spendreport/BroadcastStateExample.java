package spendreport;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入流
        DataStream<String> inputStream = env.fromElements("A", "B", "C");

        // 规则流
        DataStream<String> ruleStream = env.fromElements("rule1", "rule2");

        // 定义BroadcastState描述器
        MapStateDescriptor<String, String> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        // 将规则流广播并创建BroadcastStream
        BroadcastStream<String> broadcastRuleStream = ruleStream.broadcast(ruleStateDescriptor);

        // 使用KeyedBroadcastProcessFunction连接输入流和广播流
        DataStream<String> resultStream = inputStream
                .keyBy(x -> x)
                .connect(broadcastRuleStream)
                .process(new KeyedBroadcastProcessFunction<String, String, String, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        // 从BroadcastState中获取规则
                        String rule = ctx.getBroadcastState(ruleStateDescriptor).get("rule");

                        // 使用规则处理输入数据
                        out.collect("Input: " + value + ", Rule: " + rule);
                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                        // 更新BroadcastState中的规则
                        ctx.getBroadcastState(ruleStateDescriptor).put("rule", value);
                    }
                });

        resultStream.print();

        env.execute("BroadcastState Example");
    }
}