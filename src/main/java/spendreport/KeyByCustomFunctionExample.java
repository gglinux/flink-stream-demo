package spendreport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyByCustomFunctionExample {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define data source
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

        // Process data stream
        DataStream<Tuple2<String, Integer>> resultStream = dataStream
                .flatMap(new Splitter())
                .keyBy(new CustomKeySelector()) // 使用自定义 KeySelector 进行分组
                .reduce(new Reducer());

        // Print the result
        resultStream.print();

        // Execute the Flink program
        env.execute("KeyBy Custom Function Example");
    }

    // 自定义 KeySelector 类，根据元素的第一个字段进行分组
    public static class CustomKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    // 自定义 FlatMapFunction，将逗号分隔的元素处理成键值元组
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.split(",");
            for (String token : tokens) {
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }

    // 自定义 ReduceFunction，对具有相同键的元素的值进行累加
    public static class Reducer implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }
}
