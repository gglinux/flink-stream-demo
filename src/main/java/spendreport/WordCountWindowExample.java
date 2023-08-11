package spendreport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountWindowExample {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        // Define data source
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

        // Process data stream
        DataStream<Tuple2<String, Integer>> resultStream = dataStream
                .flatMap(new Splitter())
                .keyBy(0) // Group data elements by key (field index 0)
                .timeWindow(Time.seconds(5)) // Define a time-based window of length 5 seconds
                .reduce(new Reducer());

        // Print the result
        resultStream.print();

        // Execute the Flink program
        env.execute("Word Count Window Example");
    }

    // Define a custom FlatMapFunction to split input sentences into individual words
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            Arrays.stream(value.split("\\W+")).forEach(word -> out.collect(new Tuple2<>(word, 1)));
        }
    }

    // Define a custom ReduceFunction to aggregate the value by key
    public static class Reducer implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }
}