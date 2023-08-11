package spendreport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 添加数据源，这里使用Socket数据源，监听本地9999端口
        DataStream<String> textStream = env.socketTextStream("localhost", 9999);

        // 3. 数据处理：将每行文本拆分成单词，并计算每个单词的数量
        DataStream<Tuple2<String, Integer>> wordCountStream = textStream
                .flatMap(new Tokenizer()) // 拆分单词
                .keyBy(0) // 按单词分组
                .sum(1); // 计算每个单词的数量

        // 4. 输出结果到控制台
        wordCountStream.print();

        // 5. 启动Flink流处理应用程序
        env.execute("Word Count Example");
    }

    // 自定义FlatMapFunction，用于拆分单词
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 使用空格拆分每行文本
            String[] tokens = value.toLowerCase().split("\\s+");

            // 输出每个单词及其数量（初始数量为1）
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
