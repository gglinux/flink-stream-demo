package spendreport;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFunctionExample {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define data source
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

        // Process data stream
        DataStream<Integer> resultStream = dataStream.map(new CustomRichMapFunction());

        // Print the result
        resultStream.print();

        // Execute the Flink program
        env.execute("Rich Function Example");
    }

    // 自定义的 RichMapFunction，将输入字符串转换为大写并返回字符串长度
    public static class CustomRichMapFunction extends RichMapFunction<String, Integer> {

        @Override
        public void open(Configuration parameters) {
            // 在 Function 实例中执行设置工作
            System.out.println("CustomRichMapFunction open");
        }

        @Override
        public Integer map(String value) {
            return value.toUpperCase().length();
        }

        @Override
        public void close() {
            // 在 Function 实例不再可用时执行清理
            System.out.println("CustomRichMapFunction close");
        }
    }
}