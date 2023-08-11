package spendreport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class WatermarkExample {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define data source
        DataStream<String> rawStream = env.socketTextStream("localhost", 9999);

        // Parse raw events into ClickEvent objects and assign watermarks
        DataStream<ClickEvent> clickEvents = rawStream.map(new ClickEventParser())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        // Process the click events
        SingleOutputStreamOperator<ClickEvent> resultStream = clickEvents
                .keyBy(ClickEvent -> ClickEvent.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("count");

        // Print the result
        resultStream.print();

        // Execute the Flink program
        env.execute("Watermark Example");
    }

    // ClickEvent class
    public static class ClickEvent {
        public final long userId;
        public final long timestamp;
        public final String url;
        public final int count;

        // Constructor and getters ...
        public ClickEvent(long userId, long timestamp, String url, int count) {
            this.count = count;
            this.url = url;
            this.userId = userId;
            this.timestamp =timestamp;
        }


        public static long getUserId(ClickEvent clickEvent) {
            return clickEvent.userId;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    // Custom MapFunction to parse raw events into ClickEvent objects
    public static class ClickEventParser implements MapFunction<String, ClickEvent> {
        @Override
        public ClickEvent map(String value) {
            String[] fields = value.split(",");
            long userId = Long.parseLong(fields[0]);
            long timestamp = Long.parseLong(fields[1]); // Assume timestamp is in milliseconds
            String url = fields[2];
            int count = 1;
            return new ClickEvent(userId, timestamp, url, count);
        }
    }
}
