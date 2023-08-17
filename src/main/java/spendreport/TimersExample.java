package spendreport;

/**
 * @className: TimersExample
 * @description: TODO 类描述
 * @author: jiaweiguo
 * @date: 2023/8/9
 *
 * 将输入数据转换为UserVisit对象，并根据userId进行分组。
 * 接下来，我们使用VisitCountWithTimerProcessFunction处理函数来计算每个用户的访问次数，并在访问次数达到阈值时输出警告信息。
 * 同时，我们还会使用定时器在访问次数达到阈值后的10秒内，如果没有新的访问事件，则重置计数。
 * VisitCountWithTimerProcessFunction继承自KeyedProcessFunction，并实现了processElement和onTimer方法。
 * 在processElement方法中，我们使用ValueState来存储和更新用户的访问次数，并使用定时器来设置重置计数的时间。
 * 在onTimer方法中，我们根据定时器的触发时间来重置访问。
 *
 **/
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimersExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.socketTextStream("localhost", 9999);

        DataStream<UserVisit> visits = inputStream.map(new MapFunction<String, UserVisit>() {
            @Override
            public UserVisit map(String value) throws Exception {
                String[] fields = value.split(",");
                return new UserVisit(Integer.parseInt(fields[0]), fields[1]);
            }
        });

        visits.keyBy(visit -> visit.userId)
                .process(new VisitCountWithTimerProcessFunction(3))
                .print();

        env.execute("Timers Example");
    }

    public static class UserVisit {
        public int userId;
        public String url;

        public UserVisit(int userId, String url) {
            this.userId = userId;
            this.url = url;
        }

        @Override
        public String toString() {
            return "UserVisit{userId=" + userId + ", url='" + url + "'}";
        }
    }

    public static class VisitCountWithTimerProcessFunction extends KeyedProcessFunction<Integer, UserVisit, String> {
        private final int threshold;
        private ValueState<Integer> countState;
        private ValueState<Long> timerState;

        public VisitCountWithTimerProcessFunction(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>("count", Types.INT);
            countState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer", Types.LONG);
            timerState = getRuntimeContext().getState(timerDescriptor);
        }

        @Override
        public void processElement(UserVisit visit, Context ctx, Collector<String> out) throws Exception {
            int count = countState.value() == null ? 0 : countState.value();
            count++;
            countState.update(count);

            if (count >= threshold) {
                out.collect("User " + visit.userId + " has visited " + count + " times, exceeding the threshold!");

                if (timerState.value() == null) {
                    long timerTimestamp = ctx.timerService().currentProcessingTime() + 10000;
                    ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
                    timerState.update(timerTimestamp);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            countState.clear();
            timerState.clear();
            out.collect("User " + ctx.getCurrentKey() + "'s visit count has been reset.");
        }
    }
}