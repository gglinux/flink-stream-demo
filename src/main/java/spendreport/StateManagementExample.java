package spendreport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StateManagementExample {

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
                .process(new VisitCountProcessFunction(3))
                .print();

        env.execute("State Management Example");
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

    public static class VisitCountProcessFunction extends KeyedProcessFunction<Integer, UserVisit, String> {
        private final int threshold;
        private ValueState<Integer> countState;

        public VisitCountProcessFunction(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>("count", Types.INT);
            countState = getRuntimeContext().getState(countDescriptor);
        }

        @Override
        public void processElement(UserVisit visit, Context ctx, Collector<String> out) throws Exception {
            int count = countState.value() == null ? 0 : countState.value();
            count++;
            countState.update(count);

            if (count >= threshold) {
                out.collect("User " + visit.userId + " has visited " + count + " times, exceeding the threshold!");
            }
        }
    }
}