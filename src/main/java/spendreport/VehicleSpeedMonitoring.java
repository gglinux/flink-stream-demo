package spendreport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class VehicleSpeedMonitoring {

    public static void main(String[] args) throws Exception {
        // 1. 创建Flink流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 添加数据源，这里使用一个模拟的车辆传感器数据源
        DataStream<String> sourceStream = env.socketTextStream("localhost", 9999);

        // 3. 数据处理：解析传感器数据，并计算每辆车的平均速度
        DataStream<Tuple2<Integer, Double>> avgSpeedStream = sourceStream
                .map(new SensorDataParser()) // 解析传感器数据
                .keyBy(sensorData -> sensorData.f0) // 按车辆ID分组
                .process(new AverageSpeedCalculator()); // 计算每辆车的平均速度

        // 4. 输出结果：将超速车辆的警告信息输出到控制台
        avgSpeedStream
                .filter(avgSpeed -> avgSpeed.f1 > 100) // 过滤出超速车辆
                .print();

        // 5. 启动Flink流处理应用程序
        env.execute("Vehicle Speed Monitoring");
    }

    // 自定义MapFunction，用于解析传感器数据
    public static class SensorDataParser implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) {
            String[] tokens = value.split(",");
            int vehicleId = Integer.parseInt(tokens[0]);
            double speed = Double.parseDouble(tokens[1]);
            return new Tuple2<>(vehicleId, speed);
        }
    }

    // 自定义KeyedProcessFunction，用于计算每辆车的平均速度
    public static class AverageSpeedCalculator extends KeyedProcessFunction<Integer, Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
        private transient ValueState<Double> sumState;
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) {
            sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Double.class, 0.0));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class, 0));
        }

        @Override
        public void processElement(Tuple2<Integer, Double> value, Context ctx, Collector<Tuple2<Integer, Double>> out) throws Exception {
            double currentSum = sumState.value();
            int currentCount = countState.value();

            currentSum += value.f1;
            currentCount++;

            sumState.update(currentSum);
            countState.update(currentCount);

            double avgSpeed = currentSum / currentCount;
            out.collect(new Tuple2<>(value.f0, avgSpeed));
        }
    }
}