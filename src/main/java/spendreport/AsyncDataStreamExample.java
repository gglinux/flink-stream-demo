package spendreport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AsyncDataStreamExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.fromElements("A", "B", "C");

        // 实现一个异步操作的AsyncFunction
        AsyncFunction<String, Tuple2<String, String>> asyncFunction = new AsyncFunction<String, Tuple2<String, String>>() {
            @Override
            public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) {
                // 模拟异步访问外部系统（如REST API）
                CompletableFuture.supplyAsync(() -> {
                    try {
                        TimeUnit.MILLISECONDS.sleep(100); // 模拟延迟
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return "Result for " + input;
                }).thenAccept((String result) -> {
                    resultFuture.complete(Collections.singleton(new Tuple2<>(input, result)));
                });
            }
        };

        // 使用AsyncDataStream应用AsyncFunction
        DataStream<Tuple2<String, String>> outputStream = AsyncDataStream.orderedWait(
                inputStream,
                asyncFunction,
                1000, // 超时时间
                TimeUnit.MILLISECONDS,
                100   // 最大并发请求数
        );

        outputStream.print();

        env.execute("AsyncDataStream Example");
    }
}
