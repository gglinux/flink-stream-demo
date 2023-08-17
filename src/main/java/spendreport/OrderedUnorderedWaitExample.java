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

public class OrderedUnorderedWaitExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.fromElements("A", "B", "C");

        // 实现一个异步操作的AsyncFunction
        AsyncFunction<String, Tuple2<String, String>> asyncFunction = new AsyncFunction<String, Tuple2<String, String>>() {
            @Override
            public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) {
                // 模拟异步访问外部系统（如REST API）
                CompletableFuture.supplyAsync(() -> {
                    String ret = System.currentTimeMillis() + " " + input;
                    try {
                        TimeUnit.MILLISECONDS.sleep(input.equals("B") ? 3000 : 100); // 模拟不同延迟
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return " Result for " + ret;
                }).thenAccept((String result) -> {
                    result = System.currentTimeMillis() + result;
                    resultFuture.complete(Collections.singleton(new Tuple2<>(input, result)));
                });
            }
        };

        // 使用AsyncDataStream应用AsyncFunction（orderedWait）
        DataStream<Tuple2<String, String>> orderedWaitStream = AsyncDataStream.orderedWait(
                inputStream,
                asyncFunction,
                5000, // 超时时间
                TimeUnit.MILLISECONDS,
                100   // 最大并发请求数
        );

        // 使用AsyncDataStream应用AsyncFunction（unorderedWait）
        DataStream<Tuple2<String, String>> unorderedWaitStream = AsyncDataStream.unorderedWait(
                inputStream,
                asyncFunction,
                5000, // 超时时间
                TimeUnit.MILLISECONDS,
                100   // 最大并发请求数
        );

//        orderedWaitStream.print("Ordered Wait");
        unorderedWaitStream.print("Unordered Wait");

        env.execute("Ordered and Unordered Wait Example");
    }
}