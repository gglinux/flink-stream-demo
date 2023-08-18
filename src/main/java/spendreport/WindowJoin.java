package spendreport;

import static java.util.Objects.requireNonNull;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;



/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the streams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates the streams of pairs at a
 * configurable rate.
 */
public class WindowJoin {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println(
                "To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // create the data sources for both grades and salaries
        DataStream<Tuple2<String, Integer>> grades =
                WindowJoinSampleData.GradeSource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple2<String, Integer>> salaries =
                WindowJoinSampleData.SalarySource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple3<String, Integer, Integer>> joinedStream =
                runWindowJoin(grades, salaries, windowSize);

        // print the results with a single thread, rather than in parallel
        joinedStream.print().setParallelism(1);

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {

        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(
                        new JoinFunction<
                                Tuple2<String, Integer>,
                                Tuple2<String, Integer>,
                                Tuple3<String, Integer, Integer>>() {

                            @Override
                            public Tuple3<String, Integer, Integer> join(
                                    Tuple2<String, Integer> first, Tuple2<String, Integer> second) {
                                return new Tuple3<String, Integer, Integer>(
                                        first.f0, first.f1, second.f1);
                            }
                        });
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}


class WindowJoinSampleData {

    static final String[] NAMES = {"tom", "jerry", "alice", "bob", "john", "grace"};
    static final int GRADE_COUNT = 5;
    static final int SALARY_MAX = 10000;

    /** Continuously generates (name, grade). */
    public static class GradeSource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(GRADE_COUNT) + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new GradeSource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        }
    }

    /** Continuously generates (name, salary). */
    public static class SalarySource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(SALARY_MAX) + 1);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        public static DataStream<Tuple2<String, Integer>> getSource(
                StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(
                    new ThrottledIterator<>(new SalarySource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
        }
    }
}


class ThrottledIterator<T> implements Iterator<T>, Serializable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("NonSerializableFieldInSerializableClass")
    private final Iterator<T> source;

    private final long sleepBatchSize;
    private final long sleepBatchTime;

    private long lastBatchCheckTime;
    private long num;

    public ThrottledIterator(Iterator<T> source, long elementsPerSecond) {
        this.source = requireNonNull(source);

        if (!(source instanceof Serializable)) {
            throw new IllegalArgumentException("source must be java.io.Serializable");
        }

        if (elementsPerSecond >= 100) {
            // how many elements would we emit per 50ms
            this.sleepBatchSize = elementsPerSecond / 20;
            this.sleepBatchTime = 50;
        } else if (elementsPerSecond >= 1) {
            // how long does element take
            this.sleepBatchSize = 1;
            this.sleepBatchTime = 1000 / elementsPerSecond;
        } else {
            throw new IllegalArgumentException(
                    "'elements per second' must be positive and not zero");
        }
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        // delay if necessary
        if (lastBatchCheckTime > 0) {
            if (++num >= sleepBatchSize) {
                num = 0;

                final long now = System.currentTimeMillis();
                final long elapsed = now - lastBatchCheckTime;
                if (elapsed < sleepBatchTime) {
                    try {
                        Thread.sleep(sleepBatchTime - elapsed);
                    } catch (InterruptedException e) {
                        // restore interrupt flag and proceed
                        Thread.currentThread().interrupt();
                    }
                }
                lastBatchCheckTime = now;
            }
        } else {
            lastBatchCheckTime = System.currentTimeMillis();
        }

        return source.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
