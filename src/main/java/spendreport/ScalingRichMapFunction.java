package spendreport;

/**
 * @className: ScalingRichMapFunction
 * @description: TODO 类描述
 * @author: jiaweiguo
 * @date: 2023/8/9
 **/
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class ScalingRichMapFunction extends RichMapFunction<Integer, Integer> {
    private final int scaleFactor;

    public ScalingRichMapFunction(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 在这里可以执行一些资源的分配操作，例如打开数据库连接或初始化状态
    }

    @Override
    public Integer map(Integer value) throws Exception {
        return value * scaleFactor;
    }

    @Override
    public void close() throws Exception {
        // 在这里可以执行一些资源的释放操作，例如关闭数据库连接或释放资源
    }
}