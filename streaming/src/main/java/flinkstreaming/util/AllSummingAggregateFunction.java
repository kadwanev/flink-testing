package flinkstreaming.util;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class AllSummingAggregateFunction<W extends Window, T, ACC, V, R> extends AggregateApplyAllWindowFunction<W, T, ACC, V, R> {

    public AllSummingAggregateFunction(AggregateFunction<T, ACC, V> aggFunction, AllWindowFunction<V, R, W> windowFunction) {
        super(aggFunction, windowFunction);
    }

    @Override
    public void apply(W window, Iterable<T> values, Collector<R> out) throws Exception {
        super.apply(window, values, out);
    }
}
