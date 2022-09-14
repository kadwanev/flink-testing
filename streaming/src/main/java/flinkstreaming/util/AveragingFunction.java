package flinkstreaming.util;

import flinkstreaming.NumAggregate;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class AveragingFunction implements AllWindowFunction<Integer, NumAggregate<Double,Integer>, GlobalWindow> {

    @Override
    public void apply(GlobalWindow globalWindow, Iterable<Integer> values, Collector<NumAggregate<Double,Integer>> out) throws Exception {
        NumAggregate<Double,Integer> agg = new NumAggregate<>();
        long sum = 0l;
        int count = 0;
        for (Integer value : values) {
            sum += value;
            count += 1;
            agg.reason.add( value );
        }
        agg.number = sum / (double)count;
        out.collect(agg);
    }
}
