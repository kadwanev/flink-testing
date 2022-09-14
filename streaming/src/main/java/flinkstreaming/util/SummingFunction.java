package flinkstreaming.util;

import flinkstreaming.NumAggregate;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class SummingFunction implements AllWindowFunction<Integer, NumAggregate<Long,Integer>, GlobalWindow> {

    @Override
    public void apply(GlobalWindow globalWindow, Iterable<Integer> values, Collector<NumAggregate<Long,Integer>> out) throws Exception {
        NumAggregate<Long,Integer> agg = new NumAggregate<>();
        agg.number = 0l;
        for (Integer value : values) {
            agg.number += value;
            agg.reason.add( value );
        }
        out.collect(agg);
    }
}
