package flinkstreaming.util;

import flinkstreaming.aggregate.AllNumAggregate;
import flinkstreaming.aggregate.NumAggregate;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class SummingFunction {

    public static final class AllAggregate<T, W extends Window> implements AllWindowFunction<T, AllNumAggregate<Long,Integer>, W>
    {
        private SerializableFunction<T,Integer> numFunc = null;

        public <T extends Integer> AllAggregate() {
            this.numFunc = i -> (Integer)i;
        }

        public AllAggregate(SerializableFunction<T,Integer> numFunc) {
            this.numFunc = numFunc;
        }

        @Override
        public void apply(W globalWindow, Iterable<T> values, Collector<AllNumAggregate<Long,Integer>> out) throws Exception {
            AllNumAggregate<Long,Integer> agg = new AllNumAggregate<>();
            agg.number = 0l;
            for (T v : values) {
                Integer value = this.numFunc.apply(v);
                agg.number += value;
                agg.reason.add( value );
            }
            out.collect(agg);
        }

    }

    public static final class Aggregate<T, W extends Window> implements WindowFunction<T, NumAggregate<Integer,Long,Integer>, Integer, W> {
        private SerializableFunction<T,Integer> numFunc = null;

        public <T extends Integer> Aggregate() {
            this.numFunc = i -> (Integer)i;
        }

        public Aggregate(SerializableFunction<T,Integer> numFunc) {
            this.numFunc = numFunc;
        }

        @Override
        public void apply(Integer key, W window, Iterable<T> values, Collector<NumAggregate<Integer,Long,Integer>> out) throws Exception {
            NumAggregate<Integer,Long,Integer> agg = new NumAggregate<>();
            agg.key = key;
            agg.number = 0l;
            for (T v : values) {
                Integer value = this.numFunc.apply(v);
                agg.number += value;
                agg.reason.add( value );
            }
            out.collect(agg);
        }
    }

}
