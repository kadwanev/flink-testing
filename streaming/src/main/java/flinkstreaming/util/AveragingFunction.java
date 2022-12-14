package flinkstreaming.util;

import flinkstreaming.aggregate.AllNumAggregate;
import flinkstreaming.aggregate.NumAggregate;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class AveragingFunction<T> {

    public static final class AllAggregate<T, W extends Window> extends ProcessAllWindowFunction<T, AllNumAggregate<Double,Integer>, W>
    {
        private SerializableFunction<T,Integer> numFunc = null;

        public <T extends Integer> AllAggregate() {
            this.numFunc = i -> (Integer)i;
        }

        public AllAggregate(SerializableFunction<T,Integer> numFunc) {
            this.numFunc = numFunc;
        }

        @Override
        public void process(Context context, Iterable<T> values, Collector<AllNumAggregate<Double,Integer>> out) throws Exception {
            AllNumAggregate<Double,Integer> agg = new AllNumAggregate<>();
            long sum = 0l;
            int count = 0;
            for (T v : values) {
                Integer value = this.numFunc.apply(v);
                sum += value;
                count += 1;
                agg.reason.add( value );
            }
            agg.number = sum / (double)count;
            out.collect(agg);
        }
    }

    public static final class Aggregate<T, W extends Window> extends ProcessWindowFunction<T, NumAggregate<Integer,Double,Integer>, Integer, W> {
        private SerializableFunction<T,Integer> numFunc = null;

        public <T extends Integer> Aggregate() {
            this.numFunc = i -> (Integer)i;
        }

        public Aggregate(SerializableFunction<T,Integer> numFunc) {
            this.numFunc = numFunc;
        }

        @Override
        public void process(Integer key, Context context, Iterable<T> values, Collector<NumAggregate<Integer,Double,Integer>> out) throws Exception {
            NumAggregate<Integer,Double,Integer> agg = new NumAggregate<>();
            agg.key = key;
            long sum = 0l;
            int count = 0;
            for (T v : values) {
                Integer value = this.numFunc.apply(v);
                sum += value;
                count += 1;
                agg.reason.add( value );
            }
            agg.number = sum / (double)count;
            out.collect(agg);
        }
    }

}
