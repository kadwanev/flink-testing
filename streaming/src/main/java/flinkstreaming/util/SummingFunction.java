package flinkstreaming.util;

import flinkstreaming.aggregate.AllNumAggregate;
import flinkstreaming.aggregate.NumAggregate;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class SummingFunction {

    public static final class AllAggregate<T, W extends Window> extends ProcessAllWindowFunction<T, AllNumAggregate<Long,Integer>, W>
    {
        private SerializableFunction<T,Integer> numFunc = null;

        public <T extends Integer> AllAggregate() {
            this.numFunc = i -> (Integer)i;
        }

        public AllAggregate(SerializableFunction<T,Integer> numFunc) {
            this.numFunc = numFunc;
        }

        @Override
        public void clear(Context context) throws Exception {
            super.clear(context);
        }

        @Override
        public void process(Context context, Iterable<T> values, Collector<AllNumAggregate<Long, Integer>> out) throws Exception {
//            context.globalState().getListState()
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

    public static final class Aggregate<T, W extends Window> extends ProcessWindowFunction<T, NumAggregate<Integer,Long,Integer>, Integer, W> {
        private final static MapStateDescriptor<Integer, NumAggregate<Integer,Long,Integer>> mapDesc =
                new MapStateDescriptor<Integer, NumAggregate<Integer,Long,Integer>>("stateStore", (Class<Integer>) Integer.valueOf(1).getClass(), (Class<NumAggregate<Integer, Long, Integer>>) new NumAggregate<Integer,Long,Integer>().getClass());

        private SerializableFunction<T,Integer> numFunc = null;

        public <T extends Integer> Aggregate() {
            this.numFunc = i -> (Integer)i;
        }

        public Aggregate(SerializableFunction<T,Integer> numFunc) {
            this.numFunc = numFunc;
        }

//        @Override
//        public void open(Configuration parameters) throws Exception {
//            super.open(parameters);
//        }
//
//        @Override
//        public void clear(ProcessWindowFunction<T, NumAggregate<Integer, Long, Integer>, Integer, W>.Context context) throws Exception {
//            super.clear(context);
//        }

        @Override
        public void process(Integer key, Context context, Iterable<T> values, Collector<NumAggregate<Integer,Long,Integer>> out) throws Exception {
//            context.windowState().get
            NumAggregate<Integer,Long,Integer> agg = context.windowState().getMapState(mapDesc).get(key);
            if (agg == null) {
                agg = new NumAggregate<>();
            }
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
