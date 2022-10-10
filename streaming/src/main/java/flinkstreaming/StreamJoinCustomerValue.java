package flinkstreaming;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StreamJoinCustomerValue {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);
        final TableEnvironment tEnv = StreamTableEnvironment.create(env);

        String consumerGroupId = "streamJoinCustomerValuable";

        DataStreamSource<CustomerMessage> customersStreamSource =
                Config.getCustomersStream(consumerGroupId, env);
        DataStreamSource<AccountMessage> accountsStreamSource =
                Config.getAccountsStream(consumerGroupId, env);
        DataStreamSource<TransactionMessage> transactionStreamSource =
                Config.getTransactionsStream(consumerGroupId, env);


        DataStream<Tuple2<AccountMessage,TransactionMessage>> accountLatestTransactionStream =
                transactionStreamSource.keyBy(tm -> tm.accountId)
                    .reduce((prev,curr) -> curr)
                    .join(accountsStreamSource)
                    .where(tm -> tm.accountId)
                    .equalTo(am -> am.accountId)
                    .window(SlidingProcessingTimeWindows.of(Time.minutes(15), Time.milliseconds(1)))
                    .apply(new PassThroughFlipJoinFunction<>());
//                    .intervalJoin(accountsStreamSource.keyBy(am -> am.accountId))
//                    .between(Time.days(365), Time.days(365))
//                    .process(new PassThroughFlipProcessFunction<>())
//                    .uid("account-latest-transaction-stream");
        accountLatestTransactionStream.print();
/*
        DataStream<List<Tuple2<AccountMessage,TransactionMessage>>> listAccountsLatestTransactionStream =
                accountLatestTransactionStream
                    .keyBy(i -> i.f0.customerId)
                    .window(ProcessingTimeSessionWindows.withGap(Time.days(365)))
                    .aggregate(new ListWindowAggregateFunction())
                    .uid("list-account-latest-transaction-stream");
        listAccountsLatestTransactionStream.print();

        DataStream<Tuple2<CustomerMessage,List<Tuple2<AccountMessage,TransactionMessage>>>> customerAccountsTransactionStream =
                listAccountsLatestTransactionStream
                        .join(customersStreamSource)
                        .where(actm -> actm.get(0).f0.customerId)
                        .equalTo(cm -> cm.customerId)
                        .window(SlidingProcessingTimeWindows.of(Time.days(365), Time.milliseconds(1)))
                        .apply(new PassThroughFlipJoinFunction<>());
//                    .keyBy(i -> i.get(0).f0.customerId)
//                    .intervalJoin(customersStreamSource.keyBy(cm -> cm.customerId))
//                    .between(Time.days(365), Time.days(365))
//                    .process(new PassThroughFlipProcessFunction<>())
//                    .uid("customer-account-latest-transaction-stream");
        customerAccountsTransactionStream.print();
*/


        env.execute("Calculate Customer Valuable via Stream Joining");

    }

//    public static class CustomerValueProcessFunction<Tuple2<>>

    public static class ListWindowAggregateFunction implements AggregateFunction<Tuple2<AccountMessage,TransactionMessage>,
                                                                                 ArrayList<Tuple2<AccountMessage,TransactionMessage>>,
                                                                                 List<Tuple2<AccountMessage, TransactionMessage>>> {
        @Override
        public ArrayList<Tuple2<AccountMessage,TransactionMessage>> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public ArrayList<Tuple2<AccountMessage,TransactionMessage>> add(Tuple2<AccountMessage,TransactionMessage> input, ArrayList<Tuple2<AccountMessage,TransactionMessage>> acc) {
            acc.add(input);
            return acc;
        }

        @Override
        public List<Tuple2<AccountMessage, TransactionMessage>> getResult(ArrayList<Tuple2<AccountMessage,TransactionMessage>> acc) {
            HashMap<Integer,Tuple2<AccountMessage,TransactionMessage>> combined = new HashMap<>();
            for (Tuple2<AccountMessage,TransactionMessage> item : acc) {
                Tuple2<AccountMessage,TransactionMessage> prev = combined.get(item.f0.accountId);
                if (prev != null) {
                    if (item.f1.eventTime.isAfter(prev.f1.eventTime) || item.f0.eventTime.isAfter(prev.f0.eventTime)) {
                        combined.put(item.f0.accountId, item);
                    }
                } else {
                    combined.put(item.f1.accountId, item);
                }
            }
            System.out.println("get result list window aggregates in:" + acc.size() + " to out: " + combined.size() );
            return new ArrayList<>(combined.values());
        }

        @Override
        public ArrayList<Tuple2<AccountMessage,TransactionMessage>> merge(ArrayList<Tuple2<AccountMessage,TransactionMessage>> acc1, ArrayList<Tuple2<AccountMessage,TransactionMessage>> acc2) {
            System.out.println("MERGING list window aggregates ???");
            ArrayList<Tuple2<AccountMessage,TransactionMessage>> combined = new ArrayList<>();
            combined.addAll(acc1);
            combined.addAll(acc2);
            return combined;
        }
    }

    public static class PassThroughProcessFunction<IN1,IN2> extends ProcessJoinFunction<IN1,IN2, Tuple2<IN1,IN2>> {
        @Override
        public void processElement(IN1 lhs, IN2 rhs, Context context, Collector<Tuple2<IN1, IN2>> out) throws Exception {
            out.collect(new Tuple2<>(lhs,rhs));
        }
    }
    public static class PassThroughFlipProcessFunction<IN1,IN2> extends ProcessJoinFunction<IN1,IN2, Tuple2<IN2,IN1>> {
        @Override
        public void processElement(IN1 lhs, IN2 rhs, Context context, Collector<Tuple2<IN2, IN1>> out) throws Exception {
            out.collect(new Tuple2<>(rhs,lhs));
        }
    }

    public static class PassThroughFlipJoinFunction<IN1,IN2> implements JoinFunction<IN1,IN2,Tuple2<IN2,IN1>> {
        @Override
        public Tuple2<IN2, IN1> join(IN1 in1, IN2 in2) throws Exception {
            return new Tuple2<>(in2,in1);
        }
    }

}
