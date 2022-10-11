package flinkstreaming;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class StreamJoinCustomerValue {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String consumerGroupId = "streamJoinCustomerValuable";

        DataStreamSource<CustomerMessage> customersStreamSource =
                Config.getCustomersStream(consumerGroupId, env);
        DataStreamSource<AccountMessage> accountsStreamSource =
                Config.getAccountsStream(consumerGroupId, env);
        DataStreamSource<TransactionMessage> transactionStreamSource =
                Config.getTransactionsStream(consumerGroupId, env);

        tEnv.createTemporaryView("customers", customersStreamSource, CustomerMessage.tableSchema());
        tEnv.createTemporaryView("accounts", accountsStreamSource, AccountMessage.tableSchema());
        tEnv.createTemporaryView("transactions", transactionStreamSource, TransactionMessage.tableSchema());

        Table customerValueJoinedTable = tEnv.sqlQuery("" +
                "   select c.customerId, c.notifyPreference, c.eventTime customerEventTime, " +
                "          a.accountId, a.eventTime accountEventTime, " +
                "          t.amount transactionAmount, " +
                "          t.eventTime transactionEventTime" +
//                "       , row_number() over (partition by t.accountId order by t.eventTime desc) rownum " +
                "   from customers c " +
                "        join accounts a on a.customerId = c.customerId " +
                "        join transactions t on t.accountId = a.accountId " +
                "");
/*
        tEnv.createTemporaryView("customerValueJoinedTable", customerValueJoinedTable);
        tEnv.createTemporarySystemFunction("CustomerValueAggregateFunction", CustomerValueAggregateFunction.class);

        Table resultTable = tEnv
                .from("customerValueJoinedTable")
                .where($("rownum").isEqual(1))
                .groupBy($("customerId"), $("notifyPreference"), $("customerEventTime"))
                .flatAggregate(call("CustomerValueAggregateFunction",
                        $("customerId"), $("notifyPreference"), $("customerEventTime"),
                        $("accountId"), $("accountEventTime"), $("transactionAmount"), $("transactionEventTime"))
                    .as("customerValue","updateSource","lastUpdated"))
                .select($("customerId"), $("customerValue"), $("updateSource"), $("lastUpdated"));
*/
        tEnv.toDataStream(customerValueJoinedTable).print();

        env.execute("Calculate Customer Valuable via Stream Joining");

    }

    // TODO: handle non-latest transaction filtering
    public static class CustomerValueAccumulator {
        public String value = "LOW";
        public String source = "UNK";
        public LocalDateTime lastUpdated = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
    }

    public static class CustomerValueAggregateFunction extends TableAggregateFunction<Tuple3<String,String,LocalDateTime>, CustomerValueAccumulator> {

        @Override
        public CustomerValueAccumulator createAccumulator() {
            return new CustomerValueAccumulator();
        }

        public void accumulate(CustomerValueAccumulator curValue, Integer customerId, Boolean notifyPreference,
                               LocalDateTime customerEventTime, Integer accountId, LocalDateTime accountEventTime,
                               Integer transactionAmount, LocalDateTime transactionEventTime ) {
            if (curValue.value.equals("LOW") || curValue.source.equals("UNK")) {
                if (notifyPreference && transactionAmount > 50) {
                    curValue.value = "HIGH";
                } else {
                    curValue.value = "LOW";
                }
            }
            if (accountEventTime.isAfter(customerEventTime) && accountEventTime.isAfter(transactionEventTime)) {
                curValue.source = "ACCT";
                curValue.lastUpdated = accountEventTime;
            } else
            if (transactionEventTime.isAfter(customerEventTime) && transactionEventTime.isAfter(accountEventTime)) {
                curValue.source = "TRAN";
                curValue.lastUpdated = transactionEventTime;
            } else {
                curValue.source = "CUST";
                curValue.lastUpdated = customerEventTime;
            }
        }

        public void emitValue(CustomerValueAccumulator acc, RetractableCollector<Tuple3<String,String,LocalDateTime>> out) {
            out.collect(new Tuple3<>(acc.value, acc.source, acc.lastUpdated));
        }
    }


}
