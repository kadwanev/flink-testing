package flinkstreaming.db;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.time.ZonedDateTime;

public class SqliteStoringSinkFunctions {

    public static class AccountStoring extends RichSinkFunction<AccountMessage> {
        @Override
        public void invoke(AccountMessage message, Context context) throws Exception {
            SqliteStore.getInstance().insertAccount(message);
        }
    }

    public static class CustomerStoring extends RichSinkFunction<CustomerMessage> {
        @Override
        public void invoke(CustomerMessage message, Context context) throws Exception {
            SqliteStore.getInstance().insertCustomer(message);
        }
    }

    public static class TransactionStoring extends RichSinkFunction<TransactionMessage> {
        @Override
        public void invoke(TransactionMessage message, Context context) throws Exception {
            SqliteStore.getInstance().insertTransaction(message);
        }
    }

    public static class CustomerValueQueryStoring extends RichSinkFunction<Tuple4<CustomerMessage, String, String, ZonedDateTime>> {
        @Override
        public void invoke(Tuple4<CustomerMessage, String, String, ZonedDateTime> value, Context context) throws Exception {
            SqliteStore.getInstance().insertCustomerValueQueryTable(value.f0.customerId, value.f1, value.f2, value.f3);
        }
    }

}
