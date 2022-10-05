package flinkstreaming.db;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

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

}
