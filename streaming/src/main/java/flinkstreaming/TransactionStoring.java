package flinkstreaming;

import flinkstreaming.db.SqliteStoringSinkFunctions;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransactionStoring {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatelessEnvironment(args);

        DataStreamSource<TransactionMessage> transactionStreamSource = Config.getTransactionsStream("transactionStoring", env);

        transactionStreamSource
                .addSink(new SqliteStoringSinkFunctions.TransactionStoring())
                .name("Sqlite Sink");

        env.execute("Transaction Storing");
    }

}
