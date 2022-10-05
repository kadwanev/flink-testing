package flinkstreaming;

import flinkstreaming.db.SqliteAsyncAccountFunction;
import flinkstreaming.model.TransactionMessage;
import flinkstreaming.util.AveragingFunction;
import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class TransactionProcessing {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

        KafkaSource<TransactionMessage> transactionsKafkaSource = KafkaSource.<TransactionMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("transactionSumming")
                .setTopics(Arrays.asList(Config.TOPIC_TRANSACTIONS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(TransactionMessage.TransactionMessageDeserializer.class))
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<TransactionMessage> transactionsStream =  env.fromSource(
                transactionsKafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Transactions Stream");

        KeyedStream<TransactionMessage,Integer> keyedTransactionStream = transactionsStream
                .keyBy(am -> am.accountId);

        keyedTransactionStream
                .countWindow(Long.MAX_VALUE, 1)
                .process(new SummingFunction.Aggregate<>(tm -> tm.amount))
                .map( i -> "Transaction Total: " + i)
                .name("Transaction Total by Account")
                .uid("transaction-total-by-account")
                .print();

        keyedTransactionStream
                .countWindow(50, 1)
                .process(new AveragingFunction.Aggregate<>(tm -> tm.amount))
                .map( i -> "Transaction Last 50 Average: " + i)
                .name("Transaction Last 50 Average by Account")
                .uid("transaction-50-average-by-account")
                .print();

        // Use Async IO to query account and join with transaction

        AsyncDataStream.unorderedWait(transactionsStream, new SqliteAsyncAccountFunction(),1000, TimeUnit.MILLISECONDS, 100)
                .name("Transaction joined to Account")
                .print();

        env.execute("Transaction Processing");
    }

}
