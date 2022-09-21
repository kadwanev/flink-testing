package flinkstreaming;

import flinkstreaming.model.TransactionMessage;
import flinkstreaming.util.AveragingFunction;
import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;

public class TransactionSumming {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

        KafkaSource<TransactionMessage> transactionsKafkaSource = KafkaSource.<TransactionMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("transactionSumming")
                .setTopics(Arrays.asList(Config.TOPIC_TRANSACTIONS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(TransactionMessage.TransactionMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<TransactionMessage> transactionsStream =  env.fromSource(
                transactionsKafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Transactions Stream");

        transactionsStream
                .keyBy(am -> am.accountId)
                .countWindow(Long.MAX_VALUE, 1)
                .apply(new SummingFunction.Aggregate<>(tm -> tm.amount))
                .map( i -> "Transaction Total: " + i)
                .print();

        transactionsStream
                .keyBy(am -> am.accountId)
                .countWindow(50, 1)
                .apply(new AveragingFunction.Aggregate<>(tm -> tm.amount))
                .map( i -> "Transaction Last 50 Average: " + i)
                .print();

        env.execute("Transaction Summing");
    }

}
