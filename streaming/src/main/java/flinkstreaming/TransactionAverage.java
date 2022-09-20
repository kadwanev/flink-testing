package flinkstreaming;

import flinkstreaming.model.TransactionMessage;
import flinkstreaming.model.TransactionMessageDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class TransactionAverage {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

//        env.setParallelism(1);

        KafkaSource<TransactionMessage> accountsKafkaSource = KafkaSource.<TransactionMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("transactionAverage")
                .setTopics(Arrays.asList(Config.TOPIC_TRANSACTIONS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(TransactionMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<TransactionMessage> accountsStream =  env.fromSource(
                accountsKafkaSource, WatermarkStrategy.noWatermarks(), "Transactions Stream");

/*
        accountsStream
                .keyBy(am -> am.accountId)
                .countWindowAll(5)
                .apply(new AveragingFunction<>(tm -> tm.amount))
                .map( i -> "Transaction Tumbling Average: " + i)
                .print().setParallelism(1);

        accountsStream
                .keyBy(am -> am.accountId)
                .countWindowAll(5, 1)
                .apply(new AveragingFunction<>(tm -> tm.amount))
                .map( i -> "Transaction Sliding Average: " + i)
                .print().setParallelism(1);
*/
        env.execute("Transaction Average");
    }

}
