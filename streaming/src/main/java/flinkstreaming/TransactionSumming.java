package flinkstreaming;

import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;

public class TransactionSumming {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

//        env.setParallelism(1);

        KafkaSource<String> transactionsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("transactionSumming")
                .setTopics(Arrays.asList(Config.TOPIC_TRANSACTIONS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();


        DataStreamSource<String> transactionsStream =  env.fromSource(
                transactionsKafkaSource, WatermarkStrategy.noWatermarks(), "Transactions Stream");

        transactionsStream
                .map(Integer::parseInt)
                .countWindowAll(5)
                .apply(new SummingFunction())
                .map( i -> "Transaction Tumbling Total: " + i)
                .print().setParallelism(1);

        transactionsStream
                .map(Integer::parseInt)
                .countWindowAll(5, 1)
                .apply(new SummingFunction())
                .map( i -> "Transaction Sliding Total: " + i)
                .print().setParallelism(1);

        env.execute("Transaction Summing");
    }

}
