package flinkstreaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;

public class TotalSumming {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
//        env.setParallelism(1);

        KafkaSource<String> accountsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("totalSumming")
                .setTopics(Arrays.asList(Config.TOPIC_ACCOUNTS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        KafkaSource<String> transactionsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("totalSumming")
                .setTopics(Arrays.asList(Config.TOPIC_TRANSACTIONS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();


        DataStreamSource<String> accountsStream =  env.fromSource(
                accountsKafkaSource, WatermarkStrategy.noWatermarks(), "Accounts Stream");
        DataStreamSource<String> transactionsStream =  env.fromSource(
                transactionsKafkaSource, WatermarkStrategy.noWatermarks(), "Transactions Stream");

//        accountsStream.join(transactionsStream).

        accountsStream
                .map(Integer::parseInt)
                .countWindowAll(10)
                .sum("0")
                .map(i -> "Total Sum: " + i)
                .print().setParallelism(1);

        env.execute("Total Summing");
    }


}
