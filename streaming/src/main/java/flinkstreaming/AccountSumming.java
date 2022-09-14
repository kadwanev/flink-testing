package flinkstreaming;

import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;

public class AccountSumming {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

        KafkaSource<String> accountsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("accountSumming")
                .setTopics(Arrays.asList(Config.TOPIC_ACCOUNTS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();


        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(Config.BOOTSTRAP_SERVERS,
                Config.TOPIC_ACCOUNT_TOTALS,             // target topic
                new SimpleStringSchema()    // serialization schema
//                properties,             // producer config
                ); // fault-tolerance

        DataStreamSource<String> accountsStream =  env.fromSource(
                accountsKafkaSource, WatermarkStrategy.noWatermarks(), "Accounts Stream");

        accountsStream
                .map(Integer::parseInt)
                .countWindowAll(10, 1)
                .apply(new SummingFunction())
                .map(i -> "Account Sliding Sum: " + i.toString())
                .print().setParallelism(1);

//        accountsStream.addSink(new KafkaSink<>)

        env.execute("Account Summing");
    }
}
