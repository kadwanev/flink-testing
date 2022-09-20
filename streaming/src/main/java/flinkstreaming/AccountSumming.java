package flinkstreaming;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.AccountMessageDeserializer;
import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class AccountSumming {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

        KafkaSource<AccountMessage> accountsKafkaSource = KafkaSource.<AccountMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("accountSumming")
                .setTopics(Arrays.asList(Config.TOPIC_ACCOUNTS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(AccountMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
/*
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(Config.BOOTSTRAP_SERVERS,
                Config.TOPIC_ACCOUNT_TOTALS,             // target topic
                new SimpleStringSchema()    // serialization schema
//                properties,             // producer config
                ); // fault-tolerance
*/
        DataStreamSource<AccountMessage> accountsStream =  env.fromSource(
                accountsKafkaSource, WatermarkStrategy.noWatermarks(), "Accounts Stream");

        accountsStream
                .keyBy(am -> am.accountId)
                .countWindow(10, 1)
                .apply(new SummingFunction.Aggregate<>(am -> am.balance))
                .map(i -> "Account Sliding Sum: " + i.toString())
                .print().setParallelism(5);

//        accountsStream.addSink(new KafkaSink<>)

        env.execute("Account Summing");
    }
}
