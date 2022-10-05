package flinkstreaming;

import flinkstreaming.db.SqliteStoringSinkFunctions;
import flinkstreaming.model.AccountMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class AccountStoring {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatelessEnvironment(args);

        KafkaSource<AccountMessage> accountsKafkaSource = KafkaSource.<AccountMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("accountStoring")
                .setTopics(Arrays.asList(Config.TOPIC_ACCOUNTS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(AccountMessage.AccountMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<AccountMessage> accountsStream =  env.fromSource(
                accountsKafkaSource, WatermarkStrategy.noWatermarks(), "Accounts Stream");

        // Store to Sqlite
        accountsStream
                .addSink(new SqliteStoringSinkFunctions.AccountStoring())
                .name("Sqlite Sink");

        // Store to queryable stream state
        accountsStream
                .keyBy(am -> am.accountId)
                        .asQueryableState("accounts-state")
                ;


        env.execute("Account Storing");
    }
}
