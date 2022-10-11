package flinkstreaming;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Config {

    public static final String DB_LOCATION = "/Users/nkadwa/workspace/flink-testing/streaming/db";

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static final String TOPIC_CUSTOMERS = "customers";
    public static final String TOPIC_ACCOUNTS = "accounts";
    public static final String TOPIC_ACCOUNT_TOTALS = "accountsTotal";

    public static final String TOPIC_TRANSACTIONS = "transactions";
    public static final String TOPIC_TRANSACTIONS_TOTALS = "transactionsTotal";

    public static final String TOPIC_VISITS = "visits";

    public static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(200);

    public static StreamExecutionEnvironment getStatelessEnvironment(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        return env;
    }

    public static StreamExecutionEnvironment getStatefulEnvironment(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        env.enableCheckpointing(30000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        return env;
    }

    public static TableEnvironment getTableEnvironment(StreamExecutionEnvironment env) {
        return StreamTableEnvironment.create(env);
    }

    public static DataStreamSource<CustomerMessage> getCustomersStream(String consumerGroupId, StreamExecutionEnvironment env) {
        KafkaSource<CustomerMessage> customersKafkaSource = KafkaSource.<CustomerMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId(consumerGroupId)
                .setTopics(Arrays.asList(Config.TOPIC_CUSTOMERS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(CustomerMessage.CustomerMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<CustomerMessage> customersSourceStream = env.fromSource(
                customersKafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Customers Stream");

        return customersSourceStream;
    }

    public static DataStreamSource<AccountMessage> getAccountsStream(String consumerGroupId, StreamExecutionEnvironment env) {
        KafkaSource<AccountMessage> accountsKafkaSource = KafkaSource.<AccountMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId(consumerGroupId)
                .setTopics(Arrays.asList(Config.TOPIC_ACCOUNTS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(AccountMessage.AccountMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<AccountMessage> accountsSourceStream = env.fromSource(
                accountsKafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Accounts Stream");

        return accountsSourceStream;
    }

    public static DataStreamSource<TransactionMessage> getTransactionsStream(String consumerGroupId, StreamExecutionEnvironment env) {
        KafkaSource<TransactionMessage> transactionsKafkaSource = KafkaSource.<TransactionMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId(consumerGroupId)
                .setTopics(Arrays.asList(Config.TOPIC_TRANSACTIONS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(TransactionMessage.TransactionMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<TransactionMessage> transactionsSourceStream = env.fromSource(
                transactionsKafkaSource,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Transactions Stream");

        return transactionsSourceStream;
    }

}
