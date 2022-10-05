package flinkstreaming;

import flinkstreaming.db.SqliteStoringSinkFunctions;
import flinkstreaming.model.CustomerMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class CustomerStoring {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatelessEnvironment(args);

        KafkaSource<CustomerMessage> customersKafkaSource = KafkaSource.<CustomerMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("customerStoring")
                .setTopics(Arrays.asList(Config.TOPIC_CUSTOMERS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(CustomerMessage.CustomerMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<CustomerMessage> customersStream =  env.fromSource(
                customersKafkaSource, WatermarkStrategy.noWatermarks(), "Customers Stream");

        customersStream
                .addSink(new SqliteStoringSinkFunctions.CustomerStoring())
                .name("Sqlite Sink");

//        customersStream
//                .keyBy(am -> am.customerId)
//                .asQueryableState("customers-state")
//        ;
//
        env.execute("Customer Storing");
    }

}
