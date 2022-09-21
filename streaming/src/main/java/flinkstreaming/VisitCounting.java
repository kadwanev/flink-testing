package flinkstreaming;

import flinkstreaming.model.VisitMessage;
import flinkstreaming.util.AveragingFunction;
import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;

public class VisitCounting {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

        KafkaSource<VisitMessage> visitsKafkaSource = KafkaSource.<VisitMessage>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("visitCounting")
                .setTopics(Arrays.asList(Config.TOPIC_VISITS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(VisitMessage.VisitMessageDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .build();

        DataStreamSource<VisitMessage> visitsStream = env.fromSource(visitsKafkaSource,
                WatermarkStrategy.<VisitMessage>noWatermarks().withTimestampAssigner((event,timestamp) -> event.eventTime.toInstant().toEpochMilli()),
                "Visits Stream");

        visitsStream
                .map(vm -> vm.count)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new SummingFunction.AllAggregate<>())
                .map(i -> "Visits Total (30secs): " + i.toString())
                .print();

        visitsStream
                .map(vm -> vm.count)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new AveragingFunction.AllAggregate<>())
                .map(i -> "Visits Average (30secs): " + i.toString())
                .print();

//        accountsStream.addSink(new KafkaSink<>)

        env.execute("Visits Counting");
    }

}
