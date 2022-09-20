package flinkstreaming;

import flinkstreaming.util.AveragingFunction;
import flinkstreaming.util.SummingFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;

public class VisitCounting {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);

        KafkaSource<String> visitsKafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(Config.BOOTSTRAP_SERVERS)
                .setGroupId("visitCounting")
                .setTopics(Arrays.asList(Config.TOPIC_VISITS))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<String> visitsStream = env.fromSource(
                visitsKafkaSource, WatermarkStrategy.noWatermarks(), "Visits Stream");

        visitsStream
                .map(Integer::parseInt)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new SummingFunction.AllAggregate<>())
                .map(i -> "Visits Total (30sec): " + i.toString())
                .print();

        visitsStream
                .map(Integer::parseInt)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new AveragingFunction.AllAggregate<>())
                .map(i -> "Visits Average (30sec): " + i.toString())
                .print();

//        accountsStream.addSink(new KafkaSink<>)

        env.execute("Visits Counting");
    }

}
