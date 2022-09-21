package flinkstreaming.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;

public class VisitMessage {

    public int count;
    public ZonedDateTime eventTime;

    @Override
    public String toString() {
        return "VisitMessage{" +
                "count=" + count +
                ", eventTime=" + eventTime +
                ", eventTimeMillis=" + eventTime.toInstant().toEpochMilli() +
                '}';
    }

    public static class VisitMessageDeserializer implements Deserializer<VisitMessage> {

        private static final String ENCODING = "UTF8";

        @Override
        public VisitMessage deserialize(String s, byte[] data) {
            try {
                String messageStr = data == null ? null : new String(data, this.ENCODING);

                String[] splits = messageStr.split(",");
                if (splits.length != 2) {
                    throw new SerializationException("Invalid visit message " + messageStr);
                }
                VisitMessage vm = new VisitMessage();
                vm.count = Integer.parseInt(splits[0]);
                vm.eventTime = ZonedDateTime.parse(splits[1]);
                return vm;
            } catch (UnsupportedEncodingException var4) {
                throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.ENCODING);
            }
        }
    }

}
