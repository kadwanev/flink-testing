package flinkstreaming.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;

public class CustomerMessage {

    public int customerId;
    public String email;
    public boolean notifyPreference;
    public String state;
    public ZonedDateTime eventTime;

    @Override
    public String toString() {
        return "CustomerMessage{" +
                "customerId=" + customerId +
                ", email='" + email + '\'' +
                ", notifyPreference=" + notifyPreference +
                ", state='" + state + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public static class CustomerMessageDeserializer implements Deserializer<CustomerMessage> {

        private static final String ENCODING = "UTF8";

        @Override
        public CustomerMessage deserialize(String s, byte[] data) {
            try {
                String messageStr = data == null ? null : new String(data, this.ENCODING);

                String[] splits = messageStr.split(",");
                if (splits.length != 5) {
                    throw new SerializationException("Invalid customer message " + messageStr);
                }
                CustomerMessage cm = new CustomerMessage();
                cm.customerId = Integer.parseInt(splits[0]);
                cm.email = splits[1];
                cm.notifyPreference = Boolean.parseBoolean(splits[2]);
                cm.state = splits[3];
                cm.eventTime = ZonedDateTime.parse(splits[4]);
                return cm;
            } catch (UnsupportedEncodingException var4) {
                throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.ENCODING);
            }
        }
    }

}
