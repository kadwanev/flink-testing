package flinkstreaming.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.time.ZonedDateTime;

public class AccountMessage implements Serializable {

    public int accountId;
    public int balance;
    public ZonedDateTime eventTime;

    public static class AccountMessageDeserializer implements Deserializer<AccountMessage> {

        private static final String ENCODING = "UTF8";

        @Override
        public AccountMessage deserialize(String s, byte[] data) {
            try {
                String messageStr = data == null ? null : new String(data, this.ENCODING);

                String[] splits = messageStr.split(",");
                if (splits.length != 3) {
                    throw new SerializationException("Invalid account message " + messageStr);
                }
                AccountMessage am = new AccountMessage();
                am.accountId = Integer.parseInt(splits[0]);
                am.balance = Integer.parseInt(splits[1]);
                am.eventTime = ZonedDateTime.parse(splits[2]);
                return am;
            } catch (UnsupportedEncodingException var4) {
                throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.ENCODING);
            }
        }
    }

}
