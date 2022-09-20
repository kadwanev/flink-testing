package flinkstreaming.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;

public class AccountMessageDeserializer implements Deserializer<AccountMessage> {

    private String encoding = "UTF8";

    @Override
    public AccountMessage deserialize(String s, byte[] data) {
        try {
            String messageStr = data == null ? null : new String(data, this.encoding);

            String[] splits = messageStr.split(",");
            if (splits.length != 2) {
                throw new SerializationException("Invalid account message " + messageStr);
            }
            AccountMessage am = new AccountMessage();
            am.accountId = Integer.parseInt(splits[0]);
            am.balance = Integer.parseInt(splits[1]);
            return am;
        } catch (UnsupportedEncodingException var4) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
        }
    }
}
