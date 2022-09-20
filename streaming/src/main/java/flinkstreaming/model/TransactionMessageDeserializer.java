package flinkstreaming.model;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;

public class TransactionMessageDeserializer implements Deserializer<TransactionMessage> {

    private String encoding = "UTF8";

    @Override
    public TransactionMessage deserialize(String s, byte[] data) {
        try {
            String messageStr = data == null ? null : new String(data, this.encoding);

            String[] splits = messageStr.split(",");
            if (splits.length != 2) {
                throw new SerializationException("Invalid account message " + messageStr);
            }
            TransactionMessage tm = new TransactionMessage();
            tm.accountId = Integer.parseInt(splits[0]);
            tm.amount = Integer.parseInt(splits[1]);
            return tm;
        } catch (UnsupportedEncodingException var4) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
        }
    }
}
