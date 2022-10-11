package flinkstreaming.model;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class TransactionMessage implements Serializable {

    public int accountId;
    public int amount;
    public LocalDateTime eventTime;

    @Override
    public String toString() {
        return "TransactionMessage{" +
                "accountId=" + accountId +
                ", amount=" + amount +
                ", eventTime=" + eventTime +
                ", eventTimeMillis=" + eventTime.toInstant(ZoneOffset.UTC).toEpochMilli() +
                '}';
    }

    public static Schema tableSchema() {
        return Schema.newBuilder()
                .column("accountId", DataTypes.INT().notNull())
                .column("amount", DataTypes.INT().notNull())
                .column("eventTime", DataTypes.TIMESTAMP().notNull())
                .build();
    }

    public static class TransactionMessageDeserializer implements Deserializer<TransactionMessage> {

        private static final String ENCODING = "UTF8";

        @Override
        public TransactionMessage deserialize(String s, byte[] data) {
            try {
                String messageStr = data == null ? null : new String(data, this.ENCODING);

                String[] splits = messageStr.split(",");
                if (splits.length != 3) {
                    throw new SerializationException("Invalid transaction message " + messageStr);
                }
                TransactionMessage tm = new TransactionMessage();
                tm.accountId = Integer.parseInt(splits[0]);
                tm.amount = Integer.parseInt(splits[1]);
                tm.eventTime = ZonedDateTime.parse(splits[2]).toLocalDateTime();
                return tm;
            } catch (UnsupportedEncodingException var4) {
                throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.ENCODING);
            }
        }
    }

}
