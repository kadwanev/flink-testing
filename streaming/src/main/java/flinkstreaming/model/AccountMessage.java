package flinkstreaming.model;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public class AccountMessage implements Serializable {

    public int accountId;
    public int customerId;
    public String message;
    public LocalDateTime eventTime;

    @Override
    public String toString() {
        return "AccountMessage{" +
                "accountId=" + accountId +
                ", customerId='" + customerId + '\'' +
                ", message='" + message + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }

    public static Schema tableSchema() {
        return Schema.newBuilder()
                .column("accountId", DataTypes.INT().notNull())
                .column("customerId", DataTypes.INT().notNull())
                .column("message", DataTypes.STRING().notNull())
                .column("eventTime", DataTypes.TIMESTAMP().notNull())
                .primaryKey("accountId")
                .build();
    }

    public static class AccountMessageDeserializer implements Deserializer<AccountMessage> {

        private static final String ENCODING = "UTF8";

        @Override
        public AccountMessage deserialize(String s, byte[] data) {
            try {
                String messageStr = data == null ? null : new String(data, this.ENCODING);

                String[] splits = messageStr.split(",");
                if (splits.length != 4) {
                    throw new SerializationException("Invalid account message " + messageStr);
                }
                AccountMessage am = new AccountMessage();
                am.accountId = Integer.parseInt(splits[0]);
                am.customerId = Integer.parseInt(splits[1]);
                am.message = splits[2];
                am.eventTime = ZonedDateTime.parse(splits[3]).toLocalDateTime();
                return am;
            } catch (UnsupportedEncodingException var4) {
                throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.ENCODING);
            }
        }
    }

}
