package flinkstreaming.model;

import java.io.Serializable;

public class TransactionMessage implements Serializable {

    public int accountId;
    public int amount;

    @Override
    public String toString() {
        return "TransactionMessage{" +
                "accountId=" + accountId +
                ", amount=" + amount +
                '}';
    }
}
