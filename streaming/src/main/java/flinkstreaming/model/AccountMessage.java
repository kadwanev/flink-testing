package flinkstreaming.model;

import java.io.Serializable;

public class AccountMessage implements Serializable {

    public int accountId;
    public int balance;

    @Override
    public String toString() {
        return "AccountMessage{" +
                "accountId=" + accountId +
                ", balance=" + balance +
                '}';
    }
}
