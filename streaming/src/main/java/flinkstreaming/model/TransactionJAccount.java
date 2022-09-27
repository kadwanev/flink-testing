package flinkstreaming.model;

import java.util.Optional;

public class TransactionJAccount {
    public TransactionMessage tm;
    public Optional<AccountMessage> am;

    public TransactionJAccount(TransactionMessage tm, Optional<AccountMessage> am) {
        this.tm = tm;
        this.am = am;
    }

    @Override
    public String toString() {
        return "TransactionJAccount{" +
                "tm=" + tm +
                ", am=" + am +
                '}';
    }
}
