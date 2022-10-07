package flinkstreaming.util;

import flinkstreaming.db.SqliteStore;
import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;

public class DelaySqliteSuppliers {

    public static class CustomerSupplier<K> implements Supplier<Tuple2<K, Optional<CustomerMessage>>> {
        private int customerId;
        private K key;

        public CustomerSupplier(int customerId, K key) {
            this.customerId = customerId;
            this.key = key;
        }

        @Override
        public Tuple2<K,Optional<CustomerMessage>> get() {
            int sleepMillis = new Random().nextInt(300);
            try {
                Thread.sleep(sleepMillis);
                return new Tuple2(this.key, SqliteStore.getInstance().getCustomer(this.customerId));
            }
            catch (Exception ex) {
                throw new RuntimeException("Exception getting sqlite customer after sleeping("+sleepMillis+")", ex);
            }
        }
    }


    public static class AccountListSupplier<K> implements Supplier<Tuple2<K,List<AccountMessage>>> {
        private int customerId;
        private K key;

        public AccountListSupplier(int customerId, K key) {
            this.customerId = customerId;
            this.key = key;
        }

        @Override
        public Tuple2<K,List<AccountMessage>> get() {
            int sleepMillis = new Random().nextInt(300);
            try {
                Thread.sleep(sleepMillis);
                return new Tuple2(this.key, SqliteStore.getInstance().getCustomerAccounts(this.customerId));
            }
            catch (Exception ex) {
                throw new RuntimeException("Exception getting sqlite customer accounts after sleeping("+sleepMillis+")", ex);
            }

        }
    }

    public static class AccountSupplier<K> implements Supplier<Tuple2<K,Optional<AccountMessage>>> {
        private int accountId;
        private K key;

        public AccountSupplier(int accountId, K key) {
            this.accountId = accountId;
            this.key = key;
        }

        @Override
        public Tuple2<K,Optional<AccountMessage>> get() {
            int sleepMillis = new Random().nextInt(300);
            try {
                Thread.sleep(sleepMillis);
                return new Tuple2(this.key, SqliteStore.getInstance().getAccount(this.accountId));
            }
            catch (Exception ex) {
                throw new RuntimeException("Exception getting sqlite account after sleeping("+sleepMillis+")", ex);
            }

        }
    }

    public static class TransactionsSupplier<K> implements Supplier<Tuple2<K,List<TransactionMessage>>> {
        private int accountId;
        private K key;

        public TransactionsSupplier(int accountId, K key) {
            this.accountId = accountId;
            this.key = key;
        }

        @Override
        public Tuple2<K,List<TransactionMessage>> get() {
            int sleepMillis = new Random().nextInt(300);
            try {
                Thread.sleep(sleepMillis);
                return new Tuple2(this.key, SqliteStore.getInstance().getAccountTransactions(this.accountId));
            }
            catch (Exception ex) {
                throw new RuntimeException("Exception getting sqlite account transactions after sleeping("+sleepMillis+")", ex);
            }

        }
    }

}
