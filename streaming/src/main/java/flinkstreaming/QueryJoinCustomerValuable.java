package flinkstreaming;

import flinkstreaming.db.SqliteStore;
import flinkstreaming.db.SqliteStoringSinkFunctions;
import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.CustomerMessage;
import flinkstreaming.model.TransactionMessage;
import flinkstreaming.util.DelaySqliteSuppliers;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//  Implement the logic:
//      Customer Value is HIGH if:
//          Customer has set their notifyPreference to True AND
//          Customer has any account where the latest transaction amount > 50
//
public class QueryJoinCustomerValuable {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = Config.getStatefulEnvironment(args);
        env.setParallelism(3);

        String consumerGroupId = "queryJoinCustomerValuable";

        DataStreamSource<CustomerMessage> customersStreamSource =
                Config.getCustomersStream(consumerGroupId, env);
        DataStreamSource<AccountMessage> accountsStreamSource =
                Config.getAccountsStream(consumerGroupId, env);
        DataStreamSource<TransactionMessage> transactionStreamSource =
                Config.getTransactionsStream(consumerGroupId, env);


        AsyncDataStream.unorderedWait(customersStreamSource, new AsyncCustomerLoadCustomerAccountLastTransaction(), 10, TimeUnit.SECONDS, 2)
                .name("Customer joined to Account and latest transaction")
                .process(new CustomerValueProcessFunction.CustomerSource())
                .addSink(new SqliteStoringSinkFunctions.CustomerValueQueryStoring());

        AsyncDataStream.unorderedWait(accountsStreamSource, new AsyncAccountLoadCustomerAccountLastTransaction(), 10, TimeUnit.SECONDS, 2)
                .name("Account joined to Customer and latest transaction")
                .process(new CustomerValueProcessFunction.AccountSource())
                .addSink(new SqliteStoringSinkFunctions.CustomerValueQueryStoring());

        AsyncDataStream.unorderedWait(transactionStreamSource, new AsyncTransactionLoadCustomerAccountLastTransaction(), 10, TimeUnit.SECONDS, 2)
                .name("Transaction joined to Customer and latest transaction")
                .process(new CustomerValueProcessFunction.TransactionSource())
                .addSink(new SqliteStoringSinkFunctions.CustomerValueQueryStoring());

        env.execute("Calculate Customer Valuable via Sqlite Queries");

    }


    public static class CustomerValueProcessFunction {


        //        Customer Value is HIGH if:
//        Customer has set their notifyPreference to True AND
//        Customer has any account where the latest transaction amount > 50
        static Tuple2<CustomerMessage, String> executeLogic(Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>> values) {
//            System.out.println("Customer Value Process Function entered: " + data);

            if (values.f0.notifyPreference && values.f1.stream().anyMatch(at -> at.f1.amount > 50)) {
                return new Tuple2<>(values.f0, "HIGH");
            }
            return new Tuple2<>(values.f0, "LOW");
        }

        public static class CustomerSource extends ProcessFunction<Tuple2<CustomerMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>, Tuple4<CustomerMessage, String, String, LocalDateTime>> {

            @Override
            public void processElement(Tuple2<CustomerMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>> data, Context context, Collector<Tuple4<CustomerMessage, String, String, LocalDateTime>> out) throws Exception {

                if (data.f1.isPresent()) {
                    Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>> values = data.f1.get();
                    Tuple2<CustomerMessage, String> result = CustomerValueProcessFunction.executeLogic(values);
                    out.collect(new Tuple4<>(result.f0, result.f1, "CUST", data.f0.eventTime));
                }
            }
        }

        public static class AccountSource extends ProcessFunction<Tuple2<AccountMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>, Tuple4<CustomerMessage, String, String, LocalDateTime>> {

            @Override
            public void processElement(Tuple2<AccountMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>> data, Context context, Collector<Tuple4<CustomerMessage, String,String,LocalDateTime>> out) throws Exception {

                if (data.f1.isPresent()) {
                    Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>> values = data.f1.get();
                    Tuple2<CustomerMessage, String> result = CustomerValueProcessFunction.executeLogic(values);
                    out.collect(new Tuple4<>(result.f0, result.f1, "ACCT", data.f0.eventTime));
                }
            }
        }

        public static class TransactionSource extends ProcessFunction<Tuple2<TransactionMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>, Tuple4<CustomerMessage, String, String, LocalDateTime>> {

            @Override
            public void processElement(Tuple2<TransactionMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>> data, Context context, Collector<Tuple4<CustomerMessage, String, String, LocalDateTime>> out) throws Exception {

                if (data.f1.isPresent()) {
                    Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>> values = data.f1.get();
                    Tuple2<CustomerMessage, String> result = CustomerValueProcessFunction.executeLogic(values);
                    out.collect(new Tuple4<>(result.f0, result.f1, "TRAN", data.f0.eventTime));
                }
            }
        }
    }

    // Customer Message has come in, join to accounts, then load transactions and select last transaction for each account
    public static class AsyncCustomerLoadCustomerAccountLastTransaction extends RichAsyncFunction<CustomerMessage, Tuple2<CustomerMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            SqliteStore.getInstance(); // Initialize instance giving thread safety
        }

        @Override
        public void asyncInvoke(final CustomerMessage customerMessage, ResultFuture<Tuple2<CustomerMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>> resultFuture) throws Exception {

            CompletableFuture.supplyAsync(new DelaySqliteSuppliers.AccountListSupplier<>(customerMessage.customerId, customerMessage), Config.EXECUTOR_SERVICE)
                    .thenAccept(accountsResult -> {
                        final List<AccountMessage> accounts = accountsResult.f1;

                        // Query all Account transactions from found accounts
                        final List<CompletableFuture<Tuple2<AccountMessage, List<TransactionMessage>>>> futures =
                                accounts.stream().map(account ->
                                        CompletableFuture.supplyAsync(new DelaySqliteSuppliers.TransactionsSupplier<>(account.accountId, account), Config.EXECUTOR_SERVICE)
                                ).collect(Collectors.toList());

                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                .thenAccept(unused -> {
                                    List<Tuple2<AccountMessage, TransactionMessage>> results =
                                            futures.stream().map(f -> {
                                                // Convert account's List<TransactionMessage>s to latest TransactionMessage
                                                try {
                                                    Tuple2<AccountMessage, List<TransactionMessage>> accountTransactions = f.get();
                                                    return new Tuple2<>(accountTransactions.f0, accountTransactions.f1.get(0));
                                                } catch (Exception ex) {
                                                    throw new RuntimeException("Failed to collect results", ex);
                                                }
                                            }).collect(Collectors.toList());

                                    resultFuture.complete(Collections.singleton(new Tuple2<>(customerMessage, Optional.of(new Tuple2<>(customerMessage, results)))));
                                });
                    })
                    .get();
        }
    }

    // Account Message has come in, join to accounts, then load transactions and select last transaction for each account
    public static class AsyncAccountLoadCustomerAccountLastTransaction extends RichAsyncFunction<AccountMessage, Tuple2<AccountMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            SqliteStore.getInstance(); // Initialize instance giving thread safety
        }

        @Override
        public void asyncInvoke(final AccountMessage accountMessage, ResultFuture<Tuple2<AccountMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>> resultFuture) throws Exception {

            // First load Customer
            CompletableFuture.supplyAsync(new DelaySqliteSuppliers.CustomerSupplier<Integer>(accountMessage.customerId, accountMessage.customerId), Config.EXECUTOR_SERVICE)
                    .thenAccept(customerResult -> {
                        if (customerResult.f1.isEmpty()) {
                            resultFuture.complete(Collections.singleton(new Tuple2<>(accountMessage, Optional.empty())));
                        } else {
                            final CustomerMessage customerMessage = customerResult.f1.get();

                            // Then load Accounts
                            CompletableFuture.supplyAsync(new DelaySqliteSuppliers.AccountListSupplier<>(accountMessage.customerId, accountMessage.customerId), Config.EXECUTOR_SERVICE)
                                    .thenAccept(accountsResult -> {
                                        final List<AccountMessage> accounts = accountsResult.f1;
                                        // Find Loaded account and replace with current value
                                        for (int i = 0; i < accounts.size(); i++) {
                                            if (accounts.get(i).accountId == accountMessage.accountId) {
                                                accounts.set(i, accountMessage);
                                            }
                                        }

                                        // Query all transactions from found accounts
                                        final CompletableFuture[] futures = new CompletableFuture[accounts.size()];
                                        for (int i = 0; i < accounts.size(); i++) {
                                            AccountMessage account = accounts.get(i);
                                            futures[i] = CompletableFuture.supplyAsync(new DelaySqliteSuppliers.TransactionsSupplier<>(account.accountId, account), Config.EXECUTOR_SERVICE);
                                        }

                                        CompletableFuture.allOf(futures)
                                                .thenAccept(unused -> {
                                                    List<Tuple2<AccountMessage, TransactionMessage>> results = new ArrayList<>(accounts.size());
                                                    for (int i = 0; i < futures.length; i++) {
                                                        Tuple2<AccountMessage, List<TransactionMessage>> accountTransactions =
                                                                (Tuple2<AccountMessage, List<TransactionMessage>>) futures[i].join();
                                                        results.add(new Tuple2<>(accountTransactions.f0, accountTransactions.f1.get(0)));
                                                    }

                                                    resultFuture.complete(Collections.singleton(new Tuple2<>(accountMessage, Optional.of(new Tuple2<>(customerMessage, results)))));
                                                });
                                    })
                                    .join();
                        }
                    })
                    .get();
        }

    }

    // Transaction Message has come in, join to accounts, then load transactions and select last transaction for each account
    public static class AsyncTransactionLoadCustomerAccountLastTransaction extends RichAsyncFunction<TransactionMessage, Tuple2<TransactionMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>> {

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            SqliteStore.getInstance(); // Initialize instance giving thread safety
        }

        @Override
        public void asyncInvoke(final TransactionMessage transactionMessage, ResultFuture<Tuple2<TransactionMessage, Optional<Tuple2<CustomerMessage, List<Tuple2<AccountMessage, TransactionMessage>>>>>> resultFuture) throws Exception {

            // Query Account for Customer Id
            CompletableFuture.supplyAsync(new DelaySqliteSuppliers.AccountSupplier<>(transactionMessage.accountId, transactionMessage.accountId), Config.EXECUTOR_SERVICE)
                    .thenAccept(accountResult -> {
                        if (accountResult.f1.isEmpty()) {
                            resultFuture.complete(Collections.singleton(new Tuple2<>(transactionMessage, Optional.empty())));
                        } else {
                            final AccountMessage transactionAccount = accountResult.f1.get();

                            CompletableFuture.supplyAsync(new DelaySqliteSuppliers.CustomerSupplier<Integer>(transactionAccount.customerId, transactionAccount.customerId), Config.EXECUTOR_SERVICE)
                                    .thenAccept(customerResult -> {
                                        if (customerResult.f1.isEmpty()) {
                                            resultFuture.complete(Collections.singleton(new Tuple2<>(transactionMessage, Optional.empty())));
                                        } else {
                                            final CustomerMessage customerMessage = customerResult.f1.get();

                                            CompletableFuture.supplyAsync(new DelaySqliteSuppliers.AccountListSupplier<CustomerMessage>(customerMessage.customerId, customerMessage))
                                                    .thenAccept(accountsResult -> {
                                                        final List<AccountMessage> accounts = accountsResult.f1;
                                                        // Query all Account transactions from found accounts
                                                        final List<CompletableFuture<Tuple2<AccountMessage, List<TransactionMessage>>>> futures =
                                                                accounts.stream().map(account ->
                                                                        CompletableFuture.supplyAsync(new DelaySqliteSuppliers.TransactionsSupplier<>(account.accountId, account), Config.EXECUTOR_SERVICE)
                                                                ).collect(Collectors.toList());
                                                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                                                .thenAccept(unused -> {
                                                                    List<Tuple2<AccountMessage, TransactionMessage>> results =
                                                                            futures.stream().map(f -> {
                                                                                // Convert account's List<TransactionMessage>s to latest TransactionMessage
                                                                                // Apply current transaction as latest for account
                                                                                try {
                                                                                    Tuple2<AccountMessage, List<TransactionMessage>> accountTransactions = f.get();
                                                                                    if (accountTransactions.f0.accountId == transactionMessage.accountId) {
                                                                                        return new Tuple2<>(accountTransactions.f0, transactionMessage);
                                                                                    } else {
                                                                                        return new Tuple2<>(accountTransactions.f0, accountTransactions.f1.get(0));
                                                                                    }
                                                                                } catch (Exception ex) {
                                                                                    throw new RuntimeException("Failed to collect results", ex);
                                                                                }
                                                                            }).collect(Collectors.toList());

                                                                    resultFuture.complete(Collections.singleton(new Tuple2<>(transactionMessage, Optional.of(new Tuple2<>(customerMessage, results)))));
                                                                });

                                                    }).join();
                                        }
                                    })
                                    .join();
                        }
                    })
                    .get();
        }
    }


}
