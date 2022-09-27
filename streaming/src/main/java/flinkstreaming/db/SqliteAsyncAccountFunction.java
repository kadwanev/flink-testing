package flinkstreaming.db;

import flinkstreaming.model.AccountMessage;
import flinkstreaming.model.TransactionJAccount;
import flinkstreaming.model.TransactionMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class SqliteAsyncAccountFunction extends RichAsyncFunction<TransactionMessage, TransactionJAccount> {


    @Override
    public void open(Configuration parameters) throws Exception {
        SqliteStore.getInstance(); // Initialize instance giving thread safety
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(TransactionMessage key, final ResultFuture<TransactionJAccount> resultFuture) throws Exception {

        final int accountId = key.accountId;
        CompletableFuture.supplyAsync(() -> {
            int sleepMillis = new Random().nextInt(800);
            try {
                Thread.sleep(sleepMillis);
                Optional<AccountMessage> a = SqliteStore.getInstance().getAccount(accountId);
                return a;
            }
            catch (Exception ex) {
                throw new RuntimeException("Exception getting sqlite account after sleeping("+sleepMillis+")", ex);
            }
        })
                .thenAccept(oam -> {
                    resultFuture.complete(Collections.singleton(new TransactionJAccount(key, oam)));
                } )
                .get();

    }
}
