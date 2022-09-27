package flinkstreaming.db;

import flinkstreaming.model.AccountMessage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SqliteSinkFunction<T> extends RichSinkFunction<T> {

    @Override
    public void invoke(T value, Context context) throws Exception {
        SqliteStore.getInstance().insertAccount((AccountMessage) value);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
