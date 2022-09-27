package flinkstreaming.db;

import flinkstreaming.model.AccountMessage;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public class SqliteSink<T> implements Sink<T,Object,Object,Object> {

    public static class SqliteSinkWriter<T> implements SinkWriter<T,Object,Object> {
        @Override
        public void write(T t, Context context) throws IOException {
            try {
                SqliteStore.getInstance().insertAccount((AccountMessage) t);
            } catch (SQLException ex) {
                throw new IOException("Error writing to sqlite", ex);
            }
        }

        @Override
        public List<Object> prepareCommit(boolean b) throws IOException {
            return List.of();
        }

        @Override
        public List<Object> snapshotState() throws IOException {
            return List.of();
        }

        @Override
        public void close() throws Exception { }
    }

    @Override
    public SinkWriter<T, Object, Object> createWriter(InitContext initContext, List<Object> list) throws IOException {
        return new SqliteSinkWriter<T>();
    }

    @Override
    public Optional<Committer<Object>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Object, Object>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Object>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Object>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Object>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
