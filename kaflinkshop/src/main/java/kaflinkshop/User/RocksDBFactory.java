package kaflinkshop.User;
import kaflinkshop.CommunicationFactory;
import kaflinkshop.JobParams;
import kaflinkshop.SimpleJob;
import kaflinkshop.SimpleMessageKeyExtractor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

public class RocksDBFactory {

    public RocksDBFactory(){}

    public void setupRocksDB(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(new RocksDBStateBackend(filebackend, true));

    }

}
