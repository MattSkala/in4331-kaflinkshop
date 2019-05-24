package kaflinkshop;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class CommunicationFactory {
    public final static String API_VERSION = "api1";

    public final static String USER_OUT_TOPIC = "user_out_"+API_VERSION;
    public final static String ORDER_OUT_TOPIC = "user_out_"+API_VERSION;
    public final static String PAYMENT_OUT_TOPIC = "user_out_"+API_VERSION;
    public final static String STOCK_OUT_TOPIC = "user_out_"+API_VERSION;

    public final static String USER_IN_TOPIC = "user_in";
    public final static String ORDER_IN_TOPIC = "order_in";
    public final static String PAYMENT_IN_TOPIC = "payment_in";
    public final static String STOCK_IN_TOPIC = "stock_in";


    public static FlinkKafkaProducer011<Tuple2<String, String>> createProducer(
            String topic, String kafkaAddress) {

        return new FlinkKafkaProducer011<>(kafkaAddress,
                topic, new KeyedSerializationSchema<Tuple2<String, String>>() {
            @Override
            public byte[] serializeKey(Tuple2<String, String> element) {
                return null;
            }

            @Override
            public byte[] serializeValue(Tuple2<String, String> element) {
                return element.f1.getBytes();
            }

            @Override
            public String getTargetTopic(Tuple2<String, String> element) {
                System.out.println("Sending " + element.f1 + " to " + element.f0);
                return element.f0;
            }
        });
    }

    public static Tuple2<String, String> createOutput(String topic, String output){
        return new Tuple2<>(topic, output);
    }
}