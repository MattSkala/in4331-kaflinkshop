package kaflinkshop;

public class CommunicationFactory {

	public final static String API_VERSION = "api1";

	public final static String USER_OUT_TOPIC = "user_out_" + API_VERSION;
	public final static String ORDER_OUT_TOPIC = "user_out_" + API_VERSION;
	public final static String PAYMENT_OUT_TOPIC = "user_out_" + API_VERSION;
	public final static String STOCK_OUT_TOPIC = "user_out_" + API_VERSION;

	public final static String USER_IN_TOPIC = "user_in";
	public final static String ORDER_IN_TOPIC = "order_in";
	public final static String PAYMENT_IN_TOPIC = "payment_in";
	public final static String STOCK_IN_TOPIC = "stock_in";

	public final static String SERVICE_USER = "user";
	public final static String SERVICE_ORDER = "order";
	public final static String SERVICE_STOCK = "stock";
	public final static String SERVICE_PAYMENT = "payment";
	public final static String SERVICE_WEB = "web";

	public final static String KAFKA_DEFAULT_ADDRESS = "localhost:9092";
	public final static String ZOOKEEPER_DEFAULT_ADDRESS = "localhost:2181";

}