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

	public final static String PARAM_USER_ID = "user_id";
	public final static String PARAM_ITEM_ID = "item_id";
	public final static String PARAM_ORDER_ID = "order_id";
	public final static String PARAM_AMOUNT = "amount";
	public final static String PARAM_PRICE = "price";
	public final static String PARAM_PRODUCTS = "products";
	public final static String PARAM_STATE = "state";
	public final static String PARAM_MESSAGE = "message";
	public final static String PARAM_ORDER_PAID = "is_paid";
	public final static String PARAM_ORDER_CHECKOUT_STATUS = "checkout_status";
	public final static String PARAM_ORDER_CHECKOUT_PROGRESS = "checkout_progress";
	public final static String PARAM_USER_BALANCE = "balance";
	public final static String PARAM_USER_CHECKED = "user_checked";
	public final static String PARAM_USER_CREDITS = "user_credits";
	public final static String PARAM_USER_ORDERS = "orders";
	public final static String PARAM_PAYMENT_STATUS_TEXT = "status";
	public final static String PARAM_PAYMENT_STATUS_CODE = "status_code";
	public final static String PARAM_BATCH_ID = "batch_id";
	public final static String PARAM_BATCH_COUNT = "batch_count";
	public final static String PARAM_BATCH_PASS = "batch_pass";
	public final static String PARAM_RETURN_STATE = "return_state";

	public final static String KAFKA_DEFAULT_ADDRESS = "localhost:9092";
	public final static String ZOOKEEPER_DEFAULT_ADDRESS = "localhost:2181";

}