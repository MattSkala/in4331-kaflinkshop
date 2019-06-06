package kaflinkshop.Order;

public class OrderCheckoutProgress {

	public final static int NOT_PROCESSED = 0;
	public final static int AWAITING_SEND_STOCK = 1; // 1 << 0
	public final static int AWAITING_SEND_PAYMENT = 1 << 1; // 1 << 0
	public final static int AWAITING_REPLENISH_STOCK = 1 << 2;
	public final static int AWAITING_REPLENISH_PAYMENT = 1 << 3;
	public final static int FAILED_STOCK = 1 << 4;
	public final static int FAILED_PAYMENT = 1 << 5;
	public final static int SUCCEEDED_STOCK = 1 << 6;
	public final static int SUCCEEDED_PAYMENT = 1 << 7;
	public final static int REPLENISH_STOCK_SENT = 1 << 8;
	public final static int REPLENISH_PAYMENT_SENT = 1 << 9;

	public final static int AWAITING_SEND_BOTH = AWAITING_SEND_STOCK | AWAITING_SEND_PAYMENT;
	public final static int AWAITING_REPLENISH_BOTH = AWAITING_REPLENISH_STOCK | AWAITING_REPLENISH_PAYMENT;

	public final static int FAILED_BOTH = FAILED_STOCK | FAILED_PAYMENT;
	public final static int SUCCEEDED_BOTH = SUCCEEDED_STOCK | SUCCEEDED_PAYMENT;

	public static int combine(int a, int b) {
		return a | b;
	}

	public static boolean contains(int query, int target) {
		return (query & target) == target;
	}

	public static int remove(int query, int target) {
		return query & ~target;
	}

	public static boolean isAwaitingSend(int query) {
		return contains(query, AWAITING_SEND_PAYMENT) || contains(query, AWAITING_SEND_STOCK);
	}

	public static boolean isAwaitingReplenish(int query) {
		return contains(query, AWAITING_REPLENISH_PAYMENT) || contains(query, AWAITING_REPLENISH_STOCK);
	}

	public static boolean isAwaitingAnything(int query) {
		return isAwaitingSend(query) || isAwaitingReplenish(query);
	}

	public static boolean hasFailed(int query) {
		return contains(query, FAILED_STOCK) || contains(query, FAILED_PAYMENT);
	}

	public static boolean hasSucceeded(int query) {
		return contains(query, SUCCEEDED_STOCK) && contains(query, SUCCEEDED_PAYMENT);
	}

	public static boolean hasSentBothReplenishRequests(int query) {
		return contains(query, REPLENISH_STOCK_SENT) || contains(query, REPLENISH_PAYMENT_SENT);
	}

}
