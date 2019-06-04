package kaflinkshop.Order;

import kaflinkshop.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

import static kaflinkshop.CommunicationFactory.*;
import static kaflinkshop.Payment.PaymentQueryProcess.*;

public class OrderQueryProcess extends QueryProcess {

	public static final String STATE_USER_NOT_EXISTS = "no-user";
	public static final String STATE_USER_ORDER_REMOVED = "user-order-removed";
	public static final String STATE_USER_ORDER_ADDED = "user-order-added";
	public static final String STATE_ITEM_EXISTS = "item-exists";
	public static final String STATE_ITEM_NOT_EXISTS = "no-item";
	public static final String ENTITY_NAME = "order";

	/**
	 * The state that is maintained by this process function.
	 */
	private ValueState<OrderState> state;
	private ObjectMapper objectMapper;

	public OrderQueryProcess() {
		super(CommunicationFactory.SERVICE_ORDER);
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public void open(Configuration parameters) {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("order_state", OrderState.class));
	}

	@Override
	public List<QueryProcessResult> processElement(Message message, Context context) throws Exception {
		String orderID = context.getCurrentKey();
		String route = message.state.route;
		QueryProcessResult result;

		switch (route) {
			case "orders/find":
				result = findOrder(message);
				break;
			case "orders/create":
				result = createOrder(orderID, message);
				break;
			case "orders/remove":
				result = removeOrder(message);
				break;
			case "orders/addItem":
				result = addItem(message);
				break;
			case "orders/removeItem":
				result = removeItem(message);
				break;
			case "orders/checkout":
				result = checkoutOrder();
				break;

			// Payment logic
			case "payment/pay":
				return paymentPay(message);
			case "payment/cancelPayment":
				result = paymentCancel(message);
				break;

			case "pass":
				result = new QueryProcessResult.Success(message.params, "pass");
				break;

			// Error route handler
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
	}

	private QueryProcessResult paymentCancel(Message message) throws Exception {
		OrderState current = state.value();

		if (current == null)
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_ORDER_NOT_EXISTS);

		ObjectNode params = message.params.deepCopy();
		params.put(PARAM_USER_ID, current.userID);
		params.put(PARAM_PRICE, current.countTotalItems());
		return new QueryProcessResult.Redirect(
				PAYMENT_IN_TOPIC,
				message.state.route,
				params,
				STATE_PAYMENT_ORDER_EXISTS);
	}

	private List<QueryProcessResult> paymentPay(Message message) throws Exception {
		OrderState current = state.value();

		if (message.state.state == null)
			throw new ServiceException.IllegalStateException(null);

		if (message.state.state.equals(STATE_PAYMENT_ORDER_VALIDATE)) {
			if (current == null)
				return Collections.singletonList(
						new QueryProcessResult.Redirect(
								PAYMENT_IN_TOPIC,
								message.state.route,
								message.params,
								STATE_PAYMENT_ORDER_NOT_EXISTS));

			{
				// check if all items exist
				// TODO: fix this
				ObjectNode params = message.params.deepCopy();
				params.set("products", current.getProductsAsJson(objectMapper));
				if (true)
					return Collections.singletonList(
							new QueryProcessResult.Redirect(
									STOCK_IN_TOPIC,
									"batch/validate",
									params,
									"batch:validate"));
			}

			// TODO: check if all items exist?
			long price = current.countTotalItems();

			ObjectNode params = message.params.deepCopy();
			params.put(PARAM_USER_ID, current.userID);
			params.put(PARAM_PRICE, price);

			return Collections.singletonList(
					new QueryProcessResult.Redirect(
							PAYMENT_IN_TOPIC,
							message.state.route,
							params,
							STATE_PAYMENT_ORDER_EXISTS));
		}

		if (message.state.state.equals(STATE_PAYMENT_ORDER_MARK_PAID)) {
			if (current == null)
				throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

			current.isPaid = true;
			state.update(current);

			return Collections.emptyList(); // no response needed - if this fails, well, it fails
		}

		throw new ServiceException.IllegalStateException(message.state.state);
	}

	private QueryProcessResult findOrder(Message message) throws Exception {
		OrderState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		// check if all items exist
		// TODO: remove this
		ObjectNode params = message.params.deepCopy();
		params.set("products", current.getProductsAsJson(objectMapper));
		if (true)
			return new QueryProcessResult.Redirect(
					STOCK_IN_TOPIC,
					"batch/validate",
					params,
					"batch:validate");

		return successResult(current, null);
	}

	private QueryProcessResult createOrder(String orderID, Message message) throws Exception {
		OrderState current = state.value();
		String userID = message.params.get(PARAM_USER_ID).asText();

		if (current == null) { // create new order

			if (message.state.state != null) // state should be empty/null
				throw new ServiceException.IllegalStateException(message.state.state);

			current = new OrderState(orderID, userID);
			state.update(current);
			ObjectNode newParams = message.params.deepCopy();
			newParams.put(PARAM_ORDER_ID, orderID);
			// TODO: set some timer!
			return new QueryProcessResult.Redirect(
					USER_IN_TOPIC,
					message.state.route,
					newParams,
					"order-create-check-user");

		}

		if (message.state.state == null)
			throw new ServiceException("Order already exists.");

		if (message.state.state.equals(STATE_USER_ORDER_ADDED)) {
			current.userChecked = true;
			state.update(current);
			return successResult(current, "Order created.");
		}

		if (message.state.state.equals(STATE_USER_NOT_EXISTS)) {
			state.update(null);
			return failureResult(current, "Order not created. User did not exist.");
		}

		throw new ServiceException.IllegalStateException(message.state.state);
	}

	private QueryProcessResult removeOrder(Message message) throws Exception {
		OrderState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (current.isPaid)
			throw new ServiceException("Cannot remove order that has already been paid.");

		if (message.state.state == null) { // initiate order removal process
			ObjectNode newParams = message.params.deepCopy();
			newParams.put(PARAM_ORDER_ID, current.orderID);
			newParams.put(PARAM_USER_ID, current.userID);
			// TODO: set some timer!
			return new QueryProcessResult.Redirect(
					USER_IN_TOPIC,
					message.state.route,
					newParams,
					"check-remove");
		}

		if (message.state.state.equals(STATE_USER_NOT_EXISTS) || message.state.state.equals(STATE_USER_ORDER_REMOVED)) {
			state.update(null);
			return new QueryProcessResult.Success("Order removed.");
		}

		throw new ServiceException.IllegalStateException(message.state.state);
	}

	private QueryProcessResult addItem(Message message) throws Exception {
		OrderState current = state.value();
		String itemID = message.params.get(PARAM_ITEM_ID).asText();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (current.isPaid)
			throw new ServiceException("Cannot modify an order that has already been paid.");

		int items = current.products.getOrDefault(itemID, 0);
		current.products.put(itemID, items + 1);
		state.update(current);
		return successResult(current, null);

		// if (message.state.state == null) {
		// 	// initiate item the process of adding an item
		// 	ObjectNode newParams = message.params.deepCopy();
		// 	newParams.put(PARAM_ORDER_ID, current.orderID);
		// 	newParams.put(PARAM_ITEM_ID, itemID);
		// 	return new QueryProcessResult.Redirect(STOCK_IN_TOPIC, message.state.route, newParams);
		// }
		//
		// // check if item exists (callback from the stock service)
		// if (message.state.state.equals(STATE_ITEM_EXISTS)) {
		// 	int items = current.products.getOrDefault(itemID, 0);
		// 	current.products.put(itemID, items + 1);
		// 	state.update(current);
		// 	return successResult(current, "Item added.");
		// }
		//
		// // check if item does not exist (callback from the stock service)
		// if (message.state.state.equals(STATE_ITEM_NOT_EXISTS)) {
		// 	current.products.remove(itemID);
		// 	state.update(current);
		// 	return failureResult(current, "Item does not exist.");
		// }
		//
		// throw new ServiceException.IllegalStateException(message.state.state);
	}

	private QueryProcessResult removeItem(Message message) throws Exception {
		OrderState current = state.value();
		String itemID = message.params.get(PARAM_ITEM_ID).asText();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (current.isPaid)
			throw new ServiceException("Cannot modify an order that has already been paid.");

		Integer value = current.products.remove(itemID);
		state.update(current);

		if (value == null) {
			return failureResult(current, "No such item exists.");
		} else {
			return successResult(current, "Item(s) removed.");
		}
	}

	private QueryProcessResult checkoutOrder() throws Exception {
		// TODO: implement
		throw new ServiceException("Order checkout not yet implemented.");
	}

	private QueryProcessResult successResult(OrderState state, @Nullable String msg) {
		return new QueryProcessResult.Success(state.toJsonNode(this.objectMapper), msg);
	}

	private QueryProcessResult failureResult(OrderState state, @Nullable String msg) {
		return new QueryProcessResult.Failure(state.toJsonNode(this.objectMapper), msg);
	}

}
