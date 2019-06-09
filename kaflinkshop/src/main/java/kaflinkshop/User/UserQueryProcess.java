package kaflinkshop.User;

import kaflinkshop.*;
import kaflinkshop.Order.OrderQueryProcess;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static kaflinkshop.CommunicationFactory.*;
import static kaflinkshop.Order.OrderQueryProcess.*;
import static kaflinkshop.Payment.PaymentQueryProcess.*;

public class UserQueryProcess extends QueryProcess {

	public static final String ENTITY_NAME = "user";

	/**
	 * The state that is maintained by this process function.
	 */
	private ValueState<UserState> state;
	private ObjectMapper objectMapper;

	public UserQueryProcess() {
		super(CommunicationFactory.SERVICE_USER);
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public void open(Configuration parameters) {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("user_state", UserState.class));
	}

	@Override
	public List<QueryProcessResult> processElement(Message message, Context context) throws Exception {
		String userID = context.getCurrentKey();
		String route = message.state.route;
		QueryProcessResult result;

		switch (route) {
			case "users/find":
				result = findUser();
				break;
			case "users/create":
				result = createUser(userID);
				break;
			case "users/remove":
				return removeUser();
			case "users/credit":
				result = getCredits();
				break;
			case "users/credit/add":
				result = addCredits(message);
				break;
			case "users/credit/subtract":
				result = subtractCredits(message);
				break;

			// Order logic
			case "orders/create":
				result = createOrder(message);
				break;
			case "orders/remove":
				result = removeOrder(message);
				break;
			case "orders/checkout":
				result = orderCheckout(message);
				break;

			// Payment logic
			case "payment/pay":
				result = paymentPay(message);
				break;
			case "payment/cancelPayment":
				result = paymentCancel(message);
				break;

			// Error route handler
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
	}

	private QueryProcessResult orderCheckout(Message message) throws Exception {
		if (message.state.state == null)
			throw new ServiceException("Unknown message state.");

		switch (message.state.state) {
			case OrderQueryProcess.STATE_CHECKOUT_SEND_PAYMENT:
				return checkoutPay(message);
			case OrderQueryProcess.STATE_CHECKOUT_REPLENISH_PAYMENT:
				return checkoutCancel(message);
			default:
				throw new ServiceException.IllegalStateException(message.state.state);
		}
	}

	private QueryProcessResult checkoutPay(Message message) throws Exception {
		UserState current = state.value();
		ObjectNode params = message.params.deepCopy();

		if (current == null) {
			params.put(PARAM_STATE, OperationResult.ERROR.getCode());
			params.put(PARAM_MESSAGE, "User does not exist.");
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					message.state.state);
		}

		long price = message.params.get(PARAM_PRICE).asLong();

		if (price < 0) { // we allow zero here - perhaps there's a free product
			params.put(PARAM_STATE, OperationResult.ERROR.getCode());
			params.put(PARAM_MESSAGE, "Price cannot be negative.");
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					message.state.state);
		}

		if (current.credits >= price) {
			current.credits -= price;
			state.update(current);
			params.put(PARAM_STATE, OperationResult.SUCCESS.getCode());
			params.put(PARAM_MESSAGE, "Credits subtracted.");
			params.put(PARAM_USER_CREDITS, current.credits);
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					message.state.state);
		} else {
			params.put(PARAM_STATE, OperationResult.FAILURE.getCode());
			params.put(PARAM_MESSAGE, "Not enough credits.");
			params.put(PARAM_USER_CREDITS, current.credits);
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					message.state.state);
		}
	}

	private QueryProcessResult checkoutCancel(Message message) throws Exception {
		UserState current = state.value();
		ObjectNode params = message.params.deepCopy();

		if (current == null) {
			params.put(PARAM_STATE, OperationResult.ERROR.getCode());
			params.put(PARAM_MESSAGE, "User does not exist.");
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					message.state.state);
		}

		long price = message.params.get(PARAM_PRICE).asLong();

		if (price < 0) { // we allow zero here - perhaps there's a free product
			params.put(PARAM_STATE, OperationResult.ERROR.getCode());
			params.put(PARAM_MESSAGE, "Price cannot be negative.");
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					message.state.state);
		}

		current.credits += price;
		state.update(current);

		params.put(PARAM_STATE, OperationResult.SUCCESS.getCode());
		params.put(PARAM_MESSAGE, "Credits added.");
		params.put(PARAM_USER_CREDITS, current.credits);
		return new QueryProcessResult.Redirect(
				PAYMENT_IN_TOPIC,
				message.state.route,
				params,
				message.state.state);
	}

	private QueryProcessResult paymentCancel(Message message) throws Exception {
		UserState current = state.value();

		if (current == null)
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_USER_NOT_EXISTS);

		long price = message.params.get(PARAM_PRICE).asLong();

		if (price < 0)
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_USER_PRICE_ERROR);

		current.credits += price;
		state.update(current);

		return new QueryProcessResult.Redirect(
				PAYMENT_IN_TOPIC,
				message.state.route,
				message.params,
				STATE_PAYMENT_USER_CREDITS_ADDED);
	}

	private QueryProcessResult paymentPay(Message message) throws Exception {
		UserState current = state.value();

		if (current == null)
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_USER_NOT_EXISTS);

		long price = message.params.get(PARAM_PRICE).asLong();

		if (price < 0) // we allow zero here - perhaps there's a free product
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_USER_PRICE_ERROR);

		if (current.credits >= price) {
			current.credits -= price;
			state.update(current);
			ObjectNode params = message.params.deepCopy();
			params.put(PARAM_USER_CREDITS, current.credits);
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					STATE_PAYMENT_USER_CREDITS_SUBTRACTED);
		} else {
			ObjectNode params = message.params.deepCopy();
			params.put(PARAM_USER_CREDITS, current.credits);
			return new QueryProcessResult.Redirect(
					PAYMENT_IN_TOPIC,
					message.state.route,
					params,
					STATE_PAYMENT_USER_NO_CREDITS);
		}
	}

	private QueryProcessResult removeOrder(Message message) throws Exception {
		UserState current = state.value();

		if (current == null)
			return new QueryProcessResult.Redirect(
					ORDER_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_USER_NOT_EXISTS);

		current.orders.remove(message.params.get(PARAM_ORDER_ID).asText());
		state.update(current);

		return new QueryProcessResult.Redirect(
				ORDER_IN_TOPIC,
				message.state.route,
				message.params,
				STATE_USER_ORDER_REMOVED);
	}

	private QueryProcessResult createOrder(Message message) throws Exception {
		UserState current = state.value();

		if (current == null)
			return new QueryProcessResult.Redirect(
					ORDER_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_USER_NOT_EXISTS);

		current.orders.add(message.params.get(PARAM_ORDER_ID).asText());
		state.update(current);

		return new QueryProcessResult.Redirect(
				ORDER_IN_TOPIC,
				message.state.route,
				message.params,
				STATE_USER_ORDER_ADDED);
	}

	private QueryProcessResult findUser() throws Exception {
		UserState current = state.value(); // retrieve the state

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		return successResult(current, null);
	}

	private QueryProcessResult createUser(String userID) throws Exception {
		UserState current = state.value();

		if (current != null)
			throw new ServiceException("User already exists.");

		current = new UserState(userID);
		state.update(current); // update the state

		return successResult(current, "User created.");
	}

	private List<QueryProcessResult> removeUser() throws Exception {
		UserState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		List<QueryProcessResult> out_messages = new ArrayList<>();

		Iterator<String> orderIterator = current.orders.iterator();

		while(orderIterator.hasNext()){
			String orderId = orderIterator.next();

			ObjectNode params = objectMapper.createObjectNode();
			params.put(PARAM_ORDER_ID, orderId);
			out_messages.add(new QueryProcessResult.Redirect(
					ORDER_IN_TOPIC,
					"orders/remove",
					params,
					STATE_USER_ORDER_REMOVED));
		}
		state.update(null);

		out_messages.add(new QueryProcessResult.Success("User removed."));
		return out_messages;
	}

	private QueryProcessResult getCredits() throws Exception {
		UserState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		return successResult(current, null);
	}

	private QueryProcessResult addCredits(Message message) throws Exception {
		UserState current = state.value();
		long amount = message.params.get(PARAM_AMOUNT).asLong();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (amount <= 0)
			throw new ServiceException("Amount must be positive.");

		current.credits += amount;
		state.update(current);

		return successResult(current, "Credits added.");
	}

	private QueryProcessResult subtractCredits(Message message) throws Exception {
		UserState current = state.value();
		long amount = message.params.get(PARAM_AMOUNT).asLong();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (amount <= 0)
			throw new ServiceException("Amount must be positive.");

		if (current.credits < amount)
			throw new ServiceException("Not enough credits.");

		current.credits -= amount;
		state.update(current);

		return successResult(current, "Credits subtracted.");
	}

	private QueryProcessResult successResult(UserState state, @Nullable String msg) {
		return new QueryProcessResult.Success(state.toJsonNode(this.objectMapper), msg);
	}

	private QueryProcessResult failureResult(UserState state, @Nullable String msg) {
		return new QueryProcessResult.Failure(state.toJsonNode(this.objectMapper), msg);
	}

}
