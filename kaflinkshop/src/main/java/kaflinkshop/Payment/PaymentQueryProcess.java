package kaflinkshop.Payment;

import kaflinkshop.*;
import kaflinkshop.Order.OrderQueryProcess;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static kaflinkshop.CommunicationFactory.*;

public class PaymentQueryProcess extends QueryProcess {

	public static final String STATE_PAYMENT_ORDER_EXISTS = "order-exists";
	public static final String STATE_PAYMENT_ORDER_NOT_EXISTS = "no-order";
	public static final String STATE_PAYMENT_USER_NOT_EXISTS = "no-user";
	public static final String STATE_PAYMENT_USER_NO_CREDITS = "no-user-credits";
	public static final String STATE_PAYMENT_USER_CREDITS_SUBTRACTED = "user-credits-subtracted";
	public static final String STATE_PAYMENT_USER_CREDITS_ADDED = "user-credits-subtracted-added";
	public static final String STATE_PAYMENT_USER_PRICE_ERROR = "user-credits-price-error";
	public static final String STATE_PAYMENT_ORDER_VALIDATE = "order-validate";
	public static final String STATE_PAYMENT_ORDER_MARK_PAID = "order-mark-paid";
	public static final String STATE_PAYMENT_USER_PAY = "validate-user-subtract-credits";
	public static final String STATE_PAYMENT_USER_REFUND = "add-user-credits";
	public static final String ENTITY_NAME = "payment";

	/**
	 * The state that is maintained by this process function.
	 */
	private ValueState<PaymentState> state;
	private ObjectMapper objectMapper;

	@Override
	public void open(Configuration parameters) {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("payment_state", PaymentState.class));
	}

	public PaymentQueryProcess() {
		super(CommunicationFactory.SERVICE_PAYMENT);
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public List<QueryProcessResult> processElement(Message message, Context context) throws Exception {
		String orderID = context.getCurrentKey();
		String route = message.state.route;
		QueryProcessResult result;

		switch (route) {
			case "payment/status":
				result = paymentStatus();
				break;
			case "payment/pay":
				return paymentPay(message, orderID);
			case "payment/cancelPayment":
				result = paymentCancel(message, orderID);
				break;

			// Orders service
			case "orders/checkout":
				result = orderCheckout(message, orderID);
				break;

			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
	}


	private QueryProcessResult orderCheckout(Message message, String orderID) throws Exception {
		PaymentState current = state.value();
		String messageState = message.state.state;

		// validate state and params

		if (messageState == null)
			throw new ServiceException.IllegalStateException(messageState);

		if (!message.params.has(PARAM_USER_ID) || !message.params.has(PARAM_PRICE))
			return getOrderCheckoutErrorResult(message, "Required parameters not provided.");

		String userID = message.params.get(PARAM_USER_ID).asText();
		long price = message.params.get(PARAM_PRICE).asLong();

		if (price == 0)
			return getOrderCheckoutErrorResult(message, "Cannot pay for an empty order.");

		if (price < 0)
			return getOrderCheckoutErrorResult(message, "Price cannot be negative.");

		// do business logic

		switch (messageState) {
			case OrderQueryProcess.STATE_CHECKOUT_SEND_PAYMENT: {
				// we need to subtract credits from the user, if possible

				if (current != null && current.status == PaymentStatus.PAID)
					return getOrderCheckoutErrorResult(message, "Payment has already been paid.");

				switch (message.state.sender) {
					case CommunicationFactory.SERVICE_ORDER: {
						// send a request to the Users service

						if (current != null && current.status == PaymentStatus.PROCESSING) {
							ObjectNode params = message.params.deepCopy();
							params.put(PARAM_STATE, OperationResult.ERROR.getCode());
							params.put(PARAM_MESSAGE, "Payment is already being executed.");
							return new QueryProcessResult.Redirect(
									ORDER_IN_TOPIC,
									message.state.route,
									params,
									messageState);
						}

						// create/update payment
						if (current == null)
							current = new PaymentState(orderID, PaymentStatus.PROCESSING);
						else
							current.status = PaymentStatus.PROCESSING;
						current.price = price;
						state.update(current);

						return new QueryProcessResult.Redirect(
								USER_IN_TOPIC,
								message.state.route,
								message.params,
								messageState);
					}

					case CommunicationFactory.SERVICE_USER:
						// send a response back to the Orders service

						if (current == null)
							return getOrderCheckoutErrorResult(message, "Payment does not exist.");

						int resultCode = message.params.get(PARAM_STATE).asInt();
						OperationResult operationResult = OperationResult.fromCode(resultCode);

						if (operationResult == OperationResult.SUCCESS)
							current.status = PaymentStatus.PAID;
						else if (operationResult == OperationResult.FAILURE)
							current.status = PaymentStatus.FAILED;
						else
							current.status = PaymentStatus.INVALID;
						state.update(current);

						return new QueryProcessResult.Redirect(
								ORDER_IN_TOPIC,
								message.state.route,
								message.params, // should contain PARAM_STATE and PARAM_MESSAGE fields
								messageState);

					default:
						throw new ServiceException("Unknown sender.");
				}

			}

			case OrderQueryProcess.STATE_CHECKOUT_REPLENISH_PAYMENT: {
				// we need to add user credits

				if (current == null)
					return getOrderCheckoutErrorResult(message, "Payment does not exist anymore.");

				if (price != current.price)
					throw new ServiceException("Payment and order prices do not match.");

				switch (message.state.sender) {
					case CommunicationFactory.SERVICE_ORDER: {
						// send a request to the Users service

						if (current.status != PaymentStatus.PAID)
							return getOrderCheckoutErrorResult(message, "Cannot cancel payment.");

						current.status = PaymentStatus.PROCESSING;
						state.update(current);

						return new QueryProcessResult.Redirect(
								USER_IN_TOPIC,
								message.state.route,
								message.params,
								messageState);
					}

					case CommunicationFactory.SERVICE_USER: {
						// send a response back to the Orders service

						if (current.status != PaymentStatus.PROCESSING)
							return getOrderCheckoutErrorResult(message, "Payment is not in the processing state.");

						int resultCode = message.params.get(PARAM_STATE).asInt();
						OperationResult operationResult = OperationResult.fromCode(resultCode);

						if (operationResult == OperationResult.SUCCESS)
							current.status = PaymentStatus.CANCELLED;
						else
							current.status = PaymentStatus.INVALID;
						state.update(current);

						return new QueryProcessResult.Redirect(
								ORDER_IN_TOPIC,
								message.state.route,
								message.params, // should contain PARAM_STATE and PARAM_MESSAGE fields
								messageState);
					}

					default:
						throw new ServiceException("Unknown sender.");
				} // inner switch - sender
			} // outer switch scope

			default:
				throw new ServiceException.IllegalStateException(messageState);
		} // outer switch - state
	}

	private QueryProcessResult getOrderCheckoutErrorResult(Message message, String msg) {
		ObjectNode params = message.params.deepCopy();
		params.put(PARAM_STATE, OperationResult.ERROR.getCode());
		params.put(PARAM_MESSAGE, msg);
		return new QueryProcessResult.Redirect(
				ORDER_IN_TOPIC,
				message.state.route,
				params,
				message.state.state);
	}


	private QueryProcessResult paymentStatus() throws Exception {
		PaymentState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		return successResult(current, null);
	}

	private List<QueryProcessResult> paymentPay(Message message, String orderID) throws Exception {
		PaymentState current = state.value();
		String messageState = message.state.state;

		if (current == null
				|| current.status == PaymentStatus.INVALID
				|| current.status == PaymentStatus.FAILED
				|| current.status == PaymentStatus.CANCELLED
		) {

			if (messageState != null) // we expect this to be null if went from the web service
				throw new ServiceException.IllegalStateException(messageState);

			// create/update payment
			if (current == null)
				current = new PaymentState(orderID, PaymentStatus.PROCESSING);
			else
				current.status = PaymentStatus.PROCESSING;
			state.update(current);

			if (message.params.has(PARAM_USER_ID) && message.params.has(PARAM_PRICE)) {
				// skip sending the message to the Orders service, since it already has all the required information (user ID and price)
				messageState = STATE_PAYMENT_ORDER_EXISTS; // "forward" message, see the logic below
			} else {
				ObjectNode params = message.params.deepCopy();
				params.put(PARAM_ORDER_ID, orderID);

				return Collections.singletonList(
						new QueryProcessResult.Redirect(
								ORDER_IN_TOPIC,
								message.state.route,
								params,
								STATE_PAYMENT_ORDER_VALIDATE));
			}
		}

		// check for invalid state
		if (messageState == null) { // no state == request for making a payment
			if (current.status == PaymentStatus.PROCESSING)
				throw new ServiceException("Payment is already being executed.");
			if (current.status == PaymentStatus.PAID)
				throw new ServiceException("Payment already concluded.");
			throw new ServiceException("Unknown payment state."); // should not execute
		}

		// check for the response from the Orders service

		if (messageState.equals(STATE_PAYMENT_ORDER_NOT_EXISTS)) {
			current.status = PaymentStatus.INVALID; // update this just in case there's some hidden references to this object somewhere
			state.update(null);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(params, "Order does not exist."));
		}

		if (messageState.equals(STATE_PAYMENT_ORDER_EXISTS)) {
			String userID = message.params.get(PARAM_USER_ID).asText();
			long price = message.params.get(PARAM_PRICE).asLong();

			if (price == 0) {
				current.status = PaymentStatus.INVALID;
				state.update(current);
				ObjectNode params = message.params.deepCopy();
				current.addParams(params);
				return Collections.singletonList(
						new QueryProcessResult.Failure(
								params,
								"Cannot pay for an empty order."));
			}

			if (price < 0) {
				current.status = PaymentStatus.INVALID;
				state.update(current);
				ObjectNode params = message.params.deepCopy();
				current.addParams(params);
				return Collections.singletonList(
						new QueryProcessResult.Failure(
								params,
								"Price cannot be negative"));
			}

			// set the price
			current.price = price;
			state.update(current);

			return Collections.singletonList(
					new QueryProcessResult.Redirect(
							USER_IN_TOPIC,
							message.state.route,
							message.params,
							STATE_PAYMENT_USER_PAY));
		}

		// check for the response from the Users service

		if (messageState.equals(STATE_PAYMENT_USER_NOT_EXISTS)) {
			current.status = PaymentStatus.INVALID; // let's keep this payment as the order does still exist
			state.update(current);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(params, "User does not exist."));
		}

		if (messageState.equals(STATE_PAYMENT_USER_NO_CREDITS)) {
			current.status = PaymentStatus.FAILED;
			state.update(current);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(params, "User does not have enough credits."));
		}

		if (messageState.equals(STATE_PAYMENT_USER_PRICE_ERROR)) {
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(
							params,
							"Invalid price."));
		}

		if (messageState.equals(STATE_PAYMENT_USER_CREDITS_SUBTRACTED)) {
			current.status = PaymentStatus.PAID;
			state.update(current);
			// send response to the web server as well as update the order simultaneously
			// (the order service will not give a response)
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			QueryProcessResult success = new QueryProcessResult.Success(
					params,
					"Payment completed, user credits subtracted.");
			QueryProcessResult orderMarkPaid = new QueryProcessResult.Redirect(
					ORDER_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_ORDER_MARK_PAID);
			return Arrays.asList(
					success,
					orderMarkPaid);
		}

		// illegal state
		throw new ServiceException.IllegalStateException(messageState);
	}

	private QueryProcessResult paymentCancel(Message message, String orderID) throws Exception {
		PaymentState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (current.status != PaymentStatus.PAID)
			throw new ServiceException("Cannot cancel payment.");

		if (message.state.state == null) {
			return new QueryProcessResult.Redirect(
					ORDER_IN_TOPIC,
					message.state.route,
					message.params,
					"get-user-id");
		}

		// receive message from the Orders service

		if (message.state.state.equals(STATE_PAYMENT_ORDER_NOT_EXISTS)) {
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return new QueryProcessResult.Failure(
					params,
					"Order does not exist anymore.");
		}

		if (message.state.state.equals(STATE_PAYMENT_ORDER_EXISTS)) {
			long price = message.params.get(PARAM_PRICE).asLong();
			String userID = message.params.get(PARAM_USER_ID).asText();

			if (price != current.price)
				throw new ServiceException("Payment and order prices do not match.");

			return new QueryProcessResult.Redirect(
					USER_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_PAYMENT_USER_REFUND);
		}

		// receive message form the Users service

		if (message.state.state.equals(STATE_PAYMENT_USER_NOT_EXISTS)) {
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return new QueryProcessResult.Failure(
					params,
					"User does not exist anymore.");
		}

		if (message.state.state.equals(STATE_PAYMENT_USER_PRICE_ERROR)) {
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return new QueryProcessResult.Failure(
					params,
					"Invalid price.");
		}

		if (message.state.state.equals(STATE_PAYMENT_USER_CREDITS_ADDED)) {
			current.status = PaymentStatus.CANCELLED;
			state.update(current);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return new QueryProcessResult.Success(
					params,
					"Payment cancelled. User credits refunded.");
		}

		throw new ServiceException.IllegalStateException(message.state.state);
	}

	private QueryProcessResult successResult(PaymentState state, @Nullable String msg) {
		return new QueryProcessResult.Success(state.toJsonNode(this.objectMapper), msg);
	}

	private QueryProcessResult failureResult(PaymentState state, @Nullable String msg) {
		return new QueryProcessResult.Failure(state.toJsonNode(this.objectMapper), msg);
	}

}
