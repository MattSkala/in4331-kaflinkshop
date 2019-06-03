package kaflinkshop.Payment;

import kaflinkshop.*;
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
	public static final String STATE_PAYMENT_ORDER_VALIDATE = "order-validate";
	public static final String STATE_PAYMENT_ORDER_MARK_PAID = "order-mark-paid";
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
				result = paymentCancel();
				break;
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
	}

	private QueryProcessResult paymentStatus() throws Exception {
		PaymentState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		return successResult(current, null);
	}

	private List<QueryProcessResult> paymentPay(Message message, String orderID) throws Exception {
		PaymentState current = state.value();

		if (current == null
				|| current.status == PaymentStatus.INVALID
				|| current.status == PaymentStatus.FAILED
		) {

			if (message.state.state != null) // we expect this to be null if went from the web service
				throw new ServiceException.IllegalStateException(message.state.state);

			// create/update payment
			if (current == null)
				current = new PaymentState(orderID, PaymentStatus.PROCESSING);
			else
				current.status = PaymentStatus.PROCESSING;
			state.update(current);

			ObjectNode params = message.params.deepCopy();
			params.put(PARAM_ORDER_ID, orderID);

			return Collections.singletonList(
					new QueryProcessResult.Redirect(
							ORDER_IN_TOPIC,
							message.state.route,
							params,
							STATE_PAYMENT_ORDER_VALIDATE));
		}

		// check for invalid state
		if (message.state.state == null) { // no state == request for making a payment
			if (current.status == PaymentStatus.PROCESSING)
				throw new ServiceException("Payment is already being executed.");
			if (current.status == PaymentStatus.PAID)
				throw new ServiceException("Payment already concluded.");
			throw new ServiceException("Unknown payment state."); // should not execute
		}

		// check for the response from the Orders service

		if (message.state.state.equals(STATE_PAYMENT_ORDER_NOT_EXISTS)) {
			current.status = PaymentStatus.INVALID; // update this just in case there's some hidden references to this object somewhere
			state.update(null);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(params, "Order does not exist."));
		}

		if (message.state.state.equals(STATE_PAYMENT_ORDER_EXISTS)) {
			String userID = message.params.get(PARAM_USER_ID).asText();
			long price = message.params.get(PARAM_PRICE).asLong();

			if (price < 0) { // zero is fine...
				current.status = PaymentStatus.INVALID;
				state.update(current);
				ObjectNode params = message.params.deepCopy();
				current.addParams(params);
				return Collections.singletonList(
						new QueryProcessResult.Failure(
								params,
								"Price cannot be negative.")
				);
			}

			return Collections.singletonList(
					new QueryProcessResult.Redirect(
							USER_IN_TOPIC,
							message.state.route,
							message.params,
							"validate-user-subtract-credits"));
		}

		// check for the response from the Users service

		if (message.state.state.equals(STATE_PAYMENT_USER_NOT_EXISTS)) {
			current.status = PaymentStatus.INVALID; // let's keep this payment as the order does still exist
			state.update(current);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(params, "User does not exist."));
		}

		if (message.state.state.equals(STATE_PAYMENT_USER_NO_CREDITS)) {
			current.status = PaymentStatus.FAILED;
			state.update(current);
			ObjectNode params = message.params.deepCopy();
			current.addParams(params);
			return Collections.singletonList(
					new QueryProcessResult.Failure(params, "User does not have enough credits."));
		}

		if (message.state.state.equals(STATE_PAYMENT_USER_CREDITS_SUBTRACTED)) {
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
		throw new ServiceException.IllegalStateException(message.state.state);
	}

	private QueryProcessResult paymentCancel() throws Exception {
		PaymentState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (current.status != PaymentStatus.PAID)
			throw new ServiceException("Cannot cancel payment.");

		current.status = PaymentStatus.CANCELLED;
		state.update(current);
		return successResult(current, "Payment cancelled.");
	}

	private QueryProcessResult successResult(PaymentState state, @Nullable String msg) {
		return new QueryProcessResult.Success(state.toJsonNode(this.objectMapper), msg);
	}

	private QueryProcessResult failureResult(PaymentState state, @Nullable String msg) {
		return new QueryProcessResult.Failure(state.toJsonNode(this.objectMapper), msg);
	}

}
