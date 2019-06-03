package kaflinkshop.Payment;

import kaflinkshop.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class PaymentQueryProcess extends QueryProcess {

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
				result = paymentPay(orderID);
				break;
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
			throw new ServiceException.EntryNotFoundException("payment");

		return successResult(current, null);
	}

	private QueryProcessResult paymentPay(String orderID) throws Exception {
		PaymentState current = state.value();

		// TODO: implement payment
		// This will chang ein future, with call to other services.
		// For now, let's just simulate instant yes-no replies.
		// We first mark the payment as PAID and then

		if (current != null && current.status != PaymentStatus.INVALID)
			throw new ServiceException("Payment already processed.");

		if (Math.random() >= 0.5) {
			current = new PaymentState(orderID, PaymentStatus.PAID);
			state.update(current);
			return successResult(current, "Payment successful.");
		} else {
			current = new PaymentState(orderID, PaymentStatus.FAILED);
			state.update(current);
			return failureResult(current, "Payment failed.");
		}
	}

	private QueryProcessResult paymentCancel() throws Exception {
		PaymentState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("payment");

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
