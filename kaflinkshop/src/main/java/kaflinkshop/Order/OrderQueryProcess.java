package kaflinkshop.Order;

import kaflinkshop.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class OrderQueryProcess extends QueryProcess {

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
	public void processElement(Message message, Context context, Collector<Output> collector) throws Exception {
		String route = message.state.route;

		// HOWTO: send message to multiple services
		if (route.equals("...")) {
			// do custom things, like send two message (call `collector.collect(...)` twice)
			collector.collect(new Output(
					Message.redirect(message, CommunicationFactory.SERVICE_ORDER, "route/to/process", "validate-user", null),
					CommunicationFactory.USER_IN_TOPIC
			));
		}

		super.processElement(message, context, collector);
	}

	@Override
	public List<QueryProcessResult> processElement(Message message, Context context) throws Exception {
		String orderID = context.getCurrentKey();
		String route = message.state.route;
		QueryProcessResult result;

		switch (route) {
			case "orders/find":
				result = findOrder();
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
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
	}

	private QueryProcessResult findOrder() throws Exception {
		OrderState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("order");

		return successResult(current, null);
	}

	private QueryProcessResult createOrder(String orderID, Message message) throws Exception {
		OrderState current = state.value();
		String userID = message.params.get("user_id").asText();
		System.out.println(message.state.state);

		if(message.state.state != null && message.state.state.equals("confirmed-user")){
			current.userChecked = true;
			state.update(current);

			return successResult(current, "Order created.");
		}


		if(message.state.state != null && message.state.state.equals("no-user")){
			state.update(null);

			return successResult(current, "Order not created. User did not exist.");
		}

		if (current != null)
			throw new ServiceException("Order already exists.");

		current = new OrderState(orderID, userID);
		state.update(current);

		ObjectNode newParams = message.params.deepCopy();
		newParams.put("order_id", orderID);


		// Set some timer!
		return new QueryProcessResult.Redirect(CommunicationFactory.USER_IN_TOPIC, message.state.route,  newParams, "order-create-check-user");
	}

	private QueryProcessResult removeOrder(Message message) throws Exception {
		OrderState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("order");

		if(message.state.state != null){
			state.update(null);
			return new QueryProcessResult.Success("Order removed.");
		}

		// Set some timer!

		ObjectNode newParams = message.params.deepCopy();
		newParams.put("order_id", current.orderID);
		newParams.put("user_id", current.userID);

		return new QueryProcessResult.Redirect(CommunicationFactory.USER_IN_TOPIC, message.state.route, newParams, "check-remove");
	}

	private QueryProcessResult addItem(Message message) throws Exception {
		OrderState current = state.value();
		String itemID = message.params.get("item_id").asText();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("order");

		int items = current.products.getOrDefault(itemID, 0);
		current.products.put(itemID, items + 1);
		state.update(current);

		return successResult(current, "Item added.");
	}

	private QueryProcessResult removeItem(Message message) throws Exception {
		OrderState current = state.value();
		String itemID = message.params.get("item_id").asText();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("order");

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
