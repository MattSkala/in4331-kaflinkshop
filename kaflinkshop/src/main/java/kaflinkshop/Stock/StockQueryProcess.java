package kaflinkshop.Stock;

import kaflinkshop.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

public class StockQueryProcess extends QueryProcess<String> {

	/**
	 * The state that is maintained by this process function.
	 */
	private ValueState<StockState> state;
	private ObjectMapper objectMapper;

	public StockQueryProcess() {
		super(CommunicationFactory.SERVICE_STOCK);
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public void open(Configuration parameters) {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("stock_state", StockState.class));
	}

	@Override
	public QueryProcessResult processElement(Message message, Context context) throws Exception {
		String itemID = context.getCurrentKey();
		String route = message.state.route;
		QueryProcessResult result;

		switch (route) {
			case "stock/availability":
				result = itemAvailability();
				break;
			case "stock/item/create":
				result = createItem(itemID);
				break;
			case "stock/add":
				result = itemAdd(message);
				break;
			case "stock/subtract":
				result = itemSubtract(message);
				break;
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return result;
	}

	private QueryProcessResult itemAvailability() throws Exception {
		StockState current = state.value(); // retrieve the state

		if (current == null)
			throw new ServiceException.EntryNotFoundException("stock_item");

		return successResult(current, null);
	}

	private QueryProcessResult createItem(String itemID) throws Exception {
		StockState current = state.value();

		if (current != null)
			throw new ServiceException("Item already exists.");

		current = new StockState(itemID);
		state.update(current); // update the state

		return successResult(current, "Item created.");
	}

	private QueryProcessResult itemAdd(Message message) throws Exception {
		StockState current = state.value();
		long amount = message.params.get("amount").asLong();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("stock_item");

		if (amount <= 0)
			throw new ServiceException("Amount must be positive.");

		current.amount += amount;
		state.update(current);

		return successResult(current, "Items added to stock.");
	}

	private QueryProcessResult itemSubtract(Message message) throws Exception {
		StockState current = state.value();
		long amount = message.params.get("amount").asLong();

		if (current == null)
			throw new ServiceException.EntryNotFoundException("stock_item");

		if (amount <= 0)
			throw new ServiceException("Amount must be positive.");

		if (current.amount < amount)
			throw new ServiceException("Not enough items in stock.");

		current.amount -= amount;
		state.update(current);

		return successResult(current, "Items removed.");
	}

	private QueryProcessResult successResult(StockState state, @Nullable String msg) {
		return new QueryProcessResult.Success(state.toJsonNode(this.objectMapper), msg);
	}

}
