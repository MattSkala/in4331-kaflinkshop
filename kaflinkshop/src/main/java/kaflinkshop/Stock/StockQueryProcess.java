package kaflinkshop.Stock;

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
import static kaflinkshop.Order.OrderQueryProcess.STATE_ITEM_EXISTS;
import static kaflinkshop.Order.OrderQueryProcess.STATE_ITEM_NOT_EXISTS;


public class StockQueryProcess extends QueryProcess {

	public static final String ENTITY_NAME = "stock_item";

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
	public List<QueryProcessResult> processElement(Message message, Context context) throws Exception {
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

			// Order logic
			case "orders/addItem":
				result = orderAddItem(message);
				break;

			// Batch processing
			case "batch/count":
				result = batchCount(message);
				break;
			case "batch/subtract":
				result = batchSubtractStock(message);
				break;
			case "batch/add":
				result = batchAddStock(message);
				break;

			// Error route handler
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
	}

	private QueryProcessResult batchAddStock(Message message) throws Exception {
		StockState current = state.value();
		long amount = message.params.get(PARAM_AMOUNT).asLong();

		ObjectNode params = message.params.deepCopy();
		ObjectNode pass = objectMapper.createObjectNode();
		params.set(PARAM_BATCH_PASS, pass);

		if (current == null || amount < 0) {
			pass.put(PARAM_STATE, OperationResult.ERROR.getCode());
		} else {
			current.amount += amount;
			state.update(current);
			pass.put(PARAM_STATE, OperationResult.SUCCESS.getCode());
		}

		return new QueryProcessResult.Redirect(
				STOCK_IN_TOPIC,
				message.state.route,
				params,
				message.state.state);
	}

	private QueryProcessResult batchSubtractStock(Message message) throws Exception {
		StockState current = state.value();
		long amount = message.params.get(PARAM_AMOUNT).asLong();

		ObjectNode params = message.params.deepCopy();
		ObjectNode pass = objectMapper.createObjectNode();
		params.set(PARAM_BATCH_PASS, pass);

		if (current == null || amount < 0) {
			pass.put(PARAM_STATE, OperationResult.ERROR.getCode());
		} else if (current.amount >= amount) {
			current.amount -= amount;
			state.update(current);
			pass.put(PARAM_STATE, OperationResult.SUCCESS.getCode());
			pass.put(PARAM_AMOUNT, amount);
		} else {
			pass.put(PARAM_STATE, OperationResult.FAILURE.getCode());
		}

		return new QueryProcessResult.Redirect(
				STOCK_IN_TOPIC,
				message.state.route,
				params,
				message.state.state);
	}

	private QueryProcessResult batchCount(Message message) throws Exception {
		StockState current = state.value();

		long amount = current == null ? -1 : current.amount;
		ObjectNode params = message.params.deepCopy();
		ObjectNode pass = objectMapper.createObjectNode();
		params.set(PARAM_BATCH_PASS, pass);
		pass.put(PARAM_AMOUNT, amount);

		return new QueryProcessResult.Redirect(
				STOCK_IN_TOPIC,
				message.state.route,
				params,
				message.state.state);
	}

	private QueryProcessResult orderAddItem(Message message) throws Exception {
		StockState current = state.value();

		if (current == null)
			return new QueryProcessResult.Redirect(
					ORDER_IN_TOPIC,
					message.state.route,
					message.params,
					STATE_ITEM_NOT_EXISTS);

		return new QueryProcessResult.Redirect(
				ORDER_IN_TOPIC,
				message.state.route,
				message.params,
				STATE_ITEM_EXISTS);
	}

	private QueryProcessResult itemAvailability() throws Exception {
		StockState current = state.value(); // retrieve the state

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

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
		long amount = message.params.get(PARAM_AMOUNT).asLong();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		if (amount <= 0)
			throw new ServiceException("Amount must be positive.");

		current.amount += amount;
		state.update(current);

		return successResult(current, "Items added to stock.");
	}

	private QueryProcessResult itemSubtract(Message message) throws Exception {
		StockState current = state.value();
		long amount = message.params.get(PARAM_AMOUNT).asLong();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

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
