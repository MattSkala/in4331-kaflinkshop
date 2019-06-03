package kaflinkshop.User;

import kaflinkshop.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

import static kaflinkshop.CommunicationFactory.*;
import static kaflinkshop.Order.OrderQueryProcess.*;

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
				result = removeUser();
				break;
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

			// Error route handler
			default:
				throw new ServiceException.IllegalRouteException();
		}

		return Collections.singletonList(result);
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

	private QueryProcessResult removeUser() throws Exception {
		UserState current = state.value();

		if (current == null)
			throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

		state.update(null);

		return new QueryProcessResult.Success("User removed.");
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
