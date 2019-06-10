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
import java.util.*;

import static kaflinkshop.CommunicationFactory.*;
import static kaflinkshop.Payment.PaymentQueryProcess.*;

public class OrderQueryProcess extends QueryProcess {

    public static final String STATE_USER_NOT_EXISTS = "no-user";
    public static final String STATE_USER_ORDER_REMOVED = "user-order-removed";
    public static final String STATE_USER_ORDER_ADDED = "user-order-added";
    public static final String STATE_ITEM_EXISTS = "item-exists";
    public static final String STATE_ITEM_NOT_EXISTS = "no-item";
    public static final String STATE_CHECKOUT_SEND_PAYMENT = "send-payment";
    public static final String STATE_CHECKOUT_SEND_STOCK = "send-stock";
    public static final String STATE_CHECKOUT_REPLENISH_PAYMENT = "replenish-payment";
    public static final String STATE_CHECKOUT_REPLENISH_STOCK = "replenish-stock";
    public static final String ENTITY_NAME = "order";
    public static final long TIME_OUT_DURATION = 10000;
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
                return checkoutOrder(message, context);

            // Payment logic
            case "payment/pay":
                return paymentPay(message);
            case "payment/cancelPayment":
                result = paymentCancel(message);
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
        params.put(PARAM_PRICE, current.getPrice());
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

            // TODO: should we check if all items exist?
            // nah, not here, this gets checked when orders/checkout is called
            long price = current.getPrice();

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

        if (current.isPaid || current.checkoutStatus != OrderCheckoutStatus.NOT_PROCESSED)
            throw new ServiceException("Cannot remove order.");

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

        if (!current.canModify())
            throw new ServiceException("Cannot modify order.");

        if (message.state.state == null) {
            // initiate item the process of adding an item
            ObjectNode newParams = message.params.deepCopy();
            newParams.put(PARAM_ORDER_ID, current.orderID);
            newParams.put(PARAM_ITEM_ID, itemID);
            return new QueryProcessResult.Redirect(STOCK_IN_TOPIC, message.state.route, newParams);
        }

        // check if item exists (callback from the stock service)
        if (message.state.state.equals(STATE_ITEM_EXISTS)) {
            int items = current.products.getOrDefault(itemID, 0);
            current.products.put(itemID, items + 1);
            state.update(current);
            return successResult(current, "Item added.");
        }

        // check if item does not exist (callback from the stock service)
        if (message.state.state.equals(STATE_ITEM_NOT_EXISTS)) {
            current.products.remove(itemID);
            state.update(current);
            return failureResult(current, "Item does not exist.");
        }

        throw new ServiceException.IllegalStateException(message.state.state);
    }

    private QueryProcessResult removeItem(Message message) throws Exception {
        OrderState current = state.value();
        String itemID = message.params.get(PARAM_ITEM_ID).asText();

        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        if (!current.canModify())
            throw new ServiceException("Cannot modify order.");

        Integer value = current.products.remove(itemID);
        state.update(current);

        if (value == null) {
            return failureResult(current, "No such item exists.");
        } else {
            return successResult(current, "Item(s) removed.");
        }
    }

    private List<QueryProcessResult> checkoutOrder(Message message, Context context) throws Exception {
        OrderState current = state.value();
        ArrayList<QueryProcessResult> results = new ArrayList<>(2);

        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        if (current.isPaid)
            throw new ServiceException("Order has already been paid.");

        if (!current.userChecked)
            throw new ServiceException("Cannot checkout order.");

        switch (current.checkoutStatus) {
            case SUCCEEDED:
                throw new ServiceException("Order already concluded.");
            case INVALID:
                throw new ServiceException("Invalid order state.");
            case FAILED:
                // fall-through - retry
                List<OrderState.CheckoutMessage> messages_out = current.checkoutMessages;

                results.add(sendCheckoutFailureResponse(current, message, messages_out));
                results.add(sendCheckoutPaymentCancelRequest(current, message));
                results.add(sendStockReplenishMessage(current));
                return results;
            case NOT_PROCESSED:
                current.checkoutStatus = OrderCheckoutStatus.IN_PROGRESS;
                current.checkoutProgress = OrderCheckoutProgress.NOT_PROCESSED;
                // fall-through - process this checkout

//                System.out.println("Will let of timer at:");
//                System.out.println(context.timestamp());
//                System.out.println(context.timestamp() + TIME_OUT_DURATION);

                // Set the timeout and save the response message just in case
                context.timerService().registerEventTimeTimer(context.timestamp() + TIME_OUT_DURATION);

                current.latestMessage = message;
                state.update(current);
            case IN_PROGRESS: {
                if (current.checkoutProgress == OrderCheckoutProgress.NOT_PROCESSED) {

                    if (current.getPrice() <= 0) {
                        this.clearCheckoutState(current, OrderCheckoutStatus.FAILED);
                        state.update(current);
                        throw new ServiceException("Cannot checkout an order with no items.");
                    }

                    // send two requests
                    QueryProcessResult stockRequest = sendStockSubtractRequest(current);
                    QueryProcessResult paymentRequest = sendCheckoutPaymentPayRequest(current, message);
                    results.add(stockRequest);
                    results.add(paymentRequest);

                    // update the current state
                    current.checkoutProgress = OrderCheckoutProgress.AWAITING_SEND_BOTH;
                    current.checkoutMessages = new ArrayList<>();
                    state.update(current);

                    // the first part is done.
                    break;
                }

                // message's state should be set at every step of the way - if not, yield an error
                if (message.state.state == null)
                    throw new ServiceException.IllegalStateException(null);

                // this switch updates the status of the state given its response
                switch (message.state.state) {
                    case STATE_CHECKOUT_SEND_STOCK: { // this is the longest of all cases (in terms of LOC) as we need to mark what products to replenish
                        // retrieve a list of products processed
                        JsonNode products = message.params.get(PARAM_PRODUCTS);
                        Iterator<Map.Entry<String, JsonNode>> iterator = products.fields();

                        // these two booleans indicate whether any items has yielded a failure or an error
                        boolean isFailure = false;
                        boolean isError = false;

                        // build a map of items that need to be refunded
                        Map<String, Long> refunds = new HashMap<>();
                        while (iterator.hasNext()) {
                            // extract item status
                            Map.Entry<String, JsonNode> product = iterator.next();
                            String itemID = product.getKey();
                            JsonNode value = product.getValue();
                            int code = value.get(PARAM_STATE).asInt();
                            OperationResult batchResult = OperationResult.fromCode(code);

                            // process item given its status
                            if (batchResult == OperationResult.ERROR) {
                                // mark error
                                isError = true;
                                current.checkoutMessages.add(new OrderState.CheckoutMessage(OperationResult.ERROR, "Item does not exist: " + itemID));
                            } else if (batchResult == OperationResult.FAILURE) {
                                // mark failure
                                isFailure = true;
                                current.checkoutMessages.add(new OrderState.CheckoutMessage(OperationResult.FAILURE, "Not enough items in stock: " + itemID));
                            } else if (batchResult == OperationResult.SUCCESS) {
                                // this item might need to be refunded, so put it in the map
                                long amount = value.get(PARAM_AMOUNT).asLong();
                                refunds.put(itemID, amount);
                            } else {
                                throw new IllegalStateException("Unrecognised batch result.");
                            }
                        }

                        // update the current state
                        current.checkoutStock = refunds;
                        current.checkoutProgress = OrderCheckoutProgress.remove(current.checkoutProgress, OrderCheckoutProgress.AWAITING_SEND_STOCK);
                        if (isError || isFailure) {
                            current.checkoutProgress |= OrderCheckoutProgress.FAILED_STOCK;
                        } else {
                            current.checkoutProgress |= OrderCheckoutProgress.SUCCEEDED_STOCK;
                        }
                    }
                    break;

                    case STATE_CHECKOUT_SEND_PAYMENT: {
                        // extract operation result
                        int resultCode = message.params.get(PARAM_STATE).asInt();
                        OperationResult operationResult = OperationResult.fromCode(resultCode);

                        // update the current state
                        current.checkoutProgress = OrderCheckoutProgress.remove(current.checkoutProgress, OrderCheckoutProgress.AWAITING_SEND_PAYMENT);
                        if (operationResult == OperationResult.SUCCESS) {
                            current.checkoutProgress |= OrderCheckoutProgress.SUCCEEDED_PAYMENT;
                        } else {
                            current.checkoutProgress |= OrderCheckoutProgress.FAILED_PAYMENT;
                            if (operationResult == OperationResult.FAILURE) {
                                String msg = getTextField(message.params.get(PARAM_MESSAGE), "Insufficient funds.");
                                current.checkoutMessages.add(new OrderState.CheckoutMessage(operationResult, msg));
                            } else {
                                String msg = getTextField(message.params.get(PARAM_MESSAGE), "Unknown error - send payment.");
                                current.checkoutMessages.add(new OrderState.CheckoutMessage(operationResult, msg));
                            }
                        }
                    }
                    break;

                    case STATE_CHECKOUT_REPLENISH_PAYMENT: {
                        // extract operation result
                        int resultCode = message.params.get(PARAM_STATE).asInt();
                        OperationResult operationResult = OperationResult.fromCode(resultCode);

                        // update the current state
                        current.checkoutProgress = OrderCheckoutProgress.remove(current.checkoutProgress, OrderCheckoutProgress.AWAITING_REPLENISH_PAYMENT);
                        if (operationResult != OperationResult.SUCCESS) {
                            String msg = getTextField(message.params.get(PARAM_MESSAGE), "Unknown error - payment replenish.");
                            current.checkoutMessages.add(new OrderState.CheckoutMessage(operationResult, msg));
                        }
                    }
                    break;

                    case STATE_CHECKOUT_REPLENISH_STOCK: {
                        // retrieve a list of products to process
                        JsonNode products = message.params.get(PARAM_PRODUCTS);
                        Iterator<Map.Entry<String, JsonNode>> iterator = products.fields();
                        while (iterator.hasNext()) {
                            // extract item status
                            Map.Entry<String, JsonNode> product = iterator.next();
                            String itemID = product.getKey();
                            JsonNode value = product.getValue();
                            int code = value.get(PARAM_STATE).asInt();
                            OperationResult batchResult = OperationResult.fromCode(code);

                            // process item given its status
                            if (batchResult == OperationResult.ERROR) {
                                // mark error
                                current.checkoutMessages.add(new OrderState.CheckoutMessage(OperationResult.ERROR, "Error replenishing item: " + itemID));
                            } else if (batchResult == OperationResult.FAILURE) {
                                // mark failure
                                current.checkoutMessages.add(new OrderState.CheckoutMessage(OperationResult.FAILURE, "Failed to replenish item: " + itemID));
                            }
                        }

                        // update the current state
                        current.checkoutProgress = OrderCheckoutProgress.remove(current.checkoutProgress, OrderCheckoutProgress.AWAITING_REPLENISH_STOCK);
                    }
                    break;

                    default:
                        throw new ServiceException.IllegalStateException(message.state.state);
                }

                boolean replenishStockSent = OrderCheckoutProgress.contains(current.checkoutProgress, OrderCheckoutProgress.REPLENISH_STOCK_SENT);
                boolean replenishPaymentSent = OrderCheckoutProgress.contains(current.checkoutProgress, OrderCheckoutProgress.REPLENISH_PAYMENT_SENT);
                boolean awaitingStockSend = OrderCheckoutProgress.contains(current.checkoutProgress, OrderCheckoutProgress.AWAITING_SEND_STOCK);
                boolean awaitingPaymentSend = OrderCheckoutProgress.contains(current.checkoutProgress, OrderCheckoutProgress.AWAITING_SEND_PAYMENT);
                boolean failedStock = OrderCheckoutProgress.contains(current.checkoutProgress, OrderCheckoutProgress.FAILED_STOCK);
                boolean failedPayment = OrderCheckoutProgress.contains(current.checkoutProgress, OrderCheckoutProgress.FAILED_PAYMENT);

                // check if we should send replenish request to the stock service
                if (!replenishStockSent && !awaitingStockSend && (failedStock || failedPayment)) {
                    // send replenish stock request, but only if there's something to replenish in the first place
                    if (current.checkoutStock.size() > 0) {
                        current.checkoutProgress |= OrderCheckoutProgress.AWAITING_REPLENISH_STOCK;
                        current.checkoutProgress |= OrderCheckoutProgress.REPLENISH_STOCK_SENT;
                        QueryProcessResult result = sendStockReplenishMessage(current);
                        results.add(result);
                    } else {
                        // if there's nothing to replenish, then don't send any messages
                        current.checkoutProgress |= OrderCheckoutProgress.REPLENISH_STOCK_SENT;
                    }
                }

                // check if we should send replenish request to the payment service
                if (!replenishPaymentSent && !awaitingPaymentSend && !failedPayment && failedStock) {
                    // send replenish payment request
                    current.checkoutProgress |= OrderCheckoutProgress.AWAITING_REPLENISH_PAYMENT;
                    current.checkoutProgress |= OrderCheckoutProgress.REPLENISH_PAYMENT_SENT;
                    QueryProcessResult result = sendCheckoutPaymentCancelRequest(current, message);
                    results.add(result);
                }

                // handle logic for continuing the process of checking-out the order
                if (!OrderCheckoutProgress.isAwaitingAnything(current.checkoutProgress)) {
                    // we're done
                    QueryProcessResult result;
                    if (OrderCheckoutProgress.hasSucceeded(current.checkoutProgress)) {
                        clearCheckoutState(current, OrderCheckoutStatus.SUCCEEDED);
                        current.isPaid = true;
                        result = sendCheckoutSuccessResponse(current);
                    } else if (OrderCheckoutProgress.hasFailed(current.checkoutProgress)
                            && OrderCheckoutProgress.hasSentBothReplenishRequests(current.checkoutProgress)) {
                        List<OrderState.CheckoutMessage> messages = current.checkoutMessages;
                        clearCheckoutState(current, OrderCheckoutStatus.FAILED);
                        result = sendCheckoutFailureResponse(current, message, messages);
                    } else {
                        // this should not happen
                        clearCheckoutState(current, OrderCheckoutStatus.INVALID);
                        state.update(current);
                        throw new ServiceException("Invalid checkout state - not succeeded yet not failed.");
                    }
                    results.add(result);
                } // end inner switch
            }
            break;
            default:
                throw new ServiceException("Unrecognised checkout order status.");
        } // end outer switch

        state.update(current);
        return results;
    }

    private static String getTextField(JsonNode node, String defaultMessage) {
        if (node == null || node.isNull())
            return defaultMessage;
        else
            return node.asText();
    }

    private void clearCheckoutState(OrderState current, OrderCheckoutStatus status) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        current.checkoutStatus = status;
        current.checkoutProgress = OrderCheckoutProgress.NOT_PROCESSED;
        current.checkoutMessages = null;
        current.checkoutStock = null;
    }

    private QueryProcessResult sendCheckoutSuccessResponse(OrderState current) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        return successResult(current, "Order successfully purchased.");
    }

    private QueryProcessResult sendCheckoutFailureResponse(OrderState current, Message message, List<OrderState.CheckoutMessage> log) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        ObjectNode params = message.params.deepCopy();
        params.set("log", OrderState.CheckoutMessage.getMessagesAsJson(objectMapper, log));
        current.fillJsonNode(params, objectMapper);

        return new QueryProcessResult.Failure(
                params,
                "Order checkout failed.");
    }

    private QueryProcessResult sendStockReplenishMessage(OrderState current) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        if (current.checkoutStock == null)
            throw new ServiceException("Checkout stock not defined.");

        ObjectNode params = objectMapper.createObjectNode();
        params.set(PARAM_PRODUCTS, current.getCheckoutStockAsJson(objectMapper));
        params.put(PARAM_ORDER_ID, current.orderID);
        params.put(PARAM_USER_ID, current.userID);
        params.put(PARAM_RETURN_STATE, STATE_CHECKOUT_REPLENISH_STOCK);

        String targetRoute = "batch/add"; // count how many entries are available
        String targetState = "batch:add";

        // message will return to ORDER_IN_TOPIC (hardcoded), and to the route equal to message.input.route
        return new QueryProcessResult.Redirect(
                STOCK_IN_TOPIC,
                targetRoute,
                params,
                targetState);
    }

    private QueryProcessResult sendStockSubtractRequest(OrderState current) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        ObjectNode params = objectMapper.createObjectNode();
        params.set(PARAM_PRODUCTS, current.getProductsAsJson(objectMapper));
        params.put(PARAM_ORDER_ID, current.orderID);
        params.put(PARAM_USER_ID, current.userID);
        params.put(PARAM_RETURN_STATE, STATE_CHECKOUT_SEND_STOCK);

        String targetRoute = "batch/subtract"; // count how many entries are available
        String targetState = "batch:subtract";

        // message will return to ORDER_IN_TOPIC (hardcoded), and to the route equal to message.input.route
        return new QueryProcessResult.Redirect(
                STOCK_IN_TOPIC,
                targetRoute,
                params,
                targetState);
    }

    private QueryProcessResult sendCheckoutPaymentPayRequest(OrderState current, Message message) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        ObjectNode params = objectMapper.createObjectNode();
        params.put(PARAM_ORDER_ID, current.orderID);
        params.put(PARAM_USER_ID, current.userID);
        params.put(PARAM_PRICE, current.getPrice());

        return new QueryProcessResult.Redirect(
                PAYMENT_IN_TOPIC,
                message.state.route,
                params,
                STATE_CHECKOUT_SEND_PAYMENT);
    }

    private QueryProcessResult sendCheckoutPaymentCancelRequest(OrderState current, Message message) throws Exception {
        if (current == null)
            throw new ServiceException.EntryNotFoundException(ENTITY_NAME);

        ObjectNode params = objectMapper.createObjectNode();
        params.put(PARAM_ORDER_ID, current.orderID);
        params.put(PARAM_USER_ID, current.userID);
        params.put(PARAM_PRICE, current.getPrice());

        return new QueryProcessResult.Redirect(
                PAYMENT_IN_TOPIC,
                message.state.route,
                params,
                STATE_CHECKOUT_REPLENISH_PAYMENT);
    }

    private QueryProcessResult successResult(OrderState state, @Nullable String msg) {
        return new QueryProcessResult.Success(state.toJsonNode(this.objectMapper), msg);
    }

    private QueryProcessResult failureResult(OrderState state, @Nullable String msg) {
        return new QueryProcessResult.Failure(state.toJsonNode(this.objectMapper), msg);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context, Collector<Output> out) throws Exception {

        // get the state for the key that scheduled the timer
        OrderState current = state.value();
        Message message = current.latestMessage;
        current.latestMessage = null;
        if (current.checkoutStatus == OrderCheckoutStatus.SUCCEEDED) {
//            System.out.println("All good");
        } else {
//            System.out.println("Failed");
            current.checkoutStatus = OrderCheckoutStatus.FAILED;
            state.update(current);
//            System.out.println(message);
            try {
                processElement(message, context);
            } catch (Exception e) {
                Output output = new Output(Message.error(message, this.serviceName, e));
                out.collect(output);
            }
        }

    }

}
