package kaflinkshop;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class OrderQueryProcess
        extends KeyedProcessFunction<Tuple, Tuple2<String, JsonNode>, Tuple2<String, String>> {

    public static int WAIT_FOR_CALLBACK = 60000;

    /**
     * The state that is maintained by this process function
     */
    private ValueState<OrderState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("order_state", OrderState.class));
    }

    private transient ObjectMapper jsonParser;

    @Override
    public void processElement(
            Tuple2<String, JsonNode> value,
            Context ctx,
            Collector<Tuple2<String,String>> out) throws Exception {


        String order_id = value.f0;
        JsonNode value_node = value.f1;
        String route = value_node.get("route").asText();
        Tuple2<String, String> output;
        System.out.println(route);

        switch (route) {
            case "orders/create":
                output = CreateOrder(value_node, order_id);
                break;
            case "order/remove":
                output = RemoveOrder(value_node);
                break;
            case "order/find":
                output = FindOrder(value_node);
                break;
            case "order/addItem":
                output = addItem(value_node);
                break;
            case "order/removeItem":
                output = removeItem(value_node);
                break;
            case "order/checkout":
                //Need multiple nodes
            default:
                output = ErrorState(value_node);
        }

        out.collect(output);

    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, String>> out) throws Exception {

        // get the state for the key that scheduled the timer
        OrderState result = state.value();

        // check if this is an outdated timer or the latest timer
        if (!result.check_user) {
            // emit the state on timeout
            System.out.println("Have not checked this order yet!");
        }
    }


    private Tuple2<String, String> ErrorState(JsonNode value_node){
        ObjectNode jNode = CreateOutput(value_node);
        jNode.put("Error", "Something went wrong");
        return CommunicationFactory.createOutput("order_out_api1", jNode.toString());
    }

    private Tuple2<String, String> CreateOrder(JsonNode value_node, String order_id) throws Exception {
        OrderState current = new OrderState();
        current.order_id = order_id;
        ObjectNode jNode = (ObjectNode) value_node;
        jNode.with("params").put("order_id", current.order_id);

        // write the state back
        state.update(current);
        return CommunicationFactory.createOutput("user_in", jNode.toString());
    }

    private ObjectNode CreateOutput(JsonNode input_node){
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        ObjectNode jNode = jsonParser.createObjectNode();
        jNode.put("request_id", input_node.get("request_id"));
        return jNode;
    }

    private Tuple2<String, String> addItem(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);


        JsonNode params = value_node.get("params");
        String add_item = params.get("item").toString();
        // retrieve the current count
        OrderState current = state.value();

        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            current.products[current.products.length] = add_item;
            state.update(current);
            jNode.put("message", "Added item");
            jNode.put("result", "success");
            jNode.put("products", current.products.toString());
        }

        return CommunicationFactory.createOutput("add_item", jNode.toString());

    }

    private Tuple2<String, String> removeItem(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);


        JsonNode params = value_node.get("params");
        String remove_item = params.get("item").toString();
        // retrieve the current count
        OrderState current = state.value();

        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            //Not the best solution
            List<String> list = new ArrayList<>(Arrays.asList(current.products));
            list.remove(remove_item);
            current.products = list.toArray(new String[0]);
            state.update(current);
            jNode.put("message", "Removed item");
            jNode.put("result", "success");
            jNode.put("products", current.products.toString());
        }

        return CommunicationFactory.createOutput("remove_item", jNode.toString());

    }

    private Tuple2<String, String> FindOrder(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);

        // retrieve the current count
        OrderState current = state.value();
        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            jNode.put("order_id", current.order_id);
            jNode.put("user_id", current.user_id);
            jNode.put("products", current.products.toString());
            state.update(current);
        }

        return CommunicationFactory.createOutput("find_order", jNode.toString());

    }

    private Tuple2<String, String> RemoveOrder(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);

        // retrieve the current count
        OrderState current = state.value();

        if (current == null) {
            jNode.put("result", "failure");
            jNode.put("message", "Could not find this order");
        } else {
            state.update(null);
            jNode.put("result", "success");
            jNode.put("message", "Correctly deleted the order");

        }

        return CommunicationFactory.createOutput("remove_order", jNode.toString());

    }
}