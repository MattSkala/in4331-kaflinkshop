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

import java.util.HashSet;

public class UserQueryProcess
        extends KeyedProcessFunction<Tuple, Tuple2<String, JsonNode>, String> {

    /**
     * The state that is maintained by this process function
     */
    private ValueState<UserState> state;

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("user_state", UserState.class));
    }

    private transient ObjectMapper jsonParser;

    @Override
    public void processElement(
            Tuple2<String, JsonNode> value,
            Context ctx,
            Collector<String> out) throws Exception {


        String user_id = value.f0;
        JsonNode value_node = value.f1;
        String route = value_node.get("route").asText();
        String output;

        switch (route) {
            case "users/create":
                output = CreateUser(value_node, user_id);
                break;
            case "users/find":
                output = FindUser(value_node);
                break;
            case "users/remove":
                output = RemoveUser(value_node);
                break;
            case "users/credit":
                output = GetCredits(value_node);
                break;
            case "users/credit/subtract":
                output = SubtractCredits(value_node);
                break;
            case "users/credit/add":
                output = AddCredits(value_node);
                break;
            case "orders/create":
                output = CreateOrder(value_node);
                break;
            default:
                output = ErrorState(value_node);
        }

        out.collect(output);

    }

    private String CreateOrder(JsonNode value_node) throws Exception{
        ObjectNode jNode = CreateOutput(value_node);
        // retrieve the current count
        UserState current = state.value();

        JsonNode params = value_node.get("params");
        String order_id = params.get("order_id").asText();
        System.out.println("Creating order");
        if (current == null) {
            // Send back to the order that we do not have an user!!!!

            jNode.put("error", "No user to create this order!");
        } else {
            current.order_ids.add(order_id);
            state.update(current);
            jNode.put("message", "Created an order");
            jNode.put("result", "success");
            jNode.put("order_id", order_id);
            jNode.put("user_id", current.id);
        }
        return jNode.toString();
    }

    private String ErrorState(JsonNode value_node) {
        ObjectNode jNode = CreateOutput(value_node);
        jNode.put("Error", "Something went wrong");
        return jNode.toString();
    }

    private String AddCredits(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);
        // retrieve the current count
        UserState current = state.value();

        JsonNode params = value_node.get("params");
        long delta_credits = params.get("amount").asLong();

        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {

            current.credits += delta_credits;
            state.update(current);
            jNode.put("message", "Added credits");
            jNode.put("result", "success");
            jNode.put("id", current.id);
            jNode.put("credits", current.credits);
        }
        return jNode.toString();
    }

    private String SubtractCredits(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);
        // retrieve the current count
        UserState current = state.value();

        JsonNode params = value_node.get("params");
        long delta_credits = params.get("amount").asLong();

        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            if (current.credits >= delta_credits) {
                current.credits -= delta_credits;
                state.update(current);
                jNode.put("result", "success");
                jNode.put("message", "Credits substracted");
            } else {
                jNode.put("result", "failure");
                jNode.put("message", "Not enough credits left");
            }

            jNode.put("id", current.id);
            jNode.put("credits", current.credits);
        }
        return jNode.toString();
    }

    private String GetCredits(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);
        // retrieve the current count
        UserState current = state.value();

        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            jNode.put("id", current.id);
            jNode.put("credits", current.credits);
        }
        return jNode.toString();
    }

    private ObjectNode CreateOutput(JsonNode input_node) {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        ObjectNode jNode = jsonParser.createObjectNode();
        jNode.put("request_id", input_node.get("request_id"));
        return jNode;
    }

    private String RemoveUser(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);

        // retrieve the current count
        UserState current = state.value();

        if (current == null) {
            jNode.put("result", "failure");
            jNode.put("message", "Could not find this user");
        } else {
            state.update(null);
            jNode.put("result", "success");
            jNode.put("message", "Correctly deleted the user");

        }

        return jNode.toString();

    }


    private String FindUser(JsonNode value_node) throws Exception {
        ObjectNode jNode = CreateOutput(value_node);

        // retrieve the current count
        UserState current = state.value();
        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            current.logins++;
            jNode.put("id", current.id);
            jNode.put("credits", current.credits);
            jNode.put("logins", current.logins);

            state.update(current);
        }

        return jNode.toString();

    }

    private String CreateUser(JsonNode value_node, String user_id) throws Exception {
        UserState current = new UserState();
        current.id = user_id;
        current.order_ids = new HashSet<>();
        ObjectNode jNode = CreateOutput(value_node);
        jNode.put("id", current.id);
        // write the state back
        state.update(current);
        return jNode.toString();
    }
}