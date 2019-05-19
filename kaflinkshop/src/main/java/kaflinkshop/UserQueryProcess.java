package kaflinkshop;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class UserQueryProcess
        extends KeyedProcessFunction<Tuple, Tuple2<String, JsonNode>, String> {

    /**
     * The state that is maintained by this process function
     */
    private ValueState<UserState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
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
            default:
                output = ErrorState(value_node);
        }

        out.collect(output);

    }

    private String ErrorState(JsonNode value_node){
        ObjectNode jNode = CreateOutput(value_node);
        jNode.put("Error", "Something went wrong");
        return jNode.toString();
    }

    private ObjectNode CreateOutput(JsonNode input_node){
        ObjectNode jNode = jsonParser.createObjectNode();
        jNode.put("request_id", input_node.get("request_id"));
        return jNode;
    }

    private String FindUser(JsonNode value_node) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        ObjectNode jNode = CreateOutput(value_node);

        // retrieve the current count
        UserState current = state.value();

        System.out.println(current);

        if (current == null) {
            jNode.put("error", "Something went wrong!");
        } else {
            jNode.put("id", current.id);
            jNode.put("credits", current.credits);
            jNode.put("logins", current.logins);

            current.logins++;
            state.update(current);
        }

        return jNode.toString();

    }

    private String CreateUser(JsonNode value_node, String user_id) throws Exception {
        UserState current = new UserState();
        current.id = user_id;

        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        ObjectNode jNode = CreateOutput(value_node);
        jNode.put("id", current.id);
        // write the state back
        state.update(current);
        return jNode.toString();
    }
}