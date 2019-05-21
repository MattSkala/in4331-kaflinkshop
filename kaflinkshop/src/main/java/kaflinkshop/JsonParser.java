package kaflinkshop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class JsonParser implements FlatMapFunction<String, Tuple2<String, JsonNode>> {
    private transient ObjectMapper jsonParser;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, JsonNode>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode;
        try {
            jsonNode = jsonParser.readValue(value, JsonNode.class);
        } catch(Exception e){
            System.out.println("Could not be parsed");
            return;
        }
        JsonNode params = jsonNode.get("params");
        String user_id;

        System.out.println(jsonNode.toString());

        if(params.has("user_id")){
//				System.out.println("Getting used key");
            user_id = params.get("user_id").asText();
        } else {
//				System.out.println("Creating new key");
            user_id = UUID.randomUUID().toString();
        }
        out.collect(new Tuple2<>(user_id, jsonNode));
    }
}

