package kaflinkshop;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.util.UUID;

public class JsonParser implements FlatMapFunction<String, Tuple2<String, JsonNode>> {
    private transient ObjectMapper jsonParser;
    private final String idParam;

    JsonParser(String idParam) {
        this.idParam = idParam;
    }

    @Override
    public void flatMap(String value, Collector<Tuple2<String, JsonNode>> out) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }
        JsonNode jsonNode;
        try {
            jsonNode = jsonParser.readValue(value, JsonNode.class);
        } catch (Exception e) {
            System.out.println("Could not be parsed");
            return;
        }
        JsonNode params = jsonNode.get("params");
        String id;

        System.out.println(jsonNode.toString());

        if (params.has(idParam)) {
            id = params.get(idParam).asText();
        } else {
            id = UUID.randomUUID().toString();
        }

        out.collect(new Tuple2<>(id, jsonNode));
    }
}

