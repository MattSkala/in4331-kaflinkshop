package kaflinkshop;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;

public class Test {

	public static void main(String[] args) {
		String json = "{\n" +
				"\t\"input\": {\n" +
				"\t\t\"consumer\": \"cons-1\",\n" +
				"\t\t\"params\": {\n" +
				"\t\t\t\"foo\": \"foo-val-1\",\n" +
				"\t\t\t\"bar\": \"bar-val-1\"\n" +
				"\t\t},\n" +
				"\t\t\"request_id\": \"request_id_val\",\n" +
				"\t\t\"route\": \"route/to/service-1\"\n" +
				"\t},\n" +
				// "\t\"path\": [\n" +
				// "\t\t{\n" +
				// "\t\t\t\"consumer\": \"cons-1\",\n" +
				// "\t\t\t\"route\": \"route/to/service-1\",\n" +
				// "\t\t\t\"state\": \"state-1\",\n" +
				// "\t\t\t\"params\": {\n" +
				// "\t\t\t\t\"params\": {\n" +
				// "\t\t\t\t\t\"foo\": \"foo-val-1\",\n" +
				// "\t\t\t\t\t\"bar\": \"bar-val-1\"\n" +
				// "\t\t\t\t}\n" +
				// "\t\t\t}\n" +
				// "\t\t},\n" +
				// "\t\t{\n" +
				// "\t\t\t\"consumer\": \"cons-2\",\n" +
				// "\t\t\t\"route\": \"route/to/service-2\",\n" +
				// "\t\t\t\"state\": \"state-2\",\n" +
				// "\t\t\t\"params\": {\n" +
				// "\t\t\t\t\"params\": {\n" +
				// "\t\t\t\t\t\"bar\": \"bar-val-2\",\n" +
				// "\t\t\t\t\t\"baz\": \"baz-val-2\"\n" +
				// "\t\t\t\t}\n" +
				// "\t\t\t}\n" +
				// "\t\t}\n" +
				// "\t],\n" +
				"\t\"state\": {\n" +
				"\t\t\"route\": \"route/to/service-2\",\n" +
				"\t\t\"sender\": \"sender-val\",\n" +
				"\t\t\"web\": false,\n" +
				"\t\t\"state\": null\n" +
				"\t},\n" +
				"\t\"params\": {\n" +
				"\t\t\"order_id\": \"order-orderID-val\",\n" +
				"\t\t\"user_id\": \"user-orderID-val\"\n" +
				"\t}\n" +
				"}";
		Message message = Message.parse(json);
		System.out.println(message);

		HashMap<String, Integer> map = new HashMap<>();
		map.put("haha", 4);
		map.put("xD", 2);
		System.out.println(map.toString());

		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readValue("{haha=4, xD=2}", JsonNode.class);
			System.out.println(node);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
