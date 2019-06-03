package kaflinkshop.User;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashSet;
import java.util.Set;

public class UserState {

	public String userID;
	public long credits;
	public Set<String> orders;

	public UserState() {
		this.userID = null;
		this.credits = 0;
		this.orders = new HashSet<>();
	}

	public UserState(String userID) {
		this.userID = userID;
		this.credits = 0;
		this.orders = new HashSet<>();
	}

	public UserState(String userID, long credits) {
		this.userID = userID;
		this.credits = credits;
		this.orders = new HashSet<>();
	}

	public JsonNode toJsonNode(ObjectMapper objectMapper) {
		ObjectNode node = objectMapper.createObjectNode();
		node.put("user_id", this.userID);
		node.put("balance", this.credits);
		node.put("orders", this.orders.toString());
		return node;
	}

}
