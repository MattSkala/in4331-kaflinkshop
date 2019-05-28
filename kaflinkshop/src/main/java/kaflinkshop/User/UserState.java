package kaflinkshop.User;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class UserState {

	public String userID;
	public long credits;

	public UserState() {
		this.userID = null;
		this.credits = 0;
	}

	public UserState(String userID) {
		this.userID = userID;
		this.credits = 0;
	}

	public UserState(String userID, long credits) {
		this.userID = userID;
		this.credits = credits;
	}

	public JsonNode toJsonNode(ObjectMapper objectMapper) {
		ObjectNode node = objectMapper.createObjectNode();
		node.put("user_id", this.userID);
		node.put("balance", this.credits);
		return node;
	}

}
