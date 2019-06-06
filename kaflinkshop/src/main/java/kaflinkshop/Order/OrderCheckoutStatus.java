package kaflinkshop.Order;

public enum OrderCheckoutStatus {
	NOT_PROCESSED(0),
	IN_PROGRESS(1),
	SUCCEEDED(2),
	FAILED(3),
	INVALID(4),

	;

	private final int code;

	public int getCode() {
		return code;
	}

	OrderCheckoutStatus(int code) {
		this.code = code;
	}

	public OrderCheckoutStatus fromCode(int code) {
		for (OrderCheckoutStatus value : OrderCheckoutStatus.values()) {
			if (value.code == code)
				return value;
		}
		throw new IllegalArgumentException("Unrecognised code.");
	}

}
