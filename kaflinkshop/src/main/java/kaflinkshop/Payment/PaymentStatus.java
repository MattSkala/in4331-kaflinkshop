package kaflinkshop.Payment;

public enum PaymentStatus {

	INVALID(0),
	PAID(1),
	FAILED(2),
	CANCELLED(3),

	;

	private int statusCode;

	public int getStatusCode() {
		return statusCode;
	}

	PaymentStatus(int statusCode) {
		this.statusCode = statusCode;
	}

}
