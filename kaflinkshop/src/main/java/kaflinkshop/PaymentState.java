package kaflinkshop;

public class PaymentState {

	public PaymentStatus status;

	PaymentState() {
		status = PaymentStatus.INVALID;
	}

	PaymentState(PaymentStatus status) {
		this.status = status;
	}

}
