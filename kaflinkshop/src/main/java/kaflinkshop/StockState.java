package kaflinkshop;

public class StockState {
	public String id;
	public long amount;

	StockState() {
	}

	StockState(String id) {
		this.id = id;
	}

	StockState(String id, long amount) {
		this.id = id;
		this.amount = amount;
	}

}
