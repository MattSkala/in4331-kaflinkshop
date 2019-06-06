package kaflinkshop;

public enum OperationResult {
	SUCCESS(0),
	FAILURE(1),
	ERROR(2),

	;

	private final int code;

	public int getCode() {
		return code;
	}

	OperationResult(int code) {
		this.code = code;
	}

	public static OperationResult fromCode(int code) {
		for (OperationResult value : OperationResult.values()) {
			if (value.code == code)
				return value;
		}
		throw new IllegalArgumentException("Code not recognised.");
	}

}
