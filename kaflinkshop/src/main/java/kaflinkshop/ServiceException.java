package kaflinkshop;

public class ServiceException extends Exception {

	public static final String GENERIC_MESSAGE = "Something went wrong.";

	public ServiceException() {
		super(GENERIC_MESSAGE);
	}

	public ServiceException(String message) {
		super(message);
	}


	public static class EntryNotFoundException extends ServiceException {

		public static final String GENERIC_MESSAGE = "Entry with the given ID does not exist.";

		public EntryNotFoundException() {
			super(GENERIC_MESSAGE);
		}

		public EntryNotFoundException(String entryName) {
			super("Entry '" + entryName + "' with the given ID does not exist.");
		}

	}


	public static class IllegalRouteException extends ServiceException {

		public static final String GENERIC_MESSAGE = "Unrecognised route.";

		public IllegalRouteException() {
			super(GENERIC_MESSAGE);
		}

		public IllegalRouteException(String message) {
			super(message);
		}

	}

}
