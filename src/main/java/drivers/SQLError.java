package drivers;

@SuppressWarnings("serial")
public class SQLError extends Exception {
	public SQLError(String message) {
        super(message);
    }
}