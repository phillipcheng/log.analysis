package etl.util;

public class CacheItemNotFoundException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public CacheItemNotFoundException(String message) {
		super(message);
	}
}
