package etl.util;

public enum OSType {
	
	MAC("mac"),
	UNIX("unix"),
	WINDOWS("windows");
	
    private final String value;

    OSType(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static OSType fromValue(String v) {
        for (OSType c: OSType.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }
}
