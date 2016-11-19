package etl.util;

public enum DBType {

    NONE("none"),
	VERTICA("vertica"),
    HIVE("hive");
	
    private final String value;

    DBType(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static DBType fromValue(String v) {
        for (DBType c: DBType.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
