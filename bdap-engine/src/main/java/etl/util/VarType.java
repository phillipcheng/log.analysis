package etl.util;

public enum VarType {

    STRING("string"),
    //extends string
    JAVASCRIPT("javascript"),
    REGEXP("regexp"),
    GLOBEXP("glob"),
    STRINGLIST("stringlist"),
    
	TIMESTAMP("timestamp"),
	DATE("date"),
    NUMERIC("numeric"),
    INT("int"),

    FLOAT("float"),
    LIST("list"),
    ARRAY("array"),
    BOOLEAN("boolean"),
    OBJECT("object");
	
    private final String value;

    VarType(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static VarType fromValue(String v) {
        for (VarType c: VarType.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}
