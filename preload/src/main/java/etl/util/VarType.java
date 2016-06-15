package etl.util;

public enum VarType {

    STRING("string"),
    INT("int"),
    FLOAT("float"),
    DATE("date"),
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
