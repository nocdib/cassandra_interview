package interview;

public enum Operation {
	READ("READ"),
	WRITE("WRITE"),
	MIXED("MIXED");

	private String value;

	Operation(final String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.getValue();
    }

}
