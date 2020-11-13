public class Record {
	private String[] values;

	public Record(String[] values) {
		this.values = values;
	}

	public String[] getValues() {
		return values;
	}

	public String getValue(int column) {
		return values[column];
	}
}