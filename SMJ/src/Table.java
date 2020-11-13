import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class Table {
	public static String CSV_SPLIT_BY = ",";

	private String tablename;
	private String filename;
	private List<Integer> recordsOffset;
	private List<Integer> recordsLength;
	private int numFields;
	private int numRecords;

	public Table(String tablename, String filename) {
		this.tablename = tablename;
		this.filename = filename;
		this.recordsOffset = new ArrayList<Integer>();
		this.recordsLength = new ArrayList<Integer>();
		computeMetadata();
	}

	private void computeMetadata() {
		int n = 0;
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(filename));
			recordsOffset.add(0);
			while ((line = br.readLine()) != null) {
				if (n == 0) {
					String[] l = line.split(CSV_SPLIT_BY);
					numFields = l.length;
				}
				recordsLength.add(line.getBytes().length);
				recordsOffset.add(line.getBytes().length + recordsOffset.get(recordsOffset.size() - 1) + 1);
				n++;
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		numRecords = n;
	}

	public List<Record> getAllRecords() {
		List<Record> records = new ArrayList<Record>();
		BufferedReader br = null;
		String line = "";
		try {
			br = new BufferedReader(new FileReader(filename));
			recordsOffset.add(0);
			while ((line = br.readLine()) != null) {
				Record r = new Record(line.replace("\"", "").split(Table.CSV_SPLIT_BY));
				records.add(r);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return records;
	}

	public String getTablename() {
		return tablename;
	}

	public String getFilename() {
		return filename;
	}

	public List<Integer> getRecordsOffset() {
		return recordsOffset;
	}

	public List<Integer> getRecordsLength() {
		return recordsLength;
	}

	public int getNumFields() {
		return numFields;
	}

	public int getNumRecords() {
		return numRecords;
	}
}