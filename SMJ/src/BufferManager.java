import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class BufferManager {
	public static char NEW_LINE_SEPARATOR = '\n';

	private Table table;
	private int totalRecords;
	private int numPages;

	public BufferManager(Table table) {
		this.table = table;
		this.totalRecords = table.getNumRecords();
		this.numPages = (totalRecords - 1) / Database.RECORDS_PER_PAGE + 1;
	}

	public List<Record> loadPageToMemory(int p) {
		List<Record> page = new ArrayList<Record>();
		if (p >= numPages || totalRecords==0) {
			return page;
		}
		List<Integer> recordsOffset = table.getRecordsOffset();
		List<Integer> recordsLength = table.getRecordsLength();
		try {
			RandomAccessFile raf = new RandomAccessFile(table.getFilename(), "r");
			FileChannel fc = raf.getChannel();
			int start = recordsOffset.get(p * Database.RECORDS_PER_PAGE);
			int end;
			if (p == numPages - 1) {
				end = recordsOffset.get(totalRecords - 1) + recordsLength.get(totalRecords - 1);
			} else {
				end = recordsOffset.get((p + 1) * Database.RECORDS_PER_PAGE) - 1;
			}
			MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, start, end - start);
			String line = "";
			for (int j = 0; j < end - start; j++) {
				if ((char) buffer.get(j) != NEW_LINE_SEPARATOR) {
					line += (char) buffer.get(j);
				} else {
					Record r = new Record(line.replace("\"", "").split(Table.CSV_SPLIT_BY));
					page.add(r);
					line = "";
				}
			}
			Record r = new Record(line.replace("\"", "").split(Table.CSV_SPLIT_BY));
			page.add(r);
			buffer.force();
			buffer.clear();
			fc.close();
			raf.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return page;
	}

	public int getNumPages() {
		return numPages;
	}
}