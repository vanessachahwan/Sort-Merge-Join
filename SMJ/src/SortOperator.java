import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

public class SortOperator {
	private Table table;
	private BufferManager bufferManager;
	private Comparator<Record> comparator;

	public SortOperator(Table table, Comparator<Record> comparator) {
		this.table = table;
		this.bufferManager = new BufferManager(this.table);
		this.comparator = comparator;
	}

	public void externalSort(String runsDir, String mergeRunsDir, String outputPath) {
		sort(runsDir);
		String result = merge(runsDir, mergeRunsDir);
		try {
			Files.move(Paths.get(result), Paths.get(outputPath), StandardCopyOption.REPLACE_EXISTING);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void sort(String runsDir) {
		FileManager.createDirectory(runsDir);
		int runsNum = (bufferManager.getNumPages() - 1) / Database.NUM_BUFFERS + 1;
		for (int r = 0; r < runsNum; ++r) {
			List<Record> buffersRecords = new ArrayList<Record>();
			if (r == runsNum - 1) {
				int remainingPages = bufferManager.getNumPages() - (runsNum - 1) * Database.NUM_BUFFERS;
				for (int p = 0; p < remainingPages; ++p) {
					buffersRecords.addAll(bufferManager.loadPageToMemory(r * Database.NUM_BUFFERS + p));
				}
			} else {
				for (int p = 0; p < Database.NUM_BUFFERS; ++p) {
					buffersRecords.addAll(bufferManager.loadPageToMemory(r * Database.NUM_BUFFERS + p));
				}
			}
			buffersRecords.sort(comparator);
			String filename = "run_" + r + ".csv";
			FileManager.writeRecordsToDisk(runsDir + "/" + filename, buffersRecords);
		}
	}

	public String merge(String sortRunsDir, String mergeRunsDir) {
		FileManager.createDirectory(mergeRunsDir);
		int sortRunsNum = (bufferManager.getNumPages() - 1) / Database.NUM_BUFFERS + 1;
		int mergeRunsNum = (int) Math.ceil(Math.log(sortRunsNum) / Math.log(Database.NUM_BUFFERS - 1));
		int runsToMergeTotalNum = sortRunsNum;
		int runsNum;
		int level = 0;
		do {

			runsNum = (runsToMergeTotalNum - 1) / (Database.NUM_BUFFERS - 1) + 1;

			for (int i = 0; i < runsNum; i++) {
				List<BufferManager> tablesPM = new ArrayList<>();

				int nbRunsToMerge = i == runsNum - 1 ? runsToMergeTotalNum - (runsNum - 1) * (Database.NUM_BUFFERS - 1)
						: Database.NUM_BUFFERS - 1;

				if (nbRunsToMerge == 1) {
					String path;
					if (level == 0) {
						path = sortRunsDir + "/" + "run_" + (runsToMergeTotalNum - 1) + ".csv";
					} else {
						path = mergeRunsDir + "/" + "run_" + (level - 1) + "_" + (runsToMergeTotalNum - 1) + ".csv";
					}
					String outputPath = mergeRunsDir + "/" + "run_" + (level) + "_" + (runsToMergeTotalNum - 1)
							+ ".csv";

					try {
						Files.copy(Paths.get(path), Paths.get(outputPath));
					} catch (Exception e) {
						e.printStackTrace();
					}
					continue;
				}

				for (int j = 0; j < nbRunsToMerge; j++) {
					String path;
					if (level == 0) {
						path = sortRunsDir + "/" + "run_" + (i * (Database.NUM_BUFFERS - 1) + j) + ".csv";
					} else {
						path = mergeRunsDir + "/" + "run_" + (level - 1) + "_" + (i * (Database.NUM_BUFFERS - 1) + j)
								+ ".csv";
					}
					tablesPM.add(new BufferManager(new Table("", path)));
				}

				ArrayList<Iterator<Record>> records = new ArrayList<>();
				Queue<Pair<Record, Integer>> pq = new PriorityQueue<Pair<Record, Integer>>(
						(o1, o2) -> comparator.compare(o1.getFirst(), o2.getFirst()));
				int[] nbLoadedPages = new int[tablesPM.size()];

				for (int k = 0; k < tablesPM.size(); k++) {
					records.add(tablesPM.get(k).loadPageToMemory(0).iterator());
					nbLoadedPages[k] = 1;
					pq.add(new Pair<>(records.get(k).next(), k));
				}

				List<Record> buffersRecords = new ArrayList<>();
				String outputPath = mergeRunsDir + "/" + "run_" + (level) + "_" + (i) + ".csv";

				while (pq.size() > 0) {
					Pair<Record, Integer> nextRecord = pq.remove();
					buffersRecords.add(nextRecord.getFirst());
					if (buffersRecords.size() == Database.RECORDS_PER_PAGE) {
						FileManager.appendRecordsToDisk(outputPath, buffersRecords);
						buffersRecords.clear();
					}
					if (records.get(nextRecord.getSecond()).hasNext()) {
						pq.add(new Pair<>(records.get(nextRecord.getSecond()).next(), nextRecord.getSecond()));
					} else {
						if (nbLoadedPages[nextRecord.getSecond()] < tablesPM.get(nextRecord.getSecond())
								.getNumPages()) {
							records.set(nextRecord.getSecond(), tablesPM.get(nextRecord.getSecond())
									.loadPageToMemory(nbLoadedPages[nextRecord.getSecond()]).iterator());
							nbLoadedPages[nextRecord.getSecond()]++;
							pq.add(new Pair<>(records.get(nextRecord.getSecond()).next(), nextRecord.getSecond()));
						}
					}
				}
				if (!buffersRecords.isEmpty()) {
					FileManager.appendRecordsToDisk(outputPath, buffersRecords);
				}

			}

			level++;
			runsToMergeTotalNum = runsNum;

		} while (runsNum != 1);
		String result = mergeRunsDir + "/" + "run_" + (Math.max(0, mergeRunsNum - 1)) + "_0.csv";
		return result;
	}
}
