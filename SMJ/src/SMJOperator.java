import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SMJOperator {
	private Table r;
	private Table l;
	private BufferManager PageManagerR;
	private int PageinR;
	private BufferManager PageManagerL;
	private int PageinL;
	private Comparator<Record> comparator = (r1, r2) -> Integer.compare(Integer.parseInt(r1.getValue(0)),
			Integer.parseInt(r2.getValue(0)));
	private boolean emptyTable;
	private String outputPath;
	long durationSort;
	long durationMerge;

	public SMJOperator(Table t1, Table t2, String outputPath) {
		emptyTable = t1.getNumRecords() == 0 || t2.getNumRecords() == 0;
		this.r = t1;
		this.l = t2;
		this.outputPath = outputPath;
	}

	private void sort() {
		String pathSortedR = "temp/sorted_" + r.getTablename() + ".csv";
		String pathSortedL = "temp/sorted_" + l.getTablename() + ".csv";
		SortOperator sortOperatorR = new SortOperator(r, comparator);
		sortOperatorR.externalSort("temp/run_" + r.getTablename(), "temp/merge_" + r.getTablename(), pathSortedR);
		SortOperator sortOperatorL = new SortOperator(l, comparator);
		sortOperatorL.externalSort("temp/run_" + l.getTablename(), "temp/merge_" + l.getTablename(), pathSortedL);
		this.r = new Table("sorted_" + r.getTablename(), pathSortedR);
		this.l = new Table("sorted_" + l.getTablename(), pathSortedL);
	}

	private void createPageManager() {
		this.PageManagerR = new BufferManager(r);
		this.PageinR = PageManagerR.getNumPages();
		this.PageManagerL = new BufferManager(l);
		this.PageinL = PageManagerL.getNumPages();
	}

	private void merge() {
		List<Record> joined = new ArrayList<Record>();
		int RightPointer = 0, LeftPointer = 0;
		int markRecord = -1, markPage = -1;
		int right = 0;
		boolean end = false;
		boolean lock = true;
		boolean firstWrite = true;
		int count = 0;
		int maxRecords = Database.RECORDS_PER_PAGE * Database.NUM_BUFFERS;
		List<Record> PageR = PageManagerR.loadPageToMemory(right);
		for (int x = 0; x < this.PageinL; x++) {
			LeftPointer = 0;
			lock = true;
			List<Record> PageL = PageManagerL.loadPageToMemory(x);
			while (end == false && lock == true) {
				if (markRecord == -1 && markPage == -1) {
					while (lock && comparator.compare(PageL.get(LeftPointer), PageR.get(RightPointer)) < 0) {
						if ((LeftPointer + 1) > PageL.size() - 1) {
							lock = false;
						} else {
							LeftPointer++;
						}
					}
					while (!end && lock && comparator.compare(PageL.get(LeftPointer), PageR.get(RightPointer)) > 0) {

						if ((RightPointer + 1) > PageR.size() - 1) {
							right++;
							if (right > this.PageinR - 1) {
								end = true;
							} else {
								PageR = PageManagerR.loadPageToMemory(right);
								RightPointer = 0;
							}
						} else {
							RightPointer++;
						}
					}
					if (lock && !end) {
						markPage = right;
						markRecord = RightPointer;
					}
				}
				if (!end && lock && comparator.compare(PageL.get(LeftPointer), PageR.get(RightPointer)) == 0) {
					List<String> resultList = new ArrayList<String>(Arrays.asList(PageL.get(LeftPointer).getValues()));
					resultList.addAll(Arrays.asList(PageR.get(RightPointer).getValues()));
					String[] result = resultList.toArray(new String[0]);
					Record r = new Record(result);
					joined.add(r);
					count++;
					if (count == maxRecords) {
						if (firstWrite) {
							FileManager.writeRecordsToDisk(outputPath, joined);
							firstWrite = false;
						} else {
							FileManager.appendRecordsToDisk(outputPath, joined);
						}
						joined.clear();
						count = 0;
					}
					RightPointer++;
					if (RightPointer > PageR.size() - 1) {
						right++;
						if (right > this.PageinR - 1) {
							RightPointer = markRecord;
							if ((right - 1) != markPage) {
								right = markPage;
								PageR = PageManagerR.loadPageToMemory(right);
							}
							LeftPointer++;
							if (LeftPointer > PageL.size() - 1) {
								lock = false;
							}
							markRecord = -1;
							markPage = -1;
						} else {
							PageR = PageManagerR.loadPageToMemory(right);
							RightPointer = 0;
						}

					}
				} else {
					if (lock && !end) {
						RightPointer = markRecord;
						if (right != markPage) {
							right = markPage;
							PageR = PageManagerR.loadPageToMemory(right);
						}
						LeftPointer++;
						if (LeftPointer > PageL.size() - 1) {
							lock = false;
						}
						markRecord = -1;
						markPage = -1;
					}

				}
			}
			if (end) {
				break;
			}
		}
		if (!joined.isEmpty()) {
			if (firstWrite) {
				FileManager.writeRecordsToDisk(outputPath, joined);
			} else {
				FileManager.appendRecordsToDisk(outputPath, joined);
			}
		}
	}

	public void join() {
		long startTime = System.currentTimeMillis();
		if (emptyTable) {
			FileManager.writeRecordsToDisk(outputPath, new ArrayList<Record>());
			return;
		}
		sort();
		long endTime = System.currentTimeMillis();
		durationSort = endTime - startTime;
		startTime = System.currentTimeMillis();
		createPageManager();
		merge();
		endTime = System.currentTimeMillis();
		durationMerge = endTime - startTime;
	}

}
