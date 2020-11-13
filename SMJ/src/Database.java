import java.util.HashMap;
import java.util.Map;

public class Database {
	public static int NUM_BUFFERS = 5;
	public static int RECORDS_PER_PAGE = 10000;
	String fileDir;
	Map<String, Table> tables;

	public Database(String fileDir) {
		this.fileDir = fileDir;
		this.tables = new HashMap<String, Table>();
	}
	
	public Database(String fileDir, int NumBuffers, int RecordsPerPage) {
		this(fileDir);
		Database.NUM_BUFFERS = NumBuffers;
		Database.RECORDS_PER_PAGE = RecordsPerPage;
	}

	public Table addTable(String tablename, String filename) {
		Table t = new Table(tablename, fileDir + "/" + filename);
		tables.put(tablename, t);
		return t;
	}

	public void deleteTable(String tablename) {
		tables.remove(tablename);
	}

	public static void main(String[] args) {
		Database d = new Database("database");
		Table t1 = d.addTable("clients", "clients.csv");
		Table t2 = d.addTable("purchases", "purchases.csv");
		
		System.out.println("Creating temporary directory...\n");
		String tempDir = "temp";
		FileManager.createDirectory(tempDir);
		
		System.out.println("Single-threaded Join Implementation");
		long startTime = System.currentTimeMillis();
		SMJOperator j = new SMJOperator(t1, t2, "database/joined.csv");
		j.join();
		long endTime = System.currentTimeMillis();
		long duration = endTime - startTime;
		System.out.println("Total Duration: " + duration + " ms");
		System.out.println("Sort Duration: " + j.durationSort + " ms");
		System.out.println("Merge Duration: " + j.durationMerge + " ms\n");
		

		System.out.println("Multi-threaded Join Implementation");
		long startTime0 = System.currentTimeMillis();
		SMJMainThread mj = new SMJMainThread(t1, t2, 5, "database/joined_thread.csv");
		mj.join();
		long endTime0 = System.currentTimeMillis();
		long duration0 = endTime0 - startTime0;
		System.out.println("Total duration: " + duration0 + " ms");
		System.out.println("Partitioning duration: " + mj.duration_partition + " ms");
		System.out.println("Threads duration: " + mj.duration_threads + " ms");
		System.out.println("Combine duration: " + mj.duration_combine + " ms");
		
		System.out.println("\nCleaning Disk...");
		FileManager.deleteFromDisk(tempDir);
	}
}
