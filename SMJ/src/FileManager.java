import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class FileManager {
	public static String CSV_SEPARATOR = ",";
	public static String NEW_LINE_SEPARATOR = "\n";

	public static void createDirectory(String dir) {
		try {
			Path path = Paths.get(dir);
			deleteFromDisk(dir);
			Files.createDirectories(path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static public void deleteFromDisk(String dir) {
		File file = new File(dir);
		if (file.isDirectory()) {
			for (File c : file.listFiles()) {
				deleteFromDisk(c.getPath());
			}
		} else {
			System.gc();
		}
		file.delete();
	}

	public static void writeRecordsToDisk(String filename, List<Record> records) {
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(filename));
			for (Record r : records) {
				String line = String.join(CSV_SEPARATOR, r.getValues());
				bufferedWriter.write(line);
				bufferedWriter.write(NEW_LINE_SEPARATOR);
			}
			bufferedWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void appendRecordsToDisk(String filename, List<Record> records) {
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(filename, true));
			for (Record r : records) {
				String line = String.join(CSV_SEPARATOR, r.getValues());
				bufferedWriter.write(line);
				bufferedWriter.write(NEW_LINE_SEPARATOR);
			}
			bufferedWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void splitPartitionsOnDisk(String inputPath, String outputDir, int numPartitions,
			Integer[] partitionBoundaries) {
		try (BufferedReader reader = new BufferedReader(new FileReader(inputPath))) {
			BufferedWriter[] writers = new BufferedWriter[numPartitions];
			for (int i = 0; i < numPartitions; i++) {
				writers[i] = new BufferedWriter(new FileWriter(outputDir + Integer.toString(i) + ".csv"));
			}
			String line;
			while ((line = reader.readLine()) != null) {
				Integer val = Integer.parseInt(line.substring(0, line.indexOf(CSV_SEPARATOR)).replace("\"", ""));
				int bin = Arrays.binarySearch(partitionBoundaries, val);
				if (bin < 0)
					bin = -(bin + 1);

				writers[bin].write(line);
				writers[bin].write(NEW_LINE_SEPARATOR);
			}
			for (int i = 0; i < numPartitions; i++) {
				writers[i].close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void mergePartitionsOnDisk(String inputDir, String outputPath, int numPartitions) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
			for (int i = 0; i < numPartitions; i++) {
				try {
					BufferedReader reader = new BufferedReader(
							new FileReader(inputDir + "/" + Integer.toString(i) + ".csv"));
					String line;
					while ((line = reader.readLine()) != null) {
						writer.write(line);
						writer.write(NEW_LINE_SEPARATOR);
					}
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
