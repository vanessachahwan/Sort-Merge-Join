public class SMJThread implements Runnable {
	Table t1;
	Table t2;
	String output_path;

	public SMJThread(Table t1, Table t2, String filename) {
		this.t1 = t1;
		this.t2 = t2;
		this.output_path = filename;
	}

	@Override
	public void run() {
		SMJOperator smj = new SMJOperator(t1, t2, output_path);
		smj.join();
	}
}
