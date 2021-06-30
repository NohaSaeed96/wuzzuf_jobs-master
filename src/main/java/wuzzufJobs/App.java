package wuzzufJobs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;

public class App {

	private static final String COMMA_DELIMITER = ",";
	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		JobDao testing = new JobDao();
		testing.read_file();
		testing.clean_data();
		testing.company_job_count();
	}

}
