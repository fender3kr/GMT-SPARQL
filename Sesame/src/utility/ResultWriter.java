package utility;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class ResultWriter {
	private String directoryPath;
	private String fileName;
	private PrintWriter writer;
	
	public ResultWriter(String directoryPath, String fileName) throws IOException {
		this.directoryPath = directoryPath;
		this.fileName = fileName;
		
		File directory = new File(directoryPath);
		if(!directory.exists()) 
			directory.mkdir();
		
		writer = new PrintWriter(new FileWriter(directoryPath + System.getProperty("file.separator") + fileName));
	}
	
	public void writeLine(String line) {
		writer.println(line);
	}
	
	public void close() {
		if(writer != null) 
			writer.close();
	}
}
