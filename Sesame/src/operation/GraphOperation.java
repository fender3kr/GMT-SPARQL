package operation;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;
import utility.ResultWriter;

public abstract class GraphOperation {
	private static final String resultDirectory = System.getProperty("user.dir") + System.getProperty("file.separator") + "Result";
	protected String name;
	protected ResultWriter writer;
	protected RepositoryConnection connection;
	
	protected GraphOperation(String name, RepositoryConnection connection) throws OperationException {
		if(name == null || connection == null)
			throw new OperationException("Cannot create graph operation.");
		
		this.name = name;
		this.connection = connection;
		
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
		Date date = new Date(); 
		try {
			writer = new ResultWriter(resultDirectory + System.getProperty("file.separator") + name, dateFormat.format(date));
		} catch (IOException e) {
			throw new OperationException("Cannot create result writer.");
		}
	}
	
	public void execute() {
		long start = System.currentTimeMillis();
		long p0 = start;
		init();
		System.out.println("\tInitialization: " + (System.currentTimeMillis() - start) + " ms");
		start = System.currentTimeMillis();
		process();
		System.out.println("\tProcess: " + (System.currentTimeMillis() - start) + " ms");
		start = System.currentTimeMillis();
		end();
		System.out.println("\tFinalization: " + (System.currentTimeMillis() - start) + " ms");
		System.out.println("Total: " + (System.currentTimeMillis() - p0) + " ms");
	}
	
	protected abstract void init();
	protected abstract void process();
	protected abstract void end();
	
	public void close() {
		if(writer != null)
			writer.close();
	}
}
