import java.io.File;

import operation.GraphOperation;
import operation.graph.ConnectedComponents;
import operation.graph.NodeEccentricity;
import operation.graph.PageRank;
import operation.graph.TestOperation;
import operation.graph.TriangleCounter;
import operation.initial.InitialConnectedComponents;
import operation.initial.InitialTriangleCounter;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.memory.MemoryStore;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import exception.OperationException;


public class GraphMiningMain {
	private static Repository repository;
	private static RepositoryConnection connection;
	private static String endPoint = "http://localhost:8080/openrdf-sesame";
	private static String repositoryID = "ttt";
	private static String INPUT_FILE = "/home/phantom/Dataset/SNAP/soc-Epinions1.txt.nt";
	
	public static void main(String[] args) {
		GraphOperation operation = null;
		
		try {
			//initialize();
			initializeFile();
			//initializeRemote();
			//operation = new NodeEccentricity(connection, "<urn:0>");
			//operation = new TestOperation(connection, "<urn:0>");
			operation = new TriangleCounter(connection, true, true);
			//operation = new InitialTriangleCounter(connection);
			//operation = new ConnectedComponents(connection);
			//operation = new InitialConnectedComponents(connection);
			//operation = new Eccentricity(connection);
			//operation = new PageRank(connection, 0.00001f, 0.85f);
			operation.execute();
			operation.close();
		} catch (OperationException e) {
			e.printStackTrace();
		} finally {
			if (connection != null)
				connection.close();
		}
	}

	private static void initializeRemote() throws OperationException {
		File inputFile = null;
		
		Logger rootLogger = (Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(ch.qos.logback.classic.Level.OFF);
		try {
			//inputFile = new File(INPUT_FILE);
			repository = new HTTPRepository(endPoint, repositoryID);
			repository.initialize();
			connection = repository.getConnection();
			//connection.add(inputFile, "", RDFFormat.TURTLE);
		} catch (Exception e) {
			throw new OperationException("Cannot connect to repository.");
		}
	}
	
	private static void initialize() throws OperationException {
		File inputFile = null;
		
		Logger rootLogger = (Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(ch.qos.logback.classic.Level.OFF);
		try {
			inputFile = new File(INPUT_FILE);
			repository = new SailRepository(new MemoryStore());
			repository.initialize();
			connection = repository.getConnection();
			connection.add(inputFile, "", RDFFormat.TURTLE);
		} catch (Exception e) {
			throw new OperationException("Cannot connect to repository.");
		}
	}
	
	private static void initializeFile() throws OperationException {
		File inputFile = null;
		File dataDir = null;
		Logger rootLogger = (Logger)LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
		rootLogger.setLevel(ch.qos.logback.classic.Level.OFF);
		try {
			dataDir = new File("/home/phantom/Desktop/data/");
			inputFile = new File(INPUT_FILE);
			String indexes = "spoc,posc,cosp";
			//String indexes = "";
			repository = new SailRepository(new NativeStore(dataDir, indexes));
			repository.initialize();
			connection = repository.getConnection();
			connection.add(inputFile, "", RDFFormat.TURTLE);
		} catch (Exception e) {
			throw new OperationException("Cannot connect to repository.");
		}
	}
}
