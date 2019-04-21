package operation.graph;

import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;
import operation.GraphOperation;

public class TriangleCounter extends GraphOperation {
	private static final String NAME = "Triangle Counting";
	
	private boolean isDirected;
	private boolean isMultithreaded;
	private Thread workers[];
	private int counts[];
	
	private static final String QUERY_INIT_GRAPH = ""
			+ "CREATE GRAPH <http://workingGraph>;";
	
	private static final String QUERY_STORE_INITIAL_GRAPH = ""
			+ "INSERT { GRAPH <http://workingGraph> { ?vertex <temp:links> ?neighbor }}\n"
			+ "WHERE {\n"
			+ "  SELECT ?vertex ?neighbor\n"
			+ "  WHERE {\n"
			+ "    ?vertex <urn:connectedTo>|^<urn:connectedTo> ?neighbor .\n"
			+ "    FILTER (STR(?vertex) < STR(?neighbor))\n"
			+ "  }\n"
			+ "};";
	private static final String QUERY_FIND_TRIANGLES = ""
			+ "SELECT (COUNT(DISTINCT *) AS ?count)\n"
			+ "WHERE {\n"
			+ "  GRAPH <http://workingGraph> {\n"
			+ "    ?x ?p ?y .\n"
			+ "    ?y ?p ?z .\n"
			+ "    ?x ?p ?z\n"
			+ "  }\n"
			+ "}";
	private static final String QUERY_FIND_TRIANGLES_FOR_DIRECTED_GRAPH = ""
			+ "SELECT (COUNT(*) AS ?count)\n"
            + "WHERE {\n"
			+ "  {\n"
            + "    ?x ?a ?y .\n"
			+ "    ?y ?b ?z .\n"
            + "    ?z ?c ?x .\n"
			+ "    FILTER (STR(?x) < STR(?y)) .\n"
            + "    FILTER (STR(?y) < STR(?z))\n"
			+ "  }\n"
            + "  UNION\n"
			+ "  {\n"
            + "    ?x ?a ?y .\n"
			+ "    ?y ?b ?z .\n"
            + "    ?z ?c ?x .\n"
			+ "    FILTER (STR(?y) > STR(?z)) .\n"
            + "    FILTER (STR(?z) > STR(?x))\n"
			+ "  }\n"
            + "  UNION\n"
			+ "  {\n"
            + "    ?x ?a ?y .\n"
			+ "    ?y ?b ?z .\n"
            + "    ?x ?c ?z\n"
			+ "  }\n"
            + "}";
	private static final String[] QUERY_FOR_MULTITHREADS = {
			"SELECT (COUNT(*) AS ?count)\n" +
			"WHERE {\n" +
			"  ?x ?a ?y .\n" +
			"  ?y ?b ?z .\n" +
			"  ?z ?c ?x .\n" +
			"  FILTER (STR(?x) < STR(?y)) .\n" +
			"  FILTER (STR(?y) < STR(?z))\n" +
			"}",
			"SELECT (COUNT(*) AS ?count)\n" +
			"WHERE {\n" +
			"  ?x ?a ?y .\n" +
			"  ?y ?b ?z .\n" +
			"  ?z ?c ?x .\n" +
			"  FILTER (STR(?y) > STR(?z)) .\n" +
			"  FILTER (STR(?z) > STR(?x))\n" +
			"}",
			"SELECT (COUNT(*) AS ?count)\n" +
			"WHERE {\n" +
			"  ?x ?a ?y .\n" +
			"  ?y ?b ?z .\n" +
			"  ?x ?c ?z\n" +
			"}"
	};
	private static final String QUERY_DROP_GRAPH = ""
			+ "DROP GRAPH <http://workingGraph>;";
	
	public TriangleCounter(RepositoryConnection connection, boolean isDirected, boolean isMultithreaded) throws OperationException {
		super(NAME, connection);
		this.isDirected = isDirected;
		this.isMultithreaded = isMultithreaded;
		if(isDirected && isMultithreaded) {
			workers = new Worker[3];
			counts = new int[3];
		}
	}
	
	@Override
	protected void init() {
		if(!this.isDirected)
			connection.prepareUpdate(QUERY_INIT_GRAPH).execute();
		else {
			if(this.isDirected && this.isMultithreaded) {
				Repository repository = connection.getRepository();
				for(int index = 0;index < workers.length;index++) {
					workers[index] = new Worker(repository.getConnection(), index);
					counts[index] = 0;
				}
			}
		}
	}
	
	private class Worker extends Thread {
		private RepositoryConnection connection;
		private int id;
		
		public Worker(RepositoryConnection connection, int id) {
			this.connection = connection;
			this.id = id;
		}
		
		public void run() {
			TupleQueryResult result = this.connection.prepareTupleQuery(QUERY_FOR_MULTITHREADS[id]).evaluate();
			
			if(result.hasNext()) {
				BindingSet binding = result.next();
				counts[id] = ((Literal)binding.getValue("count")).intValue();
			}
			else
				System.out.println("[Thread" + id + "]Cannot count triangles.");
		}
	}

	@Override
	protected void process() {
		if(!isDirected) {
			connection.prepareUpdate(QUERY_STORE_INITIAL_GRAPH).execute();
			TupleQueryResult result = connection.prepareTupleQuery(QUERY_FIND_TRIANGLES).evaluate();
			
			if(result.hasNext()) {
				BindingSet binding = result.next();
				int count = ((Literal)binding.getValue("count")).intValue();
				writer.writeLine("The number of triangles is " + count);
			}
			else
				System.out.println("Cannot count triangles.");
		}
		else {
			if(!isMultithreaded) {
				TupleQueryResult result = connection.prepareTupleQuery(QUERY_FIND_TRIANGLES_FOR_DIRECTED_GRAPH).evaluate();
				
				if(result.hasNext()) {
					BindingSet binding = result.next();
					int count = ((Literal)binding.getValue("count")).intValue();
					writer.writeLine("The number of triangles is " + count);
				}
				else
					System.out.println("Cannot count triangles.");
			}
			else {
				int count = 0;
				for(int index = 0;index < workers.length;index++) {
					workers[index].start();
				}
				for(int index = 0;index < workers.length;index++) {
					try {
						workers[index].join();
						count += counts[index];
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				writer.writeLine("The number of triangles is " + count);
			}
		}
	}

	@Override
	protected void end() {
		if(!this.isDirected)
			connection.prepareUpdate(QUERY_DROP_GRAPH).execute();
	}
}
