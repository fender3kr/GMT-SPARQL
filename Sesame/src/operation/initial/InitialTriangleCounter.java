package operation.initial;

import org.openrdf.model.Literal;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;

import exception.OperationException;
import operation.GraphOperation;

public class InitialTriangleCounter extends GraphOperation {
	private static final String NAME = "Initial Triangle Counting";
	
	private static final String QUERY_FIND_TRIANGLES = ""
			+ "SELECT (COUNT(DISTINCT *) AS ?count)\n"
			+ "WHERE {\n"
			+ "  {?x ?p ?y} UNION {?y ?p ?x} .\n"
            + "  {?y ?p ?z} UNION {?z ?p ?y} .\n"
            + "  {?z ?p ?x} UNION {?x ?p ?z} .\n"
            + "  FILTER(STR(?x) < STR(?y)) .\n"
            + "  FILTER(STR(?y) < STR(?z))\n"
            + "}";
	
	public InitialTriangleCounter(RepositoryConnection connection) throws OperationException {
		super(NAME, connection);
	}
	
	@Override
	protected void init() {
	}

	@Override
	protected void process() {
		TupleQueryResult result = connection.prepareTupleQuery(QUERY_FIND_TRIANGLES).evaluate();
		
		if(result.hasNext()) {
			BindingSet binding = result.next();
			int count = ((Literal)binding.getValue("count")).intValue();
			writer.writeLine("The number of triangles is " + count);
		}
		else
			writer.writeLine("Cannot count triangles.");
	}

	@Override
	protected void end() {
	}

}
