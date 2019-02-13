/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.conciseness;

import java.io.IOException;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;

/*
 * SPARQL Query used to get all URIs
 * SELECT DISTINCT (STR(?uri) as ?label)  {
 * 		{ ?uri ?p ?o . } UNION
 * 		{ ?s ?p ?uri}
 * 		FILTER (isURI(?uri) && (REGEX(STR(?uri),"^http") || REGEX(STR(?uri),"^https")) && (?p != rdf:type))
 * }
 * 
 */

/**
 * @author Jeremy Debattista
 * 
 * Test for the Short URI Metric.
 * In the used dataset, there are 66 URIS
 * that do not adhere to the set rules
 * and a total of 534 unique URIs 
 */
public class ShortURIsTest extends Assert {

	TestLoader loader = new TestLoader();
	ShortURIs metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("testdumps/eis.ttl");
		metric = new ShortURIs();
	}
	
	@Test
	public void noBlankNodesTest() throws MetricProcessingException, IOException{
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.87640449, metric.metricValue(), 0.00001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/short-uri.ttl");
	}
}
