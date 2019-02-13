/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.List;

import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;
import io.github.luzzu.operations.properties.PropertyManager;


/**
 * @author Jeremy Debattista
 * 
 */
public class OntologyHijackingTest extends Assert{

	protected TestLoader loader = new TestLoader();
	protected OntologyHijacking metric;

	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/SampleInput_OntologyHijacking_Minimal.ttl");
		PropertyManager.getInstance().addToEnvironmentVars("dataset-pld", "http://example.org/data/");
		PropertyManager.getInstance().addToEnvironmentVars("dataset-location", "http://example.org/data/");
//		metric.setDatasetURI("http://example.org/data/");
		metric = new OntologyHijacking();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testOntologyHijacking() throws MetricProcessingException {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
		
		//Total Possible Hijacks = 5
		//Total Hijacks = 2
		
		// 1 - (2 / 5)
		assertEquals(0.6,metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/ontology-hijack.ttl");
	}	

}
