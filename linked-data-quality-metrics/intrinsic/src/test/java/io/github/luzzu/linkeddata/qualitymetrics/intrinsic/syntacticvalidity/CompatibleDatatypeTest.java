/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.syntacticvalidity;

import java.util.List;

import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;


/**
 * @author Jeremy Debattista
 * 
 */
public class CompatibleDatatypeTest extends Assert {

	protected TestLoader loader = new TestLoader();
	protected CompatibleDatatype metric;

	
	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/SampleInput_CompatibleDatatype.ttl");
		metric = new CompatibleDatatype();
	}
	
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * a minimal example for entities being members of disjoint classes: one entity is (of foaf:Person and foaf:Document), one isn't.
	 * 
	 * Note that the FOAF vocabulary has been published as LOD, and that foaf:Person is explicitly declared disjoint with foaf:Document.
	 * @throws MetricProcessingException 
	 */
	@Test
	public void testCompatibleDatatypeMinimalExample() throws MetricProcessingException {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
		
		// incorrect 3
		// correct 11
		// 11 / 14
		assertEquals(0.8125,metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/compatible-datatype.ttl");
	}	
	

}
