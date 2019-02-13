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


/**
 * @author Jeremy Debattista
 * 
 */
public class EstimatedUsageOfIncorrectDomainOrRangeDatatypesTest extends Assert {
	protected TestLoader loader = new TestLoader();
	protected EstimatedUsageOfIncorrectDomainOrRangeDatatypes metric;


	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/SampleInput_UsageOfIncorrectDomainOrRangeDatatypes_Minimal.ttl");
//		loader.loadDataSet("/Users/jeremy/Downloads/lobid.org.nt");
		metric = new EstimatedUsageOfIncorrectDomainOrRangeDatatypes();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testUsageOfIncorrectClassesAndPropertiesMinimalExample() throws MetricProcessingException {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
//		 # Incorrect Domains : 1; # Incorrect Ranges : 1; # Predicates Assessed : 5; # Undereferenceable Predicate : 0
		
		// 2 / 10
		assertEquals(0.8,metric.metricValue(), 0.0001);
//		metric.metricValue();
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/est-incorrect-domain-range-dt.ttl");
	}	
}
