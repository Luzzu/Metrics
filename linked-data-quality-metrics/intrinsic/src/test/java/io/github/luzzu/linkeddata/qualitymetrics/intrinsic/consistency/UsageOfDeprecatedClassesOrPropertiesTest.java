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
public class UsageOfDeprecatedClassesOrPropertiesTest extends Assert {
	protected TestLoader loader = new TestLoader();
	protected UsageOfDeprecatedClassesOrProperties metric;


	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/SampleInput_UsageOfDeprecatedClassesAndProperties_Minimal.ttl");
		metric = new UsageOfDeprecatedClassesOrProperties();
	}

	@After
	public void tearDown() throws Exception {
	}
	

	@Test
	public void testUsageOfDeprecatedClassesAndPropertiesMinimalExample() throws MetricProcessingException {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
		
//		totalTypes = 2; totalProperties = 5; deprecatedTypes = 1; deprecatedProperties = 1;
		// 2 / 7
		assertEquals(0.71428571428,metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/deprecated-classes-properties.ttl");
	}	
	
	
}
