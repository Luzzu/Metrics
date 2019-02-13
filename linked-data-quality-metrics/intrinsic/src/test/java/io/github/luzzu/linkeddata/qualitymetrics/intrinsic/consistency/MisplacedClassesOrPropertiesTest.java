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
public class MisplacedClassesOrPropertiesTest extends Assert{
	
	protected TestLoader loader = new TestLoader();
	protected MisplacedClassesOrProperties metric;
	
	@Before
	public void setUp() throws Exception {
		loader.loadDataSet("testdumps/Sample_MisplacedClassesAndProperties.ttl");
		metric = new MisplacedClassesOrProperties();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testEntitiesAsMembersOfDisjointClassesMinimalExample() throws MetricProcessingException {
		List<Quad> streamingQuads = loader.getStreamingQuads();
		
		for(Quad quad : streamingQuads){
			// here we start streaming triples to the quality metric
			metric.compute(quad);
		}
		
		// 1- (2 / 6)
		assertEquals(0.66666666666,metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Intrinsic/misplaced-classes-properties.ttl");
	}	
	
}
