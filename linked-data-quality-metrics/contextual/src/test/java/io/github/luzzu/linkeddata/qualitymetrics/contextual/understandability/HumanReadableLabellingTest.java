package io.github.luzzu.linkeddata.qualitymetrics.contextual.understandability;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;

public class HumanReadableLabellingTest extends Assert {
	TestLoader loader = new TestLoader();
	HumanReadableLabelling metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("labels.ttl");
		metric = new HumanReadableLabelling();
	}
	
	@Test
	public void labellingTest(){
		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.66667, metric.metricValue(), 0.0001);
		
		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Contextual/human-readable-labelling.ttl");
	}
}
