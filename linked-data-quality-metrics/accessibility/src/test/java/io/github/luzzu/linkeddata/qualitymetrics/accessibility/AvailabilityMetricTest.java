package io.github.luzzu.linkeddata.qualitymetrics.accessibility;

import org.apache.jena.sparql.core.Quad;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.accessibility.availability.EstimatedDereferenceabilityByStratified;
import io.github.luzzu.linkeddata.qualitymetrics.accessibility.availability.EstimatedMisreportedContentTypeByStratified;
import io.github.luzzu.linkeddata.qualitymetrics.accessibility.performance.HighThroughput;
import io.github.luzzu.linkeddata.qualitymetrics.accessibility.performance.LowLatency;
import io.github.luzzu.linkeddata.qualitymetrics.commons.TestLoader;

public class AvailabilityMetricTest extends Assert {
	
	TestLoader loader = new TestLoader();
	QualityMetric<?> metric;
	
	@Before
	public void setUp(){
		loader.loadDataSet("/Users/jeremy/Desktop/Boole/test.nt.gz");
	}
	
	@Test
	public void estimatedDereferenceabilityByStratifiedTest() throws MetricProcessingException{
		metric = new EstimatedDereferenceabilityByStratified();

		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.98033898305, (double) metric.metricValue(), 0.00001);
		
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/no-prolix-test.ttl");
	}
	
	
	@Test
	public void estimatedMisreportedContentTypeByStratifiedTest() throws MetricProcessingException{
		metric = new EstimatedMisreportedContentTypeByStratified();

		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.98033898305, (double) metric.metricValue(), 0.00001);
		
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/no-prolix-test.ttl");
	}
	
	@Test
	public void highThroughputTest() throws MetricProcessingException{
		metric = new HighThroughput();

		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.98033898305, (double) metric.metricValue(), 0.00001);
		
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/no-prolix-test.ttl");
	}
	
	@Test
	public void lowLatencyTest() throws MetricProcessingException{
		metric = new LowLatency();

		for(Quad q : loader.getStreamingQuads()){
			metric.compute(q);
		}
		
		assertEquals(0.98033898305, (double) metric.metricValue(), 0.00001);
		
//		loader.outputProblemReport(metric.getProblemCollection(), "/Users/jeremy/Desktop/luzzu-quality-tests/Representational/no-prolix-test.ttl");
	}



}
