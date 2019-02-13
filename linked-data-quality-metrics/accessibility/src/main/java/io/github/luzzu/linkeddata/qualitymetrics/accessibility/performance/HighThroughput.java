package io.github.luzzu.linkeddata.qualitymetrics.accessibility.performance;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.HTTPRetriever;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.qualitymetrics.algorithms.ReservoirSampler;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;


/**
 * @author Jeremy Debattista
 * Estimates the efficiency with which a system can bind to the dataset, by measuring the number of 
 * answered HTTP requests responded by the source of the dataset, per second.
 */
public class HighThroughput extends AbstractQualityMetric<Double> {
	
	private final Resource METRIC_URI = DQM.HighThroughputMetric;
	
	private static Logger logger = LoggerFactory.getLogger(HighThroughput.class);
	
	/**
	 * Amount of HTTP requests that will be sent to the data source in order to estimate how many requests are served per second. 
	 */
	private static final int NUM_HTTP_REQUESTS = 5;
	
	/**
	 * Number of requests per second that ideally, should be served by a data source. In other words, its the amount of served requests 
	 * per second above of which a resource will get a perfect score of 1.0. 
	 */
	private static final double NORM_SERVED_REQS_PER_MILLISEC = 0.0020;
	
	/**
	 * Holds the total delay as currently calculated by the compute method
	 */
	private long totalDelay = -1;
	
	/**
	 * Holds the metric value
	 */
	private Double metricValue = null;
	
	

	/**
	 * Processes a single quad making part of the dataset. Firstly, tries to figure out the URI/PLD of the dataset where from the quads were obtained. 
	 * A burst HTTP requests is sent to the dataset's URI and the number of requests sent is divided by the total time required to serve them,  
	 * thus obtaining the estimated number of requests server per second
	 * @param quad Quad to be processed and examined to try to extract the dataset's URI
	 */
	
	ReservoirSampler<String> resSamp = new ReservoirSampler<String>(10,true);

	public void compute(Quad quad) {
		if (quad.getSubject().isURI()){
			if (this.getDatasetURI().equals("")){
				if (!(quad.getSubject().getURI().startsWith("file"))) 
					resSamp.add(quad.getSubject().getURI());
			}
			else if (quad.getSubject().getURI().startsWith(this.getDatasetURI())) 
				resSamp.add(quad.getSubject().getURI());
		}
	}

	/**
	 * Returns the current value of the High Throughput Metric as a ranking in the range [0, 1], with 1.0 the top ranking. 
	 * First estimates the number of served requests per second, computed as the ration between the total number of requests 
	 * sent to the dataset's endpoint and the sum of their response times. Then this estimate is normalized by dividing it 
	 * by NORM_SERVED_REQS_PER_SEC, the ideal amount of requests a resource is expected to serve per second, to get a raking of 1.0
	 * @return Current value of the High Throughput metric, measured with respect to the dataset's URI
	 */
	@Override
	public Double metricValue() {
		if (this.metricValue == null){
			for(String s : resSamp.getItems()){
				totalDelay += HTTPRetriever.measureReqsBurstDelay(s, NUM_HTTP_REQUESTS);
				logger.trace("Total delay for dataset {} was {}", s, totalDelay);	
			}

			double servedReqsPerMilliSec = ((double)NUM_HTTP_REQUESTS)/((double)totalDelay / (double)resSamp.getItems().size());
			this.metricValue = Math.min(1.0, Math.max(0, servedReqsPerMilliSec / NORM_SERVED_REQS_PER_MILLISEC));
		}	
				
		return this.metricValue;
	}

	public Resource getMetricURI() {
		return METRIC_URI;
	}

	@Override
	public boolean isEstimate() {
		return true;
	}

	@Override
	public Resource getAgentURI() {
		return 	DQM.LuzzuProvenanceAgent;
	}


	@Override
	public ProblemCollection<?> getProblemCollection() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		
		//TODO: Information on the methods used to measure
		return activity;
	}
	
}
