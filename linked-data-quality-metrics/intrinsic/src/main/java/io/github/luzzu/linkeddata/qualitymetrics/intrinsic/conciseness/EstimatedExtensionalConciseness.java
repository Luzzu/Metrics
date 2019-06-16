package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.conciseness;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractComplexQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualitymetrics.algorithms.RLBSBloomFilter;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Santiago Londono
 * Implementation of the Extensional Conciseness metric by means of the Randomized Load-balanced Biased Sampling
 * Bloom Filter, as proposed by Bera et al. [bera12]. This algorithm is designed to scale well for huge amounts 
 * of data, but providing approximate results
 */
public class EstimatedExtensionalConciseness extends AbstractComplexQualityMetric<Double> {
	
	private static Logger logger = LoggerFactory.getLogger(EstimatedExtensionalConciseness.class);
	
	private final Resource METRIC_URI = DQM.ExtensionalConcisenessMetric;
	
	/**
	 * Parameter: default size of the Bloom filters, determines the precision of the estimations
	 */
	private static int defaultFilterSize = 5500000;
	
	/**
	 * Parameter: number of Bloom filters to be created, determines the precision of the estimations
	 */
	private static int numFilters = 13;
	
	// Randomized Load-balanced Biased Sampling Bloom Filter, used to find duplicate instance declarations
	private RLBSBloomFilter rlbsBloomFilterDupls = null;
	
	// Subject URI of the instance being currently built
	private String currentSubjectURI = null;
	
	// Set of predicates-values of the instance being currently built
	private SortedSet<String> currentStatements = null;
	
	// Estimated number of duplicates
	private Long estimatedDuplInstances = new Long(0);
	private Long totalCreatedInstances = new Long(0);
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.ExtensionalConcisenessMetric);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	private Long totalTriples = 0l;
	
	@Override
	public void before(Object ... args) {
		// Bloom Filters are based on an array of bits, whose size must be defined at creation. Ideally this size should 
		// match the max. number of items to be put into the filter. If the caller doesn't provide that number, initialize
		// the filter to default size
		Integer approxNumTriples = defaultFilterSize;
		int k = numFilters;
		
		// Check whether the approximated number of triples has been provided
		if(args != null && args.length > 0 && args[0] != null && !(args[0] instanceof Integer )) {
			approxNumTriples = (Integer)args[0];
		}
		//TODO: EnvironmentProperties.getInstance().getEstimateNumberDataLines() 
		
		this.rlbsBloomFilterDupls = new RLBSBloomFilter(k, approxNumTriples, 0.01);
	}
	
	/**
	 * Re-computes the value of the Extensional Conciseness Metric, by considering a new quad provided.
	 * @param quad The new quad to be considered in the computation of the metric. Must be not null.
	 */
	public void compute(Quad quad) {
		totalTriples++;
		logger.debug("Assessing {}", quad.asTriple().toString());
		// Every time a new quad is considered, get the URI of the subject
		String subjectURI = (quad.getSubject().isURI()) ? quad.getSubject().getURI() : quad.getSubject().toString();
		String predicateURI = ((quad.getPredicate() != null)?(quad.getPredicate().toString()):(""));
		String objectValue = ((quad.getObject() != null)?(quad.getObject().toString()):(""));
		
		// Serializate statement
		String statement = predicateURI + " " + objectValue;
		
		// Check if the same object is being processed
		if(!subjectURI.equals(this.currentSubjectURI)) {
			
			// Check if subject just finished being created is in the filter
			if(this.currentStatements != null) {
				
				if(this.rlbsBloomFilterDupls.checkDuplicate(this.currentStatements.toString())) {
					// Bits didn't changed, it might be a false positive, but most likely it's duplicated
					this.estimatedDuplInstances++;
					logger.debug("Duplicate instance definition detected, subject URI: {}", this.currentSubjectURI);
					
					if (requireProblemReport) {
						Quad q = new Quad(null, ModelFactory.createDefaultModel().createResource(this.currentSubjectURI).asNode(), QPRO.exceptionDescription.asNode(), DQMPROB.ResourceReplica.asNode());
						problemCollection.addProblem(q);
					}
				}
			}
			
			// A new instance is to be created
			this.currentStatements = new TreeSet<String>();
			this.currentSubjectURI = subjectURI;
			this.totalCreatedInstances++;
			logger.debug("Creating new instance with subject: " + this.currentSubjectURI);
		}
		
		this.currentStatements.add(statement);
	}
	
	/**
	 * Returns the current value of the Extensional Conciseness Metric, computed as the ratio of the 
	 * Number of Unique Subjects to the Total Number of Subjects. 
	 * Subjects are the objects being described by the quads provided on invocations to the compute 
	 * method, each subject is identified by its URI (the value of the subject attribute of the quad). 
	 * Uniqueness of subjects is determined from its properties: one subject is said to be unique 
	 * if and only if there is no other subject equivalent to it.
	 * - Note that two equivalent subjects may have different ids (URIs).
	 * @return Current value of the Extensional Conciseness Metric: (No. of Unique Subjects / Total No. of Subjects)
	 */
	@Override
	public Double metricValue() {
		double metricValue = ((double)(this.totalCreatedInstances - this.estimatedDuplInstances))/((double)this.totalCreatedInstances);;
		statsLogger.debug("Datasets: {};Total Created Instances: {}, Estimated Duplicates: {}, Metric Value: {}",this.getDatasetURI(), this.totalCreatedInstances, this.estimatedDuplInstances, metricValue);
		return metricValue;
	}
	
	@Override
	public Resource getMetricURI() {
		return this.METRIC_URI;
	}
	
	@Override
	public void after(Object ... arg0) {
	}

	public static int getDefaultFilterSize() {
		return defaultFilterSize;
	}

	public static void setDefaultFilterSize(int defaultFilterSize) {
		EstimatedExtensionalConciseness.defaultFilterSize = defaultFilterSize;
	}

	public static int getNumFilters() {
		return numFilters;
	}

	public static void setNumFilters(int numFilters) {
		EstimatedExtensionalConciseness.numFilters = numFilters;
	}

	@Override
	public boolean isEstimate() {
		return true;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}

	@Override
	public ProblemCollection<?> getProblemCollection() {
		return this.problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.totalTriples));
		activity.add(mp, DAQ.totalDatasetTriplesAssessed, ResourceCommons.generateTypeLiteral(this.totalTriples));
		activity.add(mp, DAQ.estimationTechniqueUsed, ModelFactory.createDefaultModel().createResource("https://dblp.org/rec/journals/corr/abs-1212-3964"));

		
		Resource ep = ResourceCommons.generateURI();
		activity.add(mp, DAQ.estimationParameter, ep);
		activity.add(ep, RDF.type, DAQ.EstimationParameter);
		activity.add(ep, DAQ.estimationParameterValue, ResourceCommons.generateTypeLiteral(defaultFilterSize));
		activity.add(ep, DAQ.estimationParameterKey, ResourceCommons.generateTypeLiteral("M"));
		
		Resource ep2 = ResourceCommons.generateURI();
		activity.add(mp, DAQ.estimationParameter, ep2);
		activity.add(ep2, RDF.type, DAQ.EstimationParameter);
		activity.add(ep2, DAQ.estimationParameterValue, ResourceCommons.generateTypeLiteral(numFilters));
		activity.add(ep2, DAQ.estimationParameterKey, ResourceCommons.generateTypeLiteral("k"));

		
		return activity;
	}

}