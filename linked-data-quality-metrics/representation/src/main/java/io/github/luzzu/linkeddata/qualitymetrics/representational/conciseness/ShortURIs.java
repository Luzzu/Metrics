package io.github.luzzu.linkeddata.qualitymetrics.representational.conciseness;


import java.util.concurrent.ConcurrentMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Santiago Londono
 * Detects long URIs or those that contains query parameters, thereby providing a 
 * measure of how compactly is information represented in the dataset
 * 
 * The W3C best practices for URIs say that a URI (including scheme) should be at max 80 characters
 * http://www.w3.org/TR/chips/#uri.
 * Parameterised URIs are considered bad immediately.
 * 
 * Value returns a ratio of the total number of short uris in relation to the
 * number of local URIs of a dataset
 *
 */
public class ShortURIs extends AbstractQualityMetric<Double> {
	
	private static Logger logger = LoggerFactory.getLogger(ShortURIs.class);
	
	private final Resource METRIC_URI = DQM.ShortURIsMetric;
	
    private ConcurrentMap<String, Boolean> seenSet = new ConcurrentLinkedHashMap.Builder<String, Boolean>().maximumWeightedCapacity(100000).build();
	
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	/**
	 * Sum short uri's
	 */
	private long shortURICount = 0;
	
	/**
	 * Counts the total number of possible dereferenceable URIs defined in the dataset
	 */
	private long countLocalDefURIs = 0;
	
	private int totalTriples = 0;
	

	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.ShortURIsMetric);
	
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing quad: " + quad.asTriple().toString());
		
		totalTriples++;

		if (!(quad.getPredicate().hasURI(RDF.type.getURI()))){
			Node subject = quad.getSubject();
			if ((!(subject.isBlank())) && (!(this.seenSet.containsKey(subject.getURI())))){
				if (subject.isURI()){
					if (possibleDereferenceableURI(subject.getURI())){
						countLocalDefURIs++;
						
						String uri = subject.getURI();
						if (uri.contains("?")){
							if (requireProblemReport) {
								Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.ParametarisedURI.asNode());
								problemCollection.addProblem(q);
							}
						} else if (uri.length() > 80){
							if (requireProblemReport) {
								Quad q = new Quad(null, subject, QPRO.exceptionDescription.asNode(), DQMPROB.LongURI.asNode());
								problemCollection.addProblem(q);
							}
						} else {
							shortURICount++;
						}
					}
				}
				this.seenSet.put(subject.getURI(),true);
			}
			
			Node object = quad.getObject();
			if (object.isURI()){
				if ((!(object.isBlank())) &&  (!(this.seenSet.containsKey(object.getURI())))){
					if (possibleDereferenceableURI(object.getURI())){
						countLocalDefURIs++;
						
						String uri = object.getURI();
						if (uri.contains("?")){
							if (requireProblemReport) {
								Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.ParametarisedURI.asNode());
								problemCollection.addProblem(q);
							}
						} else if (uri.length() > 80){
							if (requireProblemReport) {
								Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.LongURI.asNode());
								problemCollection.addProblem(q);
							}
						} else {
							shortURICount++;
						}
					}
				}
				this.seenSet.put(object.getURI(),true);
			}
		}
	}

	@Override
	public Double metricValue() {
		return ((double)shortURICount / (double)countLocalDefURIs);
	}

	
	public Resource getMetricURI() {
		return METRIC_URI;
	}
	

	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return 	DQM.LuzzuProvenanceAgent;
	}
	
	private boolean possibleDereferenceableURI(String uri){
		if (uri.startsWith("http") || uri.startsWith("https")) return true;
		return false;
	}
	

	@Override
	public ProblemCollection<Quad> getProblemCollection() {
		return this.problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.totalTriples));
		activity.add(mp, DQM.totalLocalURIs, ResourceCommons.generateTypeLiteral((int)this.countLocalDefURIs));
		
		return activity;
	}


}
