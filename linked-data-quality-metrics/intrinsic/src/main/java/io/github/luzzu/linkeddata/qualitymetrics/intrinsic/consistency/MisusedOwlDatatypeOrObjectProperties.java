package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.VocabularyLoader;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQM;
import io.github.luzzu.linkeddata.qualitymetrics.vocabulary.DQMPROB;
import io.github.luzzu.operations.properties.EnvironmentProperties;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.qualityproblems.ProblemCollectionModel;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;


/**
 * 
 * This metric is based on the metric defined by Hogan et al.
 * Weaving the Pedantic Web. Detect properties that are defined as a owl:datatype 
 * property but is used as object property and properties defined as a owl:object 
 * property and used as datatype property The metric is computed as a ratio of 
 * misused properties against the whole dataset.
 * 
 * @author Jeremy Debattista
 * 
 */
public class MisusedOwlDatatypeOrObjectProperties extends AbstractQualityMetric<Double> {

	private final Resource METRIC_URI = DQM.MisusedOwlDatatypeOrObjectPropertiesMetric;

	private static Logger logger = LoggerFactory.getLogger(MisusedOwlDatatypeOrObjectProperties.class);
	
		
	private long misuseDatatypeProperties = 0;
	private long misuseObjectProperties = 0;
	private long validPredicates = 0;
	private long totalTriples = 0;

	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(METRIC_URI);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	/**
	 * This method computes identified a given quad is a misuse owl data type
	 * property or object property.
	 * 
	 * @param quad - to be identified
	 */
	
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing {}", quad.asTriple());

		Node subject = quad.getSubject();
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();

		this.totalTriples++;
		
		if (VocabularyLoader.getInstance().checkTerm(predicate)){
			this.validPredicates++;
			
			if (object.isLiteral()){
				// predicate should not be an owl:ObjectProperty
				if (VocabularyLoader.getInstance().isObjectProperty(predicate)){
					this.misuseObjectProperties++;
					this.createProblemModel(subject, predicate, DQMPROB.MisusedObjectProperty);
				}
			} else if(object.isURI() || object.isBlank()){
				// predicate should not be an owl:DataProperty
				if (VocabularyLoader.getInstance().isDatatypeProperty(predicate)){
					this.misuseDatatypeProperties++;
					this.createProblemModel(subject, predicate, DQMPROB.MisusedDatatypeProperty);
				}
			}
		}
	}

	
	
	private void createProblemModel(Node resource, Node property, Resource type){
		if (requireProblemReport) {
			Model m = ModelFactory.createDefaultModel();
			
			Resource subject = m.createResource(resource.toString());
			m.add(new StatementImpl(subject, QPRO.exceptionDescription, type));
			
			if (type.equals(DQMPROB.MisusedDatatypeProperty))
				m.add(new StatementImpl(subject, DQMPROB.hasMisusedDatatypeProperty, m.asRDFNode(property)));		
			else
				m.add(new StatementImpl(subject, DQMPROB.hasMisusedObjectProperty, m.asRDFNode(property)));
			
//			problemCollection.addProblem(m);
			((ProblemCollectionModel)problemCollection).addProblem(m, subject);

		}
	}
	
	
	/**
	 * This method computes metric value for the object of this class
	 * 
	 * @return (total misuse properties) / (total properties)
	 */
	
	public Double metricValue() {
		
		double metricValue = 1.0;
		
		double misused = this.misuseDatatypeProperties + this.misuseObjectProperties;
		if (misused > 0.0) 
			metricValue = 1.0 - (misused / this.validPredicates);
		
		return metricValue;
	}

	/**
	 * Returns Metric URI
	 * 
	 * @return metric URI
	 */
	public Resource getMetricURI() {
		return this.METRIC_URI;
	}

	@Override
	public boolean isEstimate() {
		return false;
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
		activity.add(mp, DQM.totalValidDereferenceableTerms, ResourceCommons.generateTypeLiteral(this.validPredicates));
		
		return activity;
	}
}
