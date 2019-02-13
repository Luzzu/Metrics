package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import java.util.concurrent.ConcurrentMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

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
 * This metric is based on the metric defined by Hogan et al.
 * Weaving the Pedantic Web. This metric checks if the assessed
 * dataset has a defined classed placed in the triple's predicate
 * and defined property in the object position. If an undefined
 * class or property is used, then it is ignored
 *  
 * Best Case : 1
 * Worst Case : 0
 * 
 * @author Jeremy Debattista
 */
public class MisplacedClassesOrProperties extends AbstractQualityMetric<Double> {

	private final Resource METRIC_URI = DQM.MisplacedClassesOrPropertiesMetric;
	private static Logger logger = LoggerFactory.getLogger(MisplacedClassesOrProperties.class);
	
    private ConcurrentMap<String, Boolean> seenProperties = new ConcurrentLinkedHashMap.Builder<String, Boolean>().maximumWeightedCapacity(100000).build();
    private ConcurrentMap<String, Boolean> seenClasses = new ConcurrentLinkedHashMap.Builder<String, Boolean>().maximumWeightedCapacity(100000).build();

	private long misplacedClassesCount = 0;
	private long totalClassesCount = 0;
	private long misplacedPropertiesCount = 0;
	private long totalPropertiesCount = 0;
	private long totalTriples = 0;
	
	
	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(METRIC_URI);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();
	
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing {}", quad.asTriple());
		totalTriples++;
		
		Node predicate = quad.getPredicate(); // retrieve predicate
		Node object = quad.getObject(); // retrieve object
		
		if ((predicate.getURI().equals(OWL.equivalentProperty.getURI())) ||
			(predicate.getURI().equals(OWL.inverseOf.getURI()))){
			// do nothing
		} else {
			//checking if classes are found in the property position
			logger.debug("Is the used predicate {} actually a class?", predicate.getURI());
			this.totalPropertiesCount++;
			if (seenProperties.containsKey(predicate.toString())){
				if (!(seenProperties.get(predicate.toString()))){
					this.misplacedPropertiesCount++;
					this.createProblemModel(quad.getSubject(), predicate, DQMPROB.MisplacedClass);
				}
			} else {
				if(VocabularyLoader.getInstance().checkTerm(predicate)){ //if the predicate does not exist, then do not count it as misplaced
					if ((VocabularyLoader.getInstance().isClass(predicate))){
						this.misplacedPropertiesCount++;
						this.createProblemModel(quad.getSubject(), predicate, DQMPROB.MisplacedClass);
						seenProperties.put(predicate.toString(), false);
					}
					seenProperties.put(predicate.toString(), true);
				}
			}
			
			//checking if properties are found in the object position
			if ((object.isURI()) && (predicate.getURI().equals(RDF.type.getURI()))){
				if (VocabularyLoader.getInstance().checkTerm(object)){
					logger.debug("Checking {} for misplaced class", object.getURI());
					this.totalClassesCount++;
					if (seenClasses.containsKey(object.toString())){
						if (!(seenClasses.get(object.toString()))){
							this.misplacedClassesCount++;
							this.createProblemModel(quad.getSubject(), object, DQMPROB.MisplacedProperty);
						}
					} else {
						if(VocabularyLoader.getInstance().checkTerm(object)){ //if the object does not exist, then do not count it as misplaced
							if (VocabularyLoader.getInstance().isProperty(object)){
								this.misplacedClassesCount++;
								this.createProblemModel(quad.getSubject(), object, DQMPROB.MisplacedProperty);
								seenClasses.put(object.toString(), false);
							}
							seenClasses.put(object.toString(), true);
						}
					}
				}
			}
		}
	}
	
	private void createProblemModel(Node resource, Node classOrProperty, Resource type){
		if (requireProblemReport) {
			Model m = ModelFactory.createDefaultModel();
			
			Resource subject = m.createResource(resource.toString());
			m.add(new StatementImpl(subject, QPRO.exceptionDescription, type));
			
			if (type.equals(DQMPROB.MisplacedClass))
				m.add(new StatementImpl(subject, DQMPROB.hasMisplacedClass, m.asRDFNode(classOrProperty)));		
			else
				m.add(new StatementImpl(subject, DQMPROB.hasMisplacedProperty, m.asRDFNode(classOrProperty)));		
			
//			this.problemCollection.addProblem(m);
			((ProblemCollectionModel)problemCollection).addProblem(m, subject);
		}
	}

	/**
	 * This method computes metric value for the object of this class.
	 * 
	 * @return (total number of undefined classes or properties) / (total number
	 *         of classes or properties)
	 */
	
	public Double metricValue() {

		double metricValue = 1.0;
		
		double misplaced =  (double) this.misplacedClassesCount + (double)  this.misplacedPropertiesCount;
		if (misplaced > 0.0) 
			metricValue = 1.0 - (misplaced / ((double) this.totalPropertiesCount + (double) this.totalClassesCount));
		
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
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(totalTriples));
		activity.add(mp, DQM.totalNumberOfProperties, ResourceCommons.generateTypeLiteral(this.totalPropertiesCount));
		activity.add(mp, DQM.totalNumberOfClasses, ResourceCommons.generateTypeLiteral(this.totalClassesCount));
		
		return activity;
	}	
	
}