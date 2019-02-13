package io.github.luzzu.linkeddata.qualitymetrics.representational.interpretability;

import java.util.concurrent.ConcurrentMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
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
 * @author Jeremy Debattista
 * 
 * This metric measures the number of undefined classes and
 * properties used by a data publisher in the assessed dataset.
 * By undefined classes and properties we mean that such resources
 * are used without any formal definition (e.g. using foaf:image 
 * instead of foaf:img).
 * 
 */
public class UndefinedClassesAndProperties extends AbstractQualityMetric<Double> {

	private int undefinedClasses = 0;
	private int undefinedProperties = 0;
	private int totalClasses = 0;
	private int totalProperties = 0;
	
	private static Logger logger = LoggerFactory.getLogger(UndefinedClassesAndProperties.class);
    private ConcurrentMap<String, Boolean> seenSet = new ConcurrentLinkedHashMap.Builder<String, Boolean>().maximumWeightedCapacity(100000).build();

    private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(DQM.UndefinedClassesAndPropertiesMetric);
	
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

    private int tripleCount = 0;
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing quad: " + quad.asTriple().toString());

		Node predicate = quad.getPredicate();
		this.tripleCount++;
		
		if (predicate.hasURI(RDF.type.getURI())){
			// Checking for classes
			Node object = quad.getObject();
			this.totalClasses++;

			if ((!(object.isLiteral())) && (!(object.isBlank())) &&  (!(this.seenSet.containsKey(object.getURI())))){
				logger.debug("checking class: " + object.getURI());

				if (!(object.isBlank())){
					Boolean defined = false;
					try {
						Boolean checkTerm = false;
						try {
							checkTerm = VocabularyLoader.getInstance().checkTerm(predicate);
						} catch (Exception e) {
							logger.error("Vocabulary Exception when accessing term {}.\nException Thrown:\n {}",predicate.getURI(), e.getMessage());
							throw new MetricProcessingException("[IN3 - Undefined Classes and Properties] Exception thrown when checking term  " + predicate.getURI()+ ". Skipping but not halting assessment. Exception logged.");
						}
						if (checkTerm){
							defined =  VocabularyLoader.getInstance().isClass(object);
						}					
					} catch (Exception e) {
						logger.error("Vocabulary Exception when accessing term {}.\nException Thrown:\n {}",object.getURI(), e.getMessage());
						throw new MetricProcessingException("[IN3 - Undefined Classes and Properties] Exception thrown when checking term  " + object.getURI()+ ". Skipping but not halting assessment. Exception logged.");
					}
					
					if (!defined){
						this.undefinedClasses++;
						if (requireProblemReport) {
							Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.UndefinedClass.asNode());
//							problemCollection.addProblem(createProblemModel(q));
							((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.asRDFNode(object).asResource());

						}
					}
					this.seenSet.put(object.getURI(),defined);
				}
			} else {
				if ((!(object.isLiteral())) && (!(object.isBlank()))){
					this.undefinedClasses = (!this.seenSet.get(object.getURI())) ? this.undefinedClasses + 1 : this.undefinedClasses;
				}
			}
		} else {
			logger.debug("checking predicate: " + predicate.getURI());
			this.totalProperties++;

			if (!(this.seenSet.containsKey(predicate.getURI()))){

				// Checking for properties
				if (!(this.isContainerPredicate(predicate))){
					Boolean defined = false;
					try {
						Boolean checkTerm = false;
						try {
							checkTerm = VocabularyLoader.getInstance().checkTerm(predicate);
						} catch (Exception e) {
							logger.error("Vocabulary Exception when accessing term {}.\nException Thrown:\n {}",predicate.getURI(), e.getMessage());
							throw new MetricProcessingException("[IN3 - Undefined Classes and Properties] Exception thrown when checking term  " + predicate.getURI()+ ". Skipping but not halting assessment. Exception logged.");
						}
						if (checkTerm){
							defined = VocabularyLoader.getInstance().isProperty(predicate);
						}					
					} catch (Exception e) {
						logger.error("Vocabulary Exception when accessing term {}.\nException Thrown:\n {}",predicate.getURI(), e.getMessage());
						throw new MetricProcessingException("[IN3 - Undefined Classes and Properties] Exception thrown when checking term  " + predicate.getURI()+ ". Skipping but not halting assessment. Exception logged.");
					}
					
					if (!defined){
						this.undefinedProperties++;
						if (requireProblemReport) {
							Quad q = new Quad(null, predicate, QPRO.exceptionDescription.asNode(), DQMPROB.UndefinedProperty.asNode());
//							problemCollection.addProblem(createProblemModel(q));
							((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.asRDFNode(predicate).asResource());

						}
					}
					this.seenSet.put(predicate.getURI(),defined);
				}
			} else {
				this.undefinedProperties = (!this.seenSet.get(predicate.getURI())) ? this.undefinedProperties + 1 : this.undefinedProperties;
			}
		}
	}
	
	private boolean isContainerPredicate(Node predicate){
		if (predicate.getURI().matches(RDF.getURI()+"_[0-9]+")){
			return true;
		}
		return false;
	}

	@Override
	public Double metricValue() {
		return (this.undefinedClasses + this.undefinedProperties == 0) ? 1.0 
				: 1.0 - ((double)(this.undefinedClasses + this.undefinedProperties)/(double)(this.totalClasses + this.totalProperties));
	}

	@Override
	public Resource getMetricURI() {
		return DQM.UndefinedClassesAndPropertiesMetric;
	}
	
	@Override
	public ProblemCollection<Model> getProblemCollection() {
		return this.problemCollection;
	}


	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return 	DQM.LuzzuProvenanceAgent;
	}
	
	private Model createProblemModel(Quad q) {
		Statement s = new StatementImpl(ResourceCommons.asRDFNode(q.getSubject()).asResource(),
				ModelFactory.createDefaultModel().createProperty(q.getPredicate().getURI()),
				ResourceCommons.asRDFNode(q.getObject()));
		
		return ModelFactory.createDefaultModel().add(s);
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.tripleCount));
		activity.add(mp, DQM.totalNumberOfClasses, ResourceCommons.generateTypeLiteral(this.totalClasses));
		activity.add(mp, DQM.totalNumberOfProperties, ResourceCommons.generateTypeLiteral(this.totalProperties));
		
		return activity; 
	}
}
