/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.consistency;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
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
import io.github.luzzu.qualityproblems.ProblemCollectionQuad;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Jeremy Debattista
 * 
 * This metric checks if a dataset makes use of Deprecated Classes or Properties.
 * A high usage of such classes or properties will give a low value (closer to 0),
 * whilst a low usage of such classes will give a high value (closer to 1).
 */
public class UsageOfDeprecatedClassesOrProperties extends AbstractQualityMetric<Double> {
	
	private final Resource METRIC_URI = DQM.UsageOfDeprecatedClassesOrProperties;

	private static Logger logger = LoggerFactory.getLogger(UsageOfDeprecatedClassesOrProperties.class);
	
	private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.UsageOfDeprecatedClassesOrProperties);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();


	private long totalTypes = 0;
	private long totalProperties = 0;
	private long deprecatedTypes = 0;
	private long deprecatedProperties = 0;
	private long counter = 0;
	private long triplesAssessed = 0;
	
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Assessing {}", quad.asTriple().toString());

		Node property = quad.getPredicate();
		Node object = quad.getObject();
		counter++;
		
		if (property.getURI().equals(RDF.type.getURI())){
			if (object.isURI()) {
				if (VocabularyLoader.getInstance().checkTerm(object)){
					triplesAssessed++;
					if (VocabularyLoader.getInstance().isDeprecatedTerm(object)) {
						deprecatedTypes++;
						if (requireProblemReport) {
							Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.DeprecatedClass.asNode());
							this.problemCollection.addProblem(q);
						}
					}
					totalTypes++;
				}
				totalProperties++;
			}
		} else {
			if (VocabularyLoader.getInstance().checkTerm(property)){
				triplesAssessed++;
				if (VocabularyLoader.getInstance().isDeprecatedTerm(property)) {
					deprecatedProperties++;
					if (requireProblemReport) {
						Quad q = new Quad(null, property, QPRO.exceptionDescription.asNode(), DQMPROB.DeprecatedProperty.asNode());
						this.problemCollection.addProblem(q);
					}
				}
			}
			totalProperties++;
		}
	}

	@Override
	public Double metricValue() {
		double value = 1 - (((double) deprecatedTypes + (double) deprecatedProperties) /  
				((double) totalTypes + (double) totalProperties));
		
		
		return value;
	}

	@Override
	public Resource getMetricURI() {
		return METRIC_URI;
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
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.counter));
		activity.add(mp, DAQ.totalDatasetTriplesAssessed, ResourceCommons.generateTypeLiteral(this.triplesAssessed));
		activity.add(mp, DQM.totalNumberOfProperties, ResourceCommons.generateTypeLiteral(this.totalProperties));
		activity.add(mp, DQM.totalNumberOfClasses, ResourceCommons.generateTypeLiteral(this.totalTypes));
		
		return activity;
	}

}
