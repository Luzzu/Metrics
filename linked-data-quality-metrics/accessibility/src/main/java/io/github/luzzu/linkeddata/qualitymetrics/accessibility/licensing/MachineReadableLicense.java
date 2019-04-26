package io.github.luzzu.linkeddata.qualitymetrics.accessibility.licensing;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractQualityMetric;
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
 * Verifies whether consumers of the dataset are explicitly granted permission to re-use it, under defined 
 * conditions, by annotating the resource with a machine-readable indication (e.g. a VoID description) of the license.
 *  
 */
public class MachineReadableLicense extends AbstractQualityMetric<Boolean> {
	
	private final Resource METRIC_URI = DQM.MachineReadableLicenseMetric;
	
	private static Logger logger = LoggerFactory.getLogger(MachineReadableLicense.class);
	
    private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(DQM.MachineReadableLicenseMetric);	
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
	/**
	 * Allows to determine if a predicate states what is the licensing schema of a resource
	 */
	private LicensingModelClassifier licenseClassifier = new LicensingModelClassifier();
	
	
	private boolean hasValidMachineReadableLicense = false;
	
	
	/**
	 * Processes a single quad being part of the dataset. Firstly, tries to figure out the URI of the dataset whence the quads come. 
	 * If so, the URI is extracted from the corresponding subject and stored to be used in the calculation of the metric. Otherwise, verifies 
	 * whether the quad contains licensing information (by checking if the property is part of those known to be about licensing) and if so, stores 
	 * the URL of the subject in the map of resources confirmed to have licensing information
	 * @param quad Quad to be processed and examined to try to extract the dataset's URI
	 */
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Computing : {} ", quad.asTriple().toString());

		// Extract the predicate (property) of the statement, the described resource (subject) and the value set (object)
		Node subject = quad.getSubject();
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		
		
		if (!(subject.isBlank())) {
			if ((subject.getURI().equals(this.getDatasetURI()))
				&& (licenseClassifier.isLicensingPredicate(predicate))) {
				
				if (object.isURI()) {
					if ((licenseClassifier.isCopyLeftLicenseURI(object)) || (licenseClassifier.isNotRecommendedCopyLeftLicenseURI(object))) {
						// We have a license and we have to check if it is machine readable
						try{
							Model licenseModel = RDFDataMgr.loadModel(object.getURI());
							if (licenseClassifier.containsMachineReadableLicense(licenseModel)) this.hasValidMachineReadableLicense = true;
							else {
								// add to problem report as DQMPROB.NotMachineReadableLicense
								if (requireProblemReport) {
									Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.NotMachineReadableLicense.asNode());
									((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.asRDFNode(object).asResource());
								}
							}
						} catch (Exception e) {
							// add to problem report as DQMPROB.NotMachineReadableLicense
							if (requireProblemReport) {
								Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.NotMachineReadableLicense.asNode());
								((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.asRDFNode(object).asResource());
							}
						}
						if (licenseClassifier.isNotRecommendedCopyLeftLicenseURI(object)) {
							if (requireProblemReport) {
								Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.NotRecommendedLicenseInDataset.asNode());
								((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.asRDFNode(object).asResource());
							}
						}
					}
				}
			}
		}
		
	}
	
	private Model createProblemModel(Quad q) {
		Statement s = new StatementImpl(ResourceCommons.asRDFNode(q.getSubject()).asResource(),
				ModelFactory.createDefaultModel().createProperty(q.getPredicate().getURI()),
				ResourceCommons.asRDFNode(q.getObject()));
		
		return ModelFactory.createDefaultModel().add(s);
	}

		
	@Override
	public Boolean metricValue() {
		return this.hasValidMachineReadableLicense;
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


	@Override
	public ProblemCollection<?> getProblemCollection() {
		if (!this.hasValidMachineReadableLicense) {
			if (requireProblemReport) {
				Quad q = new Quad(null, ResourceCommons.toResource(this.getDatasetURI()).asNode(), QPRO.exceptionDescription.asNode(), DQMPROB.NoValidLicenseInDataset.asNode());
				((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.toResource(this.getDatasetURI()));
			}
		}
		
		return problemCollection;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		
		return activity;
	}

}
