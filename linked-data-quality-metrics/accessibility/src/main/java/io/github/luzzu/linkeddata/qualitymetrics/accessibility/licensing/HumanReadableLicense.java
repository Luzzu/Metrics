package io.github.luzzu.linkeddata.qualitymetrics.accessibility.licensing;

import java.util.HashSet;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
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
 * Verifies whether a human-readable text, stating the of licensing model attributed to the resource, has been provided as part of the dataset.
 * In contrast with the Machine-readable Indication of a License metric, this one looks for objects containing literal values and 
 * analyzes the text searching for key, licensing related terms. Also, additional to the license related properties this metric examines comment 
 * properties such as rdfs:label, dcterms:description, rdfs:comment.
 */
public class HumanReadableLicense extends AbstractQualityMetric<Boolean> {
	
	private final Resource METRIC_URI = DQM.HumanReadableLicenseMetric;
	
	private static Logger logger = LoggerFactory.getLogger(HumanReadableLicense.class);
	
	/**
	 * Determines if an object contains a human-readable license
	 */
	private LicensingModelClassifier licenseClassifier = new LicensingModelClassifier();
	
	
    private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(DQM.HumanReadableLicenseMetric);	
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	
//	private double validLicenses = 0.0d;
//	private double totalPossibleLicenses = 0.0d;
	
	private boolean hasHumanReadableLicense = false;
	
	private static HashSet<String> setLicensingDocumProps;		
	static {		
 		setLicensingDocumProps = new HashSet<String>();		
		setLicensingDocumProps.add(DCTerms.description.getURI());		
		setLicensingDocumProps.add(RDFS.comment.getURI());	
		setLicensingDocumProps.add(RDFS.label.getURI());
		setLicensingDocumProps.add("http://schema.org/description");
		setLicensingDocumProps.add("http://www.w3.org/2004/02/skos/core#altLabel");
		setLicensingDocumProps.add(DCTerms.rights.getURI());
	}		

	
	/**
	 * Processes a single quad being part of the dataset. Detect triples containing as subject, the URI of the resource and as 
	 * predicate one of the license properties listed on the previous metric, or one of the documentation properties (rdfs:label, 
	 * dcterms:description, rdfs:comment) when found, evaluates the object contents to determine if it matches the features expected on 
	 * a licensing statement.
	 * @param quad Quad to be processed and examined to try to extract the text of the licensing statement
	 */
	public void compute(Quad quad) throws MetricProcessingException {
		logger.debug("Computing : {} ", quad.asTriple().toString());

		// Extract the predicate (property) of the statement, the described resource (subject) and the value set (object)
		Node subject = quad.getSubject();
		Node predicate = quad.getPredicate();
		Node object = quad.getObject();
		
		if (!(subject.isBlank())){
			if ((subject.getURI().equals(this.getDatasetURI()))) {
				if (object.isURI() && licenseClassifier.isLicensingPredicate(predicate)) {
					if ((licenseClassifier.isCopyLeftLicenseURI(object)) || (licenseClassifier.isNotRecommendedCopyLeftLicenseURI(object))) {
						this.hasHumanReadableLicense = true;
						
						if (licenseClassifier.isNotRecommendedCopyLeftLicenseURI(object)) {
							// add to problem report as DQMPROB.NotRecommendedLicenceInDataset
							if (requireProblemReport) {
								Quad q = new Quad(null, object, QPRO.exceptionDescription.asNode(), DQMPROB.NotRecommendedLicenseInDataset.asNode());
								((ProblemCollectionModel)problemCollection).addProblem(createProblemModel(q), ResourceCommons.asRDFNode(object).asResource());
							}
						}
					}
				} else if (object.isLiteral() && setLicensingDocumProps.contains(predicate.getURI())) {
					this.hasHumanReadableLicense = licenseClassifier.isLicenseStatement(object);
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

	

	public Resource getMetricURI() {
		return this.METRIC_URI;
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
	public Boolean metricValue() {
		return this.hasHumanReadableLicense;
	}

	@Override
	public ProblemCollection<?> getProblemCollection() {
		if (!this.hasHumanReadableLicense) {
			if (requireProblemReport) {
				Quad q = new Quad(null, ResourceCommons.toResource(this.getDatasetURI()).asNode(), QPRO.exceptionDescription.asNode(), DQMPROB.NoValidLicenseInDatasetForHumans.asNode());
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
