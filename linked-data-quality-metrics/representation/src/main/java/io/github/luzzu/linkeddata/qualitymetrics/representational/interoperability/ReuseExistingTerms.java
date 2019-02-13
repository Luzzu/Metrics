/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.representational.interoperability;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

import io.github.luzzu.exceptions.AfterException;
import io.github.luzzu.exceptions.BeforeException;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractComplexQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.LOVInterface;
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
 * In this metric we assess if dataset reuse relevant terms 
 * for a particular domain. A dataset is deemed conformant to this metric
 * if it exhibits a higher overlap within the recommended vocabularies.
 * (Issue IX and X - Hogan et al. - An empirical survey of Linked Data Conformance)
 * 
 * This is a qualitative metric and thus require some extra input from
 * the user. This metric requires a trig file representing common vocabularies 
 * for a particular domain.
 * 
 * The value of this metric is the percentage of overlapping terms (i.e. the number
 * of overlapping terms / total number of terms)
 * 
 * For example:
 * <http://linkedgeodata.org> :hasDomain "Geographic" ;
 *  :hasVocabularies <http://www.w3.org/2003/01/geo/wgs84_pos#> , <http://www.geonames.org/ontology#> ;
 *  :getFromLOV "True" .
 */
public class ReuseExistingTerms extends AbstractComplexQualityMetric<Double> {

	private Model categories = ModelFactory.createDefaultModel();
	private Set<String> topVocabs = new HashSet<String>();
	{
		// added the top 10 mostly used vocabs from 
		// linkeddatacatalog.dws.informatik.uni-mannheim.de/state/
		// and prefix.cc
		topVocabs.add("http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		topVocabs.add("http://www.w3.org/2000/01/rdf-schema#");
		topVocabs.add("http://xmlns.com/foaf/0.1/");
		topVocabs.add("http://purl.org/dc/terms/");
		topVocabs.add("http://www.w3.org/2002/07/owl#");
		topVocabs.add("http://www.w3.org/2003/01/geo/wgs84_pos#");
		topVocabs.add("http://rdfs.org/sioc/ns#");
		topVocabs.add("http://www.w3.org/2004/02/skos/core#");
		topVocabs.add("http://rdfs.org/ns/void#");
		topVocabs.add("http://www.w3.org/ns/dcat#"); // added dcat as it is becoming popular
	}
	private ConcurrentMap<String, Double> suggestedVocabs = new ConcurrentHashMap<String, Double>();
	
	private static Logger logger = LoggerFactory.getLogger(ReuseExistingTerms.class);
	
    private ConcurrentMap<String, Boolean> seenSet = new ConcurrentLinkedHashMap.Builder<String, Boolean>().maximumWeightedCapacity(100000).build();
	
    private ProblemCollection<Quad> problemCollection = new ProblemCollectionQuad(DQM.ReuseExistingTermsMetric);

	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	private int overlapClasses = 0;
	private int overlapProperties = 0;
	
	private int totalClasses = 0;
	private int totalProperties = 0;
	
	private int tripleCount = 0;
	
	private Resource configFileLocation;
	
	@Override
	public void compute(Quad quad) throws MetricProcessingException {
		Node predicate = quad.getPredicate();
		
		tripleCount++;
		if (predicate.hasURI(RDF.type.toString())){
			Node object = quad.getObject();

			if (!(object.isBlank())){
				logger.debug("checking class: {}", object.getURI());

				if (!(this.seenSet.containsKey(object.getURI()))){
					this.totalClasses++;
					if ((suggestedVocabs.containsKey(object.getNameSpace())) || (topVocabs.contains(object.getNameSpace()))){
						
						Boolean defined = false;
						try {
							defined = VocabularyLoader.getInstance().checkTerm(object);
						} catch (Exception e) {
							throw new MetricProcessingException("[IO3 - Reuse Existing Terms] Exception thrown when checking term  " + object.getURI()+ ". Exception: "+e.getMessage());
						}
						
						if (defined){
							this.overlapClasses++;
							double newVal = (suggestedVocabs.get(object.getNameSpace()) != null) ? suggestedVocabs.get(object.getNameSpace()) + 1.0 : 1.0;
							suggestedVocabs.put(object.getNameSpace(), newVal);
						}
					}
					this.seenSet.put(object.getURI(),true);
				}
			}
		}
		
		if (!(this.seenSet.containsKey(predicate.getURI()))){
			this.totalProperties++;
			if ((suggestedVocabs.containsKey(predicate.getNameSpace())) || (topVocabs.contains(predicate.getNameSpace()))){
				// its a property from a suggested or top vocabulary
				logger.info("checking predicate: {}", predicate.getURI());
				
				Boolean defined = false;
				try {
					defined = VocabularyLoader.getInstance().checkTerm(predicate);
				} catch (Exception e) {
					throw new MetricProcessingException("[IO3 - Reuse Existing Terms] Exception thrown when checking term  " + predicate.getURI()+ ". Exception: "+e.getMessage());
				}
	
				if (defined){
					this.overlapProperties++;
					double newVal = (suggestedVocabs.get(predicate.getNameSpace()) != null) ? suggestedVocabs.get(predicate.getNameSpace()) + 1.0 : 1.0;
					suggestedVocabs.put(predicate.getNameSpace(), newVal);
				}
			}
			this.seenSet.put(predicate.getURI(),true);
		}
	}

	@Override
	public Double metricValue() {
		// calculating the overlapping terms of used suggested vocabularies
		double olt = ((double) this.overlapClasses + (double) this.overlapProperties) / ((double) this.totalClasses + (double) this.totalProperties) ;
	
		if (requireProblemReport) {
			for(String s : suggestedVocabs.keySet()){
				if (suggestedVocabs.get(s) == 0.0){
					Quad q = new Quad(null, ModelFactory.createDefaultModel().createResource(s).asNode(), QPRO.exceptionDescription.asNode(), DQMPROB.UnusedSuggestedVocabulary.asNode());
					this.problemCollection.addProblem(q);//.add(new SerialisableQuad(q));
				}
			}
		}
		
		return olt;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.ReuseExistingTermsMetric;
	}
	
	@Override
	public ProblemCollection<Quad> getProblemCollection() {
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

	/** 
	 * The before method in this method should accept only
	 * a string with the trig filename 
	 */
	@Override
	public void before(Object... args) throws BeforeException {
		if (args == null || args.length <= 0 || args[0] == null){
			logger.error("Argument in the Reuse Existing Vocabulary metric should not be null.");
			throw new BeforeException("Argument should not be a string with a filename.");
		}
		String fileName = (String) args[0];
		
		try{
			categories = RDFDataMgr.loadModel(fileName);
			this.configFileLocation = ModelFactory.createDefaultModel().createResource(fileName);
		} catch (RiotException e){
			logger.error("Error loading configuration file: {}", fileName);
			throw new BeforeException("Invalid configuration file passed to the Reuse Existing Vocabulary metric. The configuration file should be RDF " + 
					"(or any other RDF serialisation format), having this following format: <http://linkedgeodata.org> :hasDomain \"geographic\"^^xsd:string ; " + 
					" :hasVocabularies <http://www.w3.org/2003/01/geo/wgs84_pos#> , <http://www.geonames.org/ontology#> .  ");
		}
		
		//Get dataset/base URI 
		String baseURI = EnvironmentProperties.getInstance().getDatasetPLD();
		if (baseURI == null){
			//Try using the dataset URI
			baseURI = EnvironmentProperties.getInstance().getDatasetPLD();
			if (baseURI == null){
				logger.error("Unknown Dataset PLD");
				throw new BeforeException("Dataset PLD is not known. This should be set in Luzzu.");
			}
		}
		
		//Load Vocabularies
		logger.info("Getting vocabularies from user configuration file for {}",baseURI);
		List<RDFNode> lst = categories.listObjectsOfProperty(categories.createResource(baseURI), categories.createProperty(":hasVocabularies")).toList();
		logger.info("Found {} vocabularies", lst.size());
		for(RDFNode n : lst){
			logger.info("Loading {}", n.toString());
			VocabularyLoader.getInstance().loadVocabulary(n.toString());
			suggestedVocabs.put(n.toString(),0.0);
		}
		
		List<RDFNode> lov = categories.listObjectsOfProperty(categories.createResource(baseURI), categories.createProperty(":getFromLOV")).toList();
		boolean getFromLov = false;
		if (lov.size() > 0) getFromLov = lov.get(0).asLiteral().getBoolean();
		
		if (getFromLov){
			List<RDFNode> domainNode = categories.listObjectsOfProperty(categories.createResource(baseURI), categories.createProperty(":hasDomain")).toList();
			for(RDFNode n : domainNode){
				try {
					List<String> vocabs = LOVInterface.getKnownVocabsPerDomain(n.asLiteral().getString());
					for(String v : vocabs) suggestedVocabs.put(v, 0.0);
				} catch (IOException e) {
					logger.error("Could not load vocabularies from LOD. Error Message: {}", e.getMessage());
				}
			}
		}
	}

	@Override
	public void after(Object... args) throws AfterException { } //nothing to do

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(this.tripleCount));
		activity.add(mp, DQM.existingTermsConfigurationFile, this.configFileLocation);
		
		return activity;
	}
}
