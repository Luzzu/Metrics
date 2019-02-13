/**
 * 
 */
package io.github.luzzu.linkeddata.qualitymetrics.intrinsic.syntacticvalidity;

import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.LangDetectException;

import au.com.bytecode.opencsv.CSVReader;
import io.github.luzzu.exceptions.AfterException;
import io.github.luzzu.exceptions.BeforeException;
import io.github.luzzu.linkeddata.qualitymetrics.commons.AbstractComplexQualityMetric;
import io.github.luzzu.linkeddata.qualitymetrics.commons.datatypes.Pair;
import io.github.luzzu.linkeddata.qualitymetrics.intrinsic.syntacticvalidity.helper.LanguageDetector;
import io.github.luzzu.linkeddata.qualitymetrics.intrinsic.syntacticvalidity.helper.LanguageDetectorSM;
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
 */
public class CorrectLanguageTag extends AbstractComplexQualityMetric<Double> {
	
	private static Logger logger = LoggerFactory.getLogger(CorrectLanguageTag.class);
	
	private String lexvoDataURI = "http://www.lexvo.org/data/term/{language}/{term}";
	private String lexvoResourceURI = "http://lexvo.org/id/term/{language}/{term}";
//	private String languageTranslatorURI = "https://services.open.xerox.com/bus/op/LanguageIdentifier/GetLanguageForString";
	

	private double languageThresholdConfidence = 0.90;
	private double languageThresholdConfidenceSM = 0.70;

	
	
	private Map<String,String> langMap = new HashMap<String, String>();
	
	private ProblemCollection<Model> problemCollection = new ProblemCollectionModel(DQM.CorrectLanguageTag);
	private boolean requireProblemReport = EnvironmentProperties.getInstance().requiresQualityProblemReport();

	private int totalvalidLangStrings = 0;
	private int totalCorrectStrings = 0;
	
	@Override
	public void compute(Quad quad) {
		Node obj = quad.getObject();
		
		if (obj.isLiteral()){
			RDFNode n = ResourceCommons.asRDFNode(obj);
			Literal lt = (Literal) n;
			String language = lt.getLanguage();
			
			if (!language.equals("")){
				totalvalidLangStrings++;
				try {
					Pair<Boolean,String> lTag = this.correctLanguageTag(obj);
					if (lTag.getFirstElement())
						totalCorrectStrings++;
					else {
						if (requireProblemReport) {
							Model m = ModelFactory.createDefaultModel();
							Resource pUri = ResourceCommons.generateURI();
							m.add(pUri, QPRO.exceptionDescription, DQMPROB.IncorrectLanguageTag);
							m.add(pUri, DQMPROB.actualLiteralValue, lt);
							m.add(pUri, DQMPROB.actualLanguageTag, ResourceCommons.generateTypeLiteral(language));
							m.add(pUri, DQMPROB.expectedLanguageTag, ResourceCommons.generateTypeLiteral(lTag.getSecondElement()));
							
//							problemCollection.addProblem(m);
							((ProblemCollectionModel)problemCollection).addProblem(m, pUri);
						}
						
					}
				} catch (UnsupportedEncodingException | LangDetectException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public Double metricValue() {
		double metricValue = (double) totalCorrectStrings / (double) totalvalidLangStrings;
		
		statsLogger.info("Correct Language Tag. Dataset: {} - Total # Correct Strings : {}; # Total Valid Language Strings : {}, Metric Value: {}", 
				this.getDatasetURI(), totalCorrectStrings, totalvalidLangStrings,metricValue);

		return metricValue;
	}

	@Override
	public Resource getMetricURI() {
		return DQM.CorrectLanguageTag;
	}


	@Override
	public boolean isEstimate() {
		return true;
	}

	@Override
	public Resource getAgentURI() {
		return DQM.LuzzuProvenanceAgent;
	}
	
	
	private Pair<Boolean, String> correctLanguageTag(Node lit_obj) throws UnsupportedEncodingException, LangDetectException{
		Pair<Boolean, String> result = new Pair<Boolean, String>(false, "unknown");
		RDFNode n = ResourceCommons.asRDFNode(lit_obj);
		Literal lt = (Literal) n;
		logger.info("Checking for {} :"+lt.toString());
		String stringValue = lt.getLexicalForm().trim();
		String language = lt.getLanguage();
		
		
		if (!language.equals("")){
			String[] splited = stringValue.split("\\b+"); 
			
			if (splited.length > 2){
				//its a sentence
				String lang = "";
				if (splited.length > 15) {
					lang = LanguageDetector.getInstance().identifyLanguageOfLabel(stringValue, languageThresholdConfidence).replace("\"","");
				} else {
					lang = LanguageDetectorSM.getInstance().identifyLanguageOfLabel(stringValue, languageThresholdConfidenceSM).replace("\"","");
				}
				
				
				if (lang.equals("")){
					// we cannot identify the languag
					return result;
				} else {
//					String shortLang = language.length() > 2 ? language.substring(language.length() - 2) : language;
					result.setFirstElement(language.equals(lang));
					result.setSecondElement(lang);
					return result;
				}
			} else {
				//its one word
				String shortLang = language.length() > 2 ? language.substring(language.length() - 2) : language;
				String lexvoLang = "";
				if (langMap.containsKey(shortLang)) lexvoLang = langMap.get(shortLang).substring(langMap.get(shortLang).length() - 3);
				
				if (!(lexvoLang.equals(""))){
					String data = this.lexvoDataURI.replace("{language}", lexvoLang).replace("{term}", URLEncoder.encode(stringValue, "UTF-8"));
					String uri = this.lexvoResourceURI.replace("{language}", lexvoLang).replace("{term}", URLEncoder.encode(stringValue, "UTF-8"));
					
					Model m = RDFDataMgr.loadModel(data);
					boolean exists = m.contains(m.createResource(uri), RDFS.seeAlso);
					
					result.setFirstElement(exists);
					return result;
				}
			}
		}
		return result;
	}
	
	
	@Override
	public void before(Object... args) throws BeforeException {
		String filename = CorrectLanguageTag.class.getClassLoader().getResource("lexvo/language_mapping.tsv").getFile();
		try {
			logger.info("Loading language file");
			CSVReader reader = new CSVReader(new FileReader(filename),'\t');
			List<String[]> allLanguages = reader.readAll();
			for(String[] language : allLanguages)
				langMap.put(language[0], language[1]);
			reader.close();
		} catch (IOException e) {
			logger.error("Error Loading language file: {}", e.toString());
			e.printStackTrace();
		}
		
	}

	@Override
	public void after(Object... args) throws AfterException {
		// Nothing to do here
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
		activity.add(mp, DQM.totalNumberOfLiteralsWithLanguageTag, ResourceCommons.generateTypeLiteral(this.totalvalidLangStrings));
		activity.add(mp, DQM.externalToolUsed, ResourceCommons.generateTypeLiteral(this.totalvalidLangStrings));

		Resource ep = ResourceCommons.generateURI();
		activity.add(mp, DAQ.estimationParameter, ep);
		activity.add(ep, RDF.type, DAQ.EstimationParameter);
		activity.add(ep, DAQ.estimationParameterValue, ResourceCommons.generateTypeLiteral(languageThresholdConfidence));
		activity.add(ep, DAQ.estimationParameterKey, ResourceCommons.generateTypeLiteral("Confidence Threshold Large Text"));
		
		Resource ep2 = ResourceCommons.generateURI();
		activity.add(mp, DAQ.estimationParameter, ep2);
		activity.add(ep2, RDF.type, DAQ.EstimationParameter);
		activity.add(ep2, DAQ.estimationParameterValue, ResourceCommons.generateTypeLiteral(languageThresholdConfidenceSM));
		activity.add(ep2, DAQ.estimationParameterKey, ResourceCommons.generateTypeLiteral("Confidence Threshold Short Text"));
		
		
		return activity;	
	}
}
