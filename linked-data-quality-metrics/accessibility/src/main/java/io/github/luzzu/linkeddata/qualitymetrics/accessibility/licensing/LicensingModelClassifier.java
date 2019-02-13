package io.github.luzzu.linkeddata.qualitymetrics.accessibility.licensing;

import java.util.HashSet;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.DC;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;

/**
 * @author Santiago Londono
 * 
 * Encapsulates knowledge about how to determine if text representing an URI or text attribute, provides information 
 * about the licensing model of a resource. For example, this class recognizes all the URIs of the predicates that 
 * can be used to specify the license under which a resource is attributed. 
 */
public class LicensingModelClassifier {
	
	/**
	 * Set of all the URIs of properties known to provide licensing information
	 */
	private static HashSet<String> setLicenseProperties;
	
	/**
	 * Set of all the URIs of classes known to provide licensing information
	 */
	private static HashSet<String> setClassProperties;
	
	
	/**
	 * Set of all the URIs of properties known to provide licensing information
	 */
	private static Pattern[] arrCopyLeftURIPatterns;
	
	/**
	 * Set of all the URIs of properties known to provide licensing information but not recommended to use
	 * according to the voID vocabulary
	 */
	private static Pattern[] arrNotRecommendedCopyLeftURIPatterns;
	
	/**
	 * Regular expressions represeting the patterns of the text deemed to be a licensing statement
	 */
	private static Pattern[] arrLicenseTextPatterns;
	
	static {
		// Initialize set of properties known to provide licensing information
		// For licencing properties we use the 10 top properties identified by Hogan et al. in An Empirical Survey of Linked Data conformance
		setLicenseProperties = new HashSet<String>();
		setLicenseProperties.add(DCTerms.license.getURI()); //dct:license
		setLicenseProperties.add(DCTerms.rights.getURI()); //dct:rights
		setLicenseProperties.add(DC.rights.getURI()); //dc:rights
		setLicenseProperties.add("http://www.w3.org/1999/xhtml/vocab#license"); //xhtml:license
		setLicenseProperties.add("http://creativecommons.org/ns#license"); //cc:license
		setLicenseProperties.add("http://purl.org/dc/elements/1.1/licence"); //dc:licence
		setLicenseProperties.add("http://dbpedia.org/ontology/licence"); //dbo:licence
		setLicenseProperties.add("http://dbpedia.org/property/licence"); //dbp:licence
		setLicenseProperties.add("http://usefulinc.com/ns/doap#license"); //doap:license
		setLicenseProperties.add("https://schema.org/license"); //schema:license
		setLicenseProperties.add("http://schema.theodi.org/odrs#dataLicense"); // odi data license 		
		
		
		setClassProperties = new HashSet<String>();
		setClassProperties.add(DCTerms.LicenseDocument.getURI());
		setClassProperties.add("http://creativecommons.org/ns#License");
		setClassProperties.add("http://schema.theodi.org/odrs#License"); 	
		setClassProperties.add("http://purl.oclc.org/NET/ldr/ns#License"); 

		// Initialize set of regex patterns corresponding to CopyLeft license URIs
		arrCopyLeftURIPatterns = new Pattern[15];
		arrCopyLeftURIPatterns[0] = Pattern.compile("^http://www\\.opendatacommons\\.org/licenses/odbl.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[1] = Pattern.compile("^http://www\\.opendatacommons\\.org/licenses/pddl/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[2] = Pattern.compile("^http://www\\.opendatacommons\\.org/licenses/by/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[3] = Pattern.compile("^http://creativecommons\\.org/publicdomain/zero/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[4] = Pattern.compile("^http://creativecommons\\.org/licenses/by/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[5] = Pattern.compile("^http://purl\\.org/NET/rdflicense/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[6] = Pattern.compile("^http://www\\.gnu\\.org/licenses/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[7] = Pattern.compile("^http://opendatacommons\\.org/licenses/odbl.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[8] = Pattern.compile("^http://opendatacommons\\.org/licenses/pddl/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[9] = Pattern.compile("^http://opendatacommons\\.org/licenses/by/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[10] = Pattern.compile("^http://gnu\\.org/licenses/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[11] = Pattern.compile("^http://www\\.creativecommons\\.org/publicdomain/zero/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[12] = Pattern.compile("^http://www\\.creativecommons\\.org/licenses/by/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[13] = Pattern.compile("^http://www\\.purl\\.org/NET/rdflicense/.*", Pattern.CASE_INSENSITIVE);
		arrCopyLeftURIPatterns[14] = Pattern.compile("^http://creativecommons\\.org/publicdomain/.*", Pattern.CASE_INSENSITIVE);

		arrNotRecommendedCopyLeftURIPatterns = new Pattern[3];
		arrNotRecommendedCopyLeftURIPatterns[0] = Pattern.compile("^http://creativecommons\\.org/licenses/by-sa/.*", Pattern.CASE_INSENSITIVE);
		arrNotRecommendedCopyLeftURIPatterns[1] = Pattern.compile("^http://www\\.gnu\\.org/copyleft/.*", Pattern.CASE_INSENSITIVE);
		arrNotRecommendedCopyLeftURIPatterns[2] = Pattern.compile("^http://creativecommons\\.org/licenses/by-nc/.*", Pattern.CASE_INSENSITIVE);

		
		// Initialize the licensing text pattern
		arrLicenseTextPatterns = new Pattern[1];
		arrLicenseTextPatterns[0] = Pattern.compile(".*(licensed?|copyrighte?d?).*(under|grante?d?|rights?).*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
	}

	/**
	 * Tells whether the predicate provided as parameter is known to provide information about how a resource
	 * is licensed. More specifically, the URI of the predicate is verified to belong to a set of predicate URIs 
	 * recognized as standard means to state the licensing schema of the resource being described.
	 * @param predicate Predicate to be evaluated to correspond to a statement of license
	 * @return true if the predicate is known to state the license of a resource, false otherwise
	 */
	public boolean isLicensingPredicate(Node predicate) {

		if(predicate != null && predicate.isURI()) {
			// Search for the predicate's URI in the set of license properties...
			if(setLicenseProperties.contains(predicate.getURI())) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Tells whether the object provided as parameter contains an URI, recognized as a CopyLeft license
	 * @param licenseObj Object of a triple expected to provide information about the license of the described resource
	 * @return true if the license is deemed as CopyLeft, false otherwise
	 */
	public boolean isCopyLeftLicenseURI(Node licenseObj) {
		
		if(licenseObj != null && licenseObj.isURI()) {
			// Compare the license URI with all the licenses known to be CopyLeft
			return matchesAnyPattern(licenseObj.getURI(), arrCopyLeftURIPatterns);
		}
		return false;
	}
	
	/**
	 * Tells whether the object provided as parameter contains an URI, recognized as a CopyLeft license that is not recommended
	 * @param licenseObj Object of a triple expected to provide information about the license of the described resource
	 * @return true if the license is deemed as CopyLeft, false otherwise
	 */
	public boolean isNotRecommendedCopyLeftLicenseURI(Node licenseObj) {
		
		if(licenseObj != null && licenseObj.isURI()) {
			// Compare the license URI with all the licenses known to be CopyLeft
			return matchesAnyPattern(licenseObj.getURI(), arrNotRecommendedCopyLeftURIPatterns);
		}
		return false;
	}
	
	
	/**
	 * Evaluates the text contained into the literal to determine whether it contains a licensing statement.
	 * @param licenseLiteralObj Text literal corresponding to the object of a triple
	 * @return true if the literal contains text considered to be of a license statement, false otherwise
	 */
	public boolean isLicenseStatement(Node licenseLiteralObj) {
		
		if(licenseLiteralObj != null && licenseLiteralObj.isLiteral()) {
			// Check whether the contents of the object match any of the license patterns
			return matchesAnyPattern(licenseLiteralObj.toString(), arrLicenseTextPatterns);
		}
		return false;
	}
	
	/**
	 * Evaluates the text contained into the literal to determine whether it contains a licensing statement
	 * that is not recommended.
	 * @param licenseLiteralObj Text literal corresponding to the object of a triple
	 * @return true if the literal contains text considered to be of a license statement, false otherwise
	 */
	public boolean isNotRecommendedLicenseStatement(Node licenseLiteralObj) {
		
		if(licenseLiteralObj != null && licenseLiteralObj.isLiteral()) {
			// Check whether the contents of the object match any of the license patterns
			return matchesAnyPattern(licenseLiteralObj.toString(), arrNotRecommendedCopyLeftURIPatterns);
		}
		return false;
	}
	
	/**
	 * Matches the text against all the patterns provided in the second argument, 
	 * to determine if the text matches any of them.
	 * @param text Text to be matched 
	 * @return true if the text matches any pattern in arrPatterns, false otherwise
	 */
	private boolean matchesAnyPattern(String text, Pattern[] arrPatterns) {
		
		Matcher matcher = null;
		
		for(Pattern pattern : arrPatterns) {
			
			matcher = pattern.matcher(text);
			
			if(matcher.matches()) {
				return true;
			}

		}
		return false;
	}
	
	
	public boolean containsMachineReadableLicense(Model m) {
		Optional<RDFNode> hasClass = m.listObjectsOfProperty(RDF.type).filterKeep(o -> setClassProperties.contains(o.asNode().getURI())).nextOptional();
		
		return !(hasClass == null);
	}
}
