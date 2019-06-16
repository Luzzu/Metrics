package io.github.luzzu.linkeddata.qualitymetrics.commons;

public class Utils {

	public static String extractDatasetNS(String targetURL) {
		if (targetURL == null) {
			return null;
		}
		
		if ((targetURL.startsWith("http")) || (targetURL.startsWith("https"))){
			try {
				if (targetURL.contains("#")) return targetURL.substring(0, targetURL.indexOf("#"));
				else return targetURL.substring(0, targetURL.lastIndexOf("/"));
			} catch (StringIndexOutOfBoundsException ex) {
				return targetURL;
			}
		} else
			return targetURL;
	}
	
	
	public static String removeProtocol(String URL) {
		if (URL == null) {
			return null;
		}
		
		if (URL.contains("://")) {
			return URL.substring(URL.indexOf("://")+3);
		} else
			return URL;
	}
}
