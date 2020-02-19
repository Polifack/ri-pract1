package es.udc.fi.ri.pract1;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class ProcessIndex {

	public static void main(String[] args) {
		Analyzer analyzer = new StandardAnalyzer();
        
        System.out.println("Hello world from ProcessIndex");

	}

}
