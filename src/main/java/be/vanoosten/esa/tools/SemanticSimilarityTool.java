package be.vanoosten.esa.tools;

import java.io.IOException;
import org.apache.lucene.queryparser.classic.ParseException;

/**
 * Calculates a numeric value for the semantic similarity between two texts.
 * @author Philip van Oosten
 */
public class SemanticSimilarityTool {

    Vectorizer vectorizer;
    
    public SemanticSimilarityTool(Vectorizer vectorizer) {
        this.vectorizer = vectorizer;
    }
    
    public float findSemanticSimilarity(String formerText, String latterText, Boolean show_concepts) throws ParseException, IOException{
        ConceptVector formerVector = vectorizer.vectorize(formerText);
        ConceptVector latterVector = vectorizer.vectorize(latterText);
        if(show_concepts){
            System.out.println("Showing concepts of \"" + formerText + "\"");
            formerVector.showConcepts();
            System.out.println("Showing concepts of \"" + latterText + "\"");
            latterVector.showConcepts();
        }

        return formerVector.dotProduct(latterVector);
    }
    
}
