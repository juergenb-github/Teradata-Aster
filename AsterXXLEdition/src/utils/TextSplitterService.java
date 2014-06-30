package utils;

import java.io.IOException;
import java.io.Reader;

/**
 * @author jb185040
 *
 * Group characters of same type (as defined in Unicode) together. 
 */
public class TextSplitterService {
	/**
	 * @author jb185040
	 *
	 * Implement this interface to get notified whenever the Lexer found a new token.
	 */
	public interface SplitterCallback {
		/**
		 * Called whenever a new group of characters was found is configured to be emitted.
		 *  
		 * @param splitterType that named the regular expression.
		 * @param value of the matching string.
		 */
		public void newRow(SplitterType splitterType, String value);
	}
	
	/**
	 * @author jb185040
	 *
	 * Grouping for characters.
	 */
	public enum SplitterType {WHITESPACE, LETTER_DIGIT, OTHER}

	/**
	 * Do the grouping.
	 * 
	 * @param reader to read characters from.
	 * @param splitterCallback to be called to inform about a new group of characters
	 * @throws IOException thrown when reading from reader failed.
	 */
	public void split(Reader reader, SplitterCallback splitterCallback) throws IOException {
		SplitterType prevType = SplitterType.OTHER;
		StringBuilder value = null;
		
		for(int c = reader.read(); c != -1L; c = reader.read()) {
			SplitterType currentType = SplitterType.OTHER;
			if(Character.isWhitespace(c))
				currentType = SplitterType.WHITESPACE;
			else if(Character.isLetterOrDigit(c))
				currentType = SplitterType.LETTER_DIGIT;
			
			// new group detected
			if(value == null || currentType != prevType) {
				value = sendValue(splitterCallback, prevType, value);
				prevType = currentType;
			}
			
			value.append((char) c);
		}
		
		// The last group
		if(value != null) sendValue(splitterCallback, prevType, value);
	}
	
	// Inform callback and reset value
	private StringBuilder sendValue(SplitterCallback splitterCallback, SplitterType splitterType, StringBuilder value) {
		if(value != null) {
			splitterCallback.newRow(splitterType, value.toString());
			value.setLength(0);
		}
		else
			value = new StringBuilder();
		return value;
	}
}
