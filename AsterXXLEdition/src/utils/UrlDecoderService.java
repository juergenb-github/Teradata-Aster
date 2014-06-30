package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URLDecoder;

/**
 * @author jb185040
 *
 * Decode strings that were encoded with the application/x-www-form-urlencoded MIME format. 
 */
public class UrlDecoderService {
	/**
	 * @author jb185040
	 *
	 * Implement this interface to get notified whenever a string was decoded.
	 */
	public interface UrlDecoderCallback {
		/**
		 * Called whenever a string was decoded.
		 *  
		 * @param value of the decoded string.
		 */
		public void newRow(String value);
	}
	
	// Character encoding
	private String encode = null;
	
	/**
	 * @param encode to be used as character encoding. Should be "UTF-8".
	 */
	public UrlDecoderService(String encode) {this.encode = encode;}

	/**
	 * Do the grouping.
	 * 
	 * @param reader to read characters from.
	 * @param urlDecoderCallback to be called to inform about a new decoded string
	 * @throws IOException thrown when reading from reader failed.
	 */
	public void urlDecode(Reader reader, UrlDecoderCallback urlDecoderCallback) throws IOException {
		urlDecoderCallback.newRow(URLDecoder.decode(new BufferedReader(reader).readLine(), encode));
	}
}
