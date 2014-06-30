package tests;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;
import utils.TextSplitterService;
import utils.TextSplitterService.SplitterCallback;
import utils.TextSplitterService.SplitterType;

public class CharacterGrouperTest extends TestCase {

	public final void testGroup() throws IOException {
		TextSplitterService cg = new TextSplitterService();
		cg.split(
				new StringReader("Hallo Welt. Hier ist ein Satz, der Satzzeichen enthält. Passt so, oder? \nWürde auch mit 42 laufen!"),
				new SplitterCallback() {
					public void newRow(SplitterType splitterType, String value) {
						System.out.printf("%s, \"%s\"\n", splitterType.name(), value);
					}
				});
		fail("Not yet implemented");
	}
}
