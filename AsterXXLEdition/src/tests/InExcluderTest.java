package tests;

import java.util.Arrays;
import java.util.EmptyStackException;

import junit.framework.TestCase;
import utils.InExcluder;


/**
 * @author jb185040
 *
 */
public class InExcluderTest extends TestCase {
	/**
	 * Test method for {@link xmlFastFilter.InExcluder#InExcluder(java.util.List, java.util.List)}.
	 */
	public final void testInExcluder() {
		// walk thru with all included
		walkThruTree(
				"walk thru with all included",
				new InExcluder(null, null, null, null),
				true, true, true, true, true, true, true, true, true, true, true, true, true, true, true);
		
		// walk thru with just B included
		walkThruTree(
				"walk thru with just B included",
				new InExcluder(Arrays.asList(new String[] {"B"}) , null, null, null),
				false, false, false, false, false, false, false, true, true, true, true, true, true, true, false);
		
		// walk thru with just B excluded
		walkThruTree(
				"walk thru with just B excluded",
				new InExcluder(null, Arrays.asList(new String[] {"B"}), null, null),
				true, true, true, true, true, true, true, false, false, false, false, false, false, false, true);

		// walk thru with A included, A2 excluded, B excluded and B1a included. Direct comparison
		walkThruTree(
				"walk thru with A included, A2 excluded, B excluded and B1a included. Direct comparison",
				new InExcluder(Arrays.asList(new String[] {"A", "B1a"}), Arrays.asList(new String[] {"B", "A2"}), null, null),
				false, true, true, true, false, true, false, false, false, true, false, false, false, false, false);

		// walk thru with just A included, A2 excluded, B excluded and B1a included. Regular expressions
		walkThruTree(
				"walk thru with just A included, A2 excluded, B excluded and B1a included. Regular expressions",
				new InExcluder(Arrays.asList(new String[] {"/A/", "/B/B1/B1a/"}), Arrays.asList(new String[] {"/B/", "/.*/A2/"}), null, null),
				false, true, true, true, false, true, false, false, false, true, false, false, false, false, false);

		// walk thru with all included. Skipped at A2
		walkThruTreeSkipTest(
				"walk thru with all included. Skipped at A2",
				new InExcluder(null, null, Arrays.asList(new String[] {"A2"}), null),
				false, false, false, false, true, false, false, false, false, false, false, false, false, false, false);
		
		// walk thru with all included and unbalanced
		InExcluder inExcluder = new InExcluder(null, null, null, null);
		walkThruTree(
				"walk thru with all included and unbalanced",
				inExcluder,
				true, true, true, true, true, true, true, true, true, true, true, true, true, true, true);
		
		try {
			inExcluder.exitNode();
		}
		catch(EmptyStackException e) {
			return; // Test OK
		}
		fail("walk thru with all included and unbalanced - should throw exception");
	}
	
	private void walkThruTree(String msg, InExcluder inExcluder,
			boolean i0,
			boolean i1,
			boolean i2,
			boolean i3,
			boolean i4,
			boolean i5,
			boolean i6,
			boolean i7,
			boolean i8,
			boolean i9,
			boolean i10,
			boolean i11,
			boolean i12,
			boolean i13,
			boolean i14) {
		
		assertEquals(msg, i0, inExcluder.isIncluded());
		inExcluder.enterNode("A");
		assertEquals(msg, i1, inExcluder.isIncluded());
		inExcluder.enterNode("A1");
		assertEquals(msg, i2, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i3, inExcluder.isIncluded());
		inExcluder.enterNode("A2");
		assertEquals(msg, i4, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i5, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i6, inExcluder.isIncluded());
		inExcluder.enterNode("B");
		assertEquals(msg, i7, inExcluder.isIncluded());
		inExcluder.enterNode("B1");
		assertEquals(msg, i8, inExcluder.isIncluded());
		inExcluder.enterNode("B1a");
		assertEquals(msg, i9, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i10, inExcluder.isIncluded());
		inExcluder.enterNode("B1b");
		assertEquals(msg, i11, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i12, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i13, inExcluder.isIncluded());
		inExcluder.exitNode();
		assertEquals(msg, i14, inExcluder.isIncluded());
	}

	private void walkThruTreeSkipTest(String msg, InExcluder inExcluder,
			boolean i0,
			boolean i1,
			boolean i2,
			boolean i3,
			boolean i4,
			boolean i5,
			boolean i6,
			boolean i7,
			boolean i8,
			boolean i9,
			boolean i10,
			boolean i11,
			boolean i12,
			boolean i13,
			boolean i14) {
		
		assertEquals(msg, i0, inExcluder.isSkipped());
		inExcluder.enterNode("A");
		assertEquals(msg, i1, inExcluder.isSkipped());
		inExcluder.enterNode("A1");
		assertEquals(msg, i2, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i3, inExcluder.isSkipped());
		inExcluder.enterNode("A2");
		assertEquals(msg, i4, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i5, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i6, inExcluder.isSkipped());
		inExcluder.enterNode("B");
		assertEquals(msg, i7, inExcluder.isSkipped());
		inExcluder.enterNode("B1");
		assertEquals(msg, i8, inExcluder.isSkipped());
		inExcluder.enterNode("B1a");
		assertEquals(msg, i9, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i10, inExcluder.isSkipped());
		inExcluder.enterNode("B1b");
		assertEquals(msg, i11, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i12, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i13, inExcluder.isSkipped());
		inExcluder.exitNode();
		assertEquals(msg, i14, inExcluder.isSkipped());
	}
}
