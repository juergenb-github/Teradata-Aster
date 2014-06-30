package tests;

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.commons.math3.stat.inference.ChiSquareTest;

/**
 * @author jb185040
 *
 */
public class ChiSquareTestTest extends TestCase {
	private ChiSquareTest chiSquareTest;
	double[] expected = new double[10];
	long[] observed = new long[expected.length];
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		chiSquareTest = new ChiSquareTest();
	}

	/**
	 * Test method for {@link org.apache.commons.math3.stat.inference.ChiSquareTest#ChiSquareTest()}.
	 */
	public final void testChiSquareTest() {
		// Two random samples
		for(int i = 0; i < expected.length; i++) expected[i] = Math.random();
		for(int i = 0; i < observed.length; i++) observed[i] = (int) (Math.random()*1000d);
		System.out.println(Arrays.toString(expected) + ", " + Arrays.toString(observed) + ": " + chiSquareTest.chiSquareTest(expected, observed));
		
		// Two times same sample
		for(int i = 0; i < expected.length; i++) expected[i] = Math.random();
		for(int i = 0; i < observed.length; i++) observed[i] = (int) (expected[i]*10000d);
		System.out.println(Arrays.toString(expected) + ", " + Arrays.toString(observed) + ": " + chiSquareTest.chiSquareTest(expected, observed));
		
		// Same sample with slight differences
		for(int i = 0; i < expected.length; i++) expected[i] = Math.random();
		for(int i = 0; i < observed.length; i++) observed[i] = (int) (expected[i]*10000d + Math.random()*100d - 50d);
		System.out.println(Arrays.toString(expected) + ", " + Arrays.toString(observed) + ": " + chiSquareTest.chiSquareTest(expected, observed));
		
		fail("Not yet implemented");
	}
}
