package edu.stanford.cs244b.mochi.testingframework;

import org.testng.Assert;

public class TestUtils {
    public static void assertIn(final String s, final String... options) {
        if (checkIn(s, options) == false) {
            Assert.fail(String.format("Could not find %s in %s", s, options));
        }
    }

    public static boolean checkIn(final String s, final String... options) {
        Assert.assertNotNull(s);
        Assert.assertNotNull(options);
        for (final String o : options) {
            if (s.equals(o)) {
                return true;
            }
        }
        return false;
    }

}
