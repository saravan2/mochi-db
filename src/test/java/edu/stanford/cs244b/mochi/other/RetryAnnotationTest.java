package edu.stanford.cs244b.mochi.other;

import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import com.jcabi.aspects.RetryOnFailure;

public class RetryAnnotationTest {
    private final AtomicInteger counter = new AtomicInteger(0);

    @RetryOnFailure(attempts = 4, delay = 0)
    private void methodThatFailsFirstTime() {
        int c = counter.getAndIncrement();
        if (c <= 1) {
            throw new IllegalStateException("well too bad it's 0 or 1, but you can retry");
        }
    }

    @Test
    public void testRetryAnnotation() {
        methodThatFailsFirstTime();
    }
}
