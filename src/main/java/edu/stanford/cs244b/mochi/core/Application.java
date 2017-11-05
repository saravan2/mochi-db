package edu.stanford.cs244b.mochi.core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "edu.stanford.cs244b.mochi")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
