package edu.stanford.cs244b.mochi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MainController {
    final static Logger LOG = LoggerFactory.getLogger(MainController.class);

    @RequestMapping("/json")
    public String index() {
        LOG.info("Inside Main Controller");
        return "Greetings from Spring Boot!";
    }
}
