package edu.stanford.cs244b.mochi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MainController {
    final static Logger LOG = LoggerFactory.getLogger(MainController.class);

    @RequestMapping("/json")
    public ResponseEntity<?> index() {
        LOG.info("Inside Main Controller");
        final Object response = new SomeObject("assadasdsads");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private class SomeObject {
        final String val;

        public SomeObject(String val) {
            this.val = val;
        }

        public String getVal() {
            return val;
        }
    }
}
