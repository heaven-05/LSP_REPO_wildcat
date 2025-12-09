package org.howard.edu.lsp.finale.question1;

import java.util.Random;

/**
 * Basic password generation algorithm that uses java.util.Random
 * and generates passwords containing digits only (0-9).
 * 
 * @author Final Exam
 * @version 1.0
 */
public class BasicAlgorithm implements PasswordAlgorithm {
    
    private static final String DIGITS = "0123456789";
    private final Random random;
    
    /**
     * Constructs a BasicAlgorithm instance with a new Random generator.
     */
    public BasicAlgorithm() {
        this.random = new Random();
    }
    
    /**
     * Generates a password containing only digits (0-9) of the specified length.
     * 
     * @param length the desired length of the password
     * @return a password string containing only digits
     */
    @Override
    public String generate(int length) {
        StringBuilder password = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(DIGITS.length());
            password.append(DIGITS.charAt(index));
        }
        return password.toString();
    }
}

