package org.howard.edu.lsp.finale.question1;

import java.util.Random;

/**
 * Letters-only password generation algorithm that generates passwords
 * containing only letters (A-Z, a-z).
 * 
 * @author Final Exam
 * @version 1.0
 */
public class LettersAlgorithm implements PasswordAlgorithm {
    
    private static final String LETTERS = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "abcdefghijklmnopqrstuvwxyz";
    
    private final Random random;
    
    /**
     * Constructs a LettersAlgorithm instance with a new Random generator.
     */
    public LettersAlgorithm() {
        this.random = new Random();
    }
    
    /**
     * Generates a password containing only letters (A-Z, a-z) of the specified length.
     * 
     * @param length the desired length of the password
     * @return a password string containing only letters
     */
    @Override
    public String generate(int length) {
        StringBuilder password = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(LETTERS.length());
            password.append(LETTERS.charAt(index));
        }
        return password.toString();
    }
}

