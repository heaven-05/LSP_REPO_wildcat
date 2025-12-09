package org.howard.edu.lsp.finale.question1;

import java.security.SecureRandom;

/**
 * Enhanced password generation algorithm that uses java.security.SecureRandom
 * and generates passwords containing uppercase letters (A-Z), lowercase letters (a-z),
 * and digits (0-9).
 * 
 * @author Final Exam
 * @version 1.0
 */
public class EnhancedAlgorithm implements PasswordAlgorithm {
    
    private static final String ALLOWED_CHARACTERS = 
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
        "abcdefghijklmnopqrstuvwxyz" +
        "0123456789";
    
    private final SecureRandom secureRandom;
    
    /**
     * Constructs an EnhancedAlgorithm instance with a new SecureRandom generator.
     */
    public EnhancedAlgorithm() {
        this.secureRandom = new SecureRandom();
    }
    
    /**
     * Generates a password containing uppercase letters, lowercase letters, and digits
     * of the specified length.
     * 
     * @param length the desired length of the password
     * @return a password string containing A-Z, a-z, and 0-9
     */
    @Override
    public String generate(int length) {
        StringBuilder password = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int index = secureRandom.nextInt(ALLOWED_CHARACTERS.length());
            password.append(ALLOWED_CHARACTERS.charAt(index));
        }
        return password.toString();
    }
}

