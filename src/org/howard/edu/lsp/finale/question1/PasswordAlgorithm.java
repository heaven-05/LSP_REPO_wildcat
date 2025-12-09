package org.howard.edu.lsp.finale.question1;

/**
 * Interface for password generation algorithms.
 * This interface is part of the Strategy design pattern, allowing
 * different password generation strategies to be implemented and swapped
 * at runtime.
 * 
 * @author Final Exam
 * @version 1.0
 */
public interface PasswordAlgorithm {
    
    /**
     * Generates a password of the specified length using the algorithm's
     * specific character set and random number generation approach.
     * 
     * @param length the desired length of the password
     * @return a password string of the specified length
     */
    String generate(int length);
}

