package org.howard.edu.lsp.finale.question1;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * JUnit test suite for PasswordGeneratorService.
 * Tests singleton behavior, algorithm selection, and password generation
 * for all supported algorithms.
 * 
 * @author Final Exam
 * @version 1.0
 */
public class PasswordGeneratorServiceTest {

    private PasswordGeneratorService service;

    @BeforeEach
    public void setup() {
        service = PasswordGeneratorService.getInstance();
    }

    @Test
    public void checkInstanceNotNull() {
        // Verify that 'service' is not null
        assertNotNull(service, "Service instance should not be null");
    }

    @Test
    public void checkSingleInstanceBehavior() {
        PasswordGeneratorService second = PasswordGeneratorService.getInstance();
        // Verify that both 'service' (created in @BeforeEach) 
        // and 'second' refer to the EXACT same object in memory. This 
        // test must confirm true singleton behavior — not just that the 
        // two objects are equal, but they are the *same instance* returned by getInstance().
        assertSame(service, second, "Both instances should refer to the same object (singleton behavior)");
    }

    @Test
    public void generateWithoutSettingAlgorithmThrowsException() {
        PasswordGeneratorService s = PasswordGeneratorService.getInstance();
        // Reset algorithm to ensure clean state for this test
        s.resetAlgorithm();
        // Verify correct exception behavior - should throw IllegalStateException
        // when generatePassword is called before setAlgorithm
        assertThrows(IllegalStateException.class, () -> {
            s.generatePassword(10);
        }, "Should throw IllegalStateException when generating password without setting algorithm");
    }

    @Test
    public void basicAlgorithmGeneratesCorrectLengthAndDigitsOnly() {
        service.setAlgorithm("basic");
        String p = service.generatePassword(10);
        // Verify required behavior:
        // 1. Password has correct length
        assertEquals(10, p.length(), "Password should have length 10");
        // 2. Password contains only digits (0-9)
        assertTrue(p.matches("[0-9]+"), "Password should contain only digits (0-9)");
    }

    @Test
    public void enhancedAlgorithmGeneratesCorrectCharactersAndLength() {
        service.setAlgorithm("enhanced");
        String p = service.generatePassword(12);
        // Verify required behavior:
        // 1. Password has correct length
        assertEquals(12, p.length(), "Password should have length 12");
        // 2. Password contains only allowed characters (A-Z, a-z, 0-9)
        assertTrue(p.matches("[A-Za-z0-9]+"), "Password should contain only A-Z, a-z, and 0-9");
    }

    @Test
    public void lettersAlgorithmGeneratesLettersOnly() {
        service.setAlgorithm("letters");
        String p = service.generatePassword(8);
        // Verify required behavior:
        // 1. Password has correct length
        assertEquals(8, p.length(), "Password should have length 8");
        // 2. Password contains only letters (A-Z, a-z)
        assertTrue(p.matches("[A-Za-z]+"), "Password should contain only letters (A-Z, a-z)");
    }

    @Test
    public void switchingAlgorithmsChangesBehavior() {
        service.setAlgorithm("basic");
        String p1 = service.generatePassword(10);
        // Verify p1 contains only digits
        assertEquals(10, p1.length(), "First password should have length 10");
        assertTrue(p1.matches("[0-9]+"), "First password should contain only digits");

        service.setAlgorithm("letters");
        String p2 = service.generatePassword(10);
        // Verify p2 contains only letters
        assertEquals(10, p2.length(), "Second password should have length 10");
        assertTrue(p2.matches("[A-Za-z]+"), "Second password should contain only letters");

        service.setAlgorithm("enhanced");
        String p3 = service.generatePassword(10);
        // Verify p3 contains A-Z, a-z, 0-9
        assertEquals(10, p3.length(), "Third password should have length 10");
        assertTrue(p3.matches("[A-Za-z0-9]+"), "Third password should contain A-Z, a-z, and 0-9");
        
        // Verify that switching algorithms actually changes behavior
        // by verifying correct behavior characteristics of each algorithm
        // p1 should be digits only (basic algorithm)
        assertTrue(p1.matches("[0-9]+"), "First password (basic) should contain only digits");
        // p2 should be letters only (letters algorithm)
        assertTrue(p2.matches("[A-Za-z]+"), "Second password (letters) should contain only letters");
        // p3 should be enhanced (can contain A-Z, a-z, 0-9)
        assertTrue(p3.matches("[A-Za-z0-9]+"), "Third password (enhanced) should contain A-Z, a-z, and 0-9");
        // Verify that algorithms produce different character sets
        assertTrue(!p1.matches("[A-Za-z]+"), "Basic algorithm should not produce letters");
        assertTrue(!p2.matches("[0-9]+"), "Letters algorithm should not produce digits");
    }
}

