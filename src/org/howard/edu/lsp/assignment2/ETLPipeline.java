package org.howard.edu.lsp.assignment2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple Extract-Transform-Load (ETL) pipeline for CSV products.
 *
 * Requirements summary:
 * - Read from data/products.csv (relative to project root)
 * - Transform:
 *   1) Uppercase Name
 *   2) 10% discount for Electronics, round HALF_UP to 2 decimals
 *   3) If final price > 500.00 and original category == Electronics, set category to "Premium Electronics"
 *   4) PriceRange from final price: Low/Medium/High/Premium (per spec)
 * - Write to data/transformed_products.csv with header
 * - Handle missing input file and empty input gracefully
 * - Print run summary: rows read, transformed, skipped; and output path
 *
 * Notes:
 * - No third-party libraries; basic parsing assuming no commas/quotes in fields.
 * - Run from project root so relative paths resolve.
 *
 * AI usage disclosure (example): This file may have been generated with assistance from an AI
 * coding assistant for structure and edge-case coverage. The final logic and code were reviewed
 * and adapted to meet assignment requirements.
 */
public class ETLPipeline {

    private static final String INPUT_FILE = "data/products.csv";
    private static final String OUTPUT_FILE = "data/transformed_products.csv";

    public static void main(String[] args) {
        Path inputPath = Paths.get(INPUT_FILE);
        Path outputPath = Paths.get(OUTPUT_FILE);

        if (!Files.exists(inputPath)) {
            System.err.println("ERROR: Input file not found: " + inputPath.toString());
            System.err.println("Ensure you run from the project root and that the data/ folder exists.");
            return;
        }

        try {
            ETLResult result = runPipeline(inputPath, outputPath);
            System.out.println("Summary:");
            System.out.println("- Rows read: " + result.rowsRead);
            System.out.println("- Rows transformed: " + result.rowsTransformed);
            System.out.println("- Rows skipped: " + result.rowsSkipped);
            System.out.println("- Output written to: " + outputPath.toString());
        } catch (IOException e) {
            System.err.println("ERROR during ETL: " + e.getMessage());
        }
    }

    private static ETLResult runPipeline(Path inputPath, Path outputPath) throws IOException {
        List<String[]> records = extractCsv(inputPath);
        // extractCsv always includes header as first element if present

        if (records.isEmpty()) {
            // No header, treat as empty; still write header to output
            writeOutputHeaderOnly(outputPath);
            return new ETLResult(0, 0, 0);
        }

        String[] header = records.get(0);
        List<String[]> dataRows = new ArrayList<>();
        for (int i = 1; i < records.size(); i++) {
            dataRows.add(records.get(i));
        }

        int rowsRead = dataRows.size();
        List<String[]> transformed = new ArrayList<>();
        int skipped = 0;
        for (String[] row : dataRows) {
            String[] out = transformRow(header, row);
            if (out == null) {
                skipped++;
            } else {
                transformed.add(out);
            }
        }

        loadCsv(outputPath, transformed);
        return new ETLResult(rowsRead, transformed.size(), skipped);
    }

    private static List<String[]> extractCsv(Path inputPath) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(inputPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Basic CSV split per spec: no commas/quotes inside fields
                String[] parts = line.split(",", -1);
                rows.add(trimAll(parts));
            }
        }
        return rows;
    }

    private static void loadCsv(Path outputPath, List<String[]> rows) throws IOException {
        // Ensure parent directory exists
        Path parent = outputPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8)) {
            // Always write header
            writer.write("ProductID,Name,Price,Category,PriceRange");
            writer.newLine();
            for (String[] row : rows) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        }
    }

    private static void writeOutputHeaderOnly(Path outputPath) throws IOException {
        loadCsv(outputPath, new ArrayList<String[]>());
    }

    private static String[] transformRow(String[] header, String[] row) {
        // Expecting columns: ProductID,Name,Price,Category
        if (row.length < 4) {
            return null; // malformed row -> skip
        }

        String productIdStr = row[0].trim();
        String name = row[1].trim();
        String priceStr = row[2].trim();
        String category = row[3].trim();

        // Uppercase name
        String upperName = name.toUpperCase();

        BigDecimal originalPrice;
        try {
            originalPrice = new BigDecimal(priceStr);
        } catch (NumberFormatException ex) {
            return null; // skip malformed price
        }

        String originalCategory = category;

        // Apply discount if Electronics
        BigDecimal finalPrice = originalPrice;
        if ("Electronics".equals(originalCategory)) {
            finalPrice = originalPrice.multiply(new BigDecimal("0.90"));
        }
        // Round HALF_UP to 2 decimals
        finalPrice = finalPrice.setScale(2, RoundingMode.HALF_UP);

        // Recategorize if post-discount price > 500 and original category Electronics
        String finalCategory = originalCategory;
        if ("Electronics".equals(originalCategory) && finalPrice.compareTo(new BigDecimal("500.00")) > 0) {
            finalCategory = "Premium Electronics";
        }

        // Compute price range from final price
        String priceRange = computePriceRange(finalPrice);

        return new String[] { productIdStr, upperName, finalPrice.toPlainString(), finalCategory, priceRange };
    }

    private static String[] trimAll(String[] parts) {
        String[] out = new String[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = parts[i] == null ? "" : parts[i].trim();
        }
        return out;
    }

    private static String computePriceRange(BigDecimal finalPrice) {
        // Ranges:
        // 0.00–10.00 → Low
        // 10.01–100.00 → Medium
        // 100.01–500.00 → High
        // 500.01 and above → Premium
        BigDecimal p = finalPrice;
        if (p.compareTo(new BigDecimal("0.00")) >= 0 && p.compareTo(new BigDecimal("10.00")) <= 0) {
            return "Low";
        } else if (p.compareTo(new BigDecimal("10.00")) > 0 && p.compareTo(new BigDecimal("100.00")) <= 0) {
            return "Medium";
        } else if (p.compareTo(new BigDecimal("100.00")) > 0 && p.compareTo(new BigDecimal("500.00")) <= 0) {
            return "High";
        } else {
            return "Premium";
        }
    }

    private static class ETLResult {
        final int rowsRead;
        final int rowsTransformed;
        final int rowsSkipped;

        ETLResult(int rowsRead, int rowsTransformed, int rowsSkipped) {
            this.rowsRead = rowsRead;
            this.rowsTransformed = rowsTransformed;
            this.rowsSkipped = rowsSkipped;
        }
    }
}


