# ETL Pipeline Assignment (Assignment 2)

This project implements a simple Extract-Transform-Load (ETL) pipeline in Java that reads `data/products.csv`, applies required transformations, and writes `data/transformed_products.csv`.

## Project Structure
```
LargeScaleProgramming25/
├── src/
│   └── org/howard/edu/lsp/assignment2/ETLPipeline.java
├── data/
│   ├── products.csv
│   └── transformed_products.csv (generated)
└── README.md
```

## How to Run
Run from the project root so relative paths resolve correctly.

### Compile
```bash
javac -d out src/org/howard/edu/lsp/assignment2/ETLPipeline.java
```

### Run
```bash
java -cp out org.howard.edu.lsp.assignment2.ETLPipeline
```

- Input: `data/products.csv`
- Output: `data/transformed_products.csv`

## Transform Rules
- Name → UPPERCASE
- Electronics → apply 10% discount; round to 2 decimals (HALF_UP)
- If final price > 500.00 and original category is Electronics → Category becomes "Premium Electronics"
- PriceRange by final price:
  - 0.00–10.00 → Low
  - 10.01–100.00 → Medium
  - 100.01–500.00 → High
  - 500.01+ → Premium

## Error Handling
- Missing input file: prints message and exits without crashing.
- Empty input (only header): still writes header to `transformed_products.csv`.
- Summary printed: rows read, transformed, skipped; output path.

## Assumptions
- CSV fields contain no commas or quotes.
- First row is a header.
- Program is run from project root.

## AI Usage
- Summary: Used an AI assistant to draft the initial `ETLPipeline.java` structure and edge-case checks. I verified logic and adapted rounding and ordering per assignment.
- Prompt:
  - "Implement a simple ETL pipeline in Java that reads CSV, transforms per rules (uppercase name, electronics discount, recategorize, price range), writes output, handles missing/empty file, and prints a summary. No third-party libraries."
- Response:
  - Suggested using `BigDecimal` with `RoundingMode.HALF_UP`, performing transformations in specified order, and writing headers unconditionally.
- How used: Adopted the `BigDecimal` rounding approach and the method decomposition; verified against requirements.

## External Sources
- None.
