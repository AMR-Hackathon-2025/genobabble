# LLM QC (Genome Assembly Quality Control)

A Python package for processing and analyzing genome assembly quality control data, providing tools for merging multiple QC metrics, species identification, and balanced dataset sampling.

## Overview

LLM QC is designed to help bioinformaticians and researchers analyze and process genome assembly quality control data. The package provides utilities for:

- Merging different types of QC metrics (CheckM2, Sylph, etc.) into comprehensive datasets
- Adding species information to assembly statistics
- Creating balanced datasets for analysis or machine learning by sampling high-quality and low-quality assemblies
- Standardized file handling and data processing for genomic assembly data

## Installation

### Requirements

- Python 3.8 or later
- pandas
- numpy

### Install from source

```bash
# Clone the repository
git clone https://github.com/yourusername/llm_qc.git
cd llm_qc

# Install the package
pip install -e .

# For development, install with development dependencies
pip install -e ".[dev]"
```

## Directory Structure

```
llm_qc/
├── src/
│   └── llm_qc/
│       ├── __init__.py
│       ├── core/
│       │   ├── __init__.py
│       │   └── merge.py        # Core functionality for merging QC data
│       ├── processing/
│       │   ├── __init__.py
│       │   ├── species.py      # Species data processing
│       │   └── sampling.py     # Dataset sampling functionality
│       └── utils/
│           ├── __init__.py
│           └── file_handling.py # Common file operations
├── data/
│   ├── raw/                    # Original input files
│   │   ├── assembly_stats/     # Assembly statistics files
│   │   ├── species_data/       # Species classification data
│   │   └── qc_data/            # QC data from tools like CheckM2, Sylph
│   └── processed/              # Generated/intermediate files
├── tests/
│   └── __init__.py
├── README.md
└── pyproject.toml
```

## Data Files and Schema

This project expects specific data files to be present in the `data/raw/` directory structure. Due to their size, these raw data files should not be committed to the repository and are included in the `.gitignore`.

### Default File Locations and Names:

The command-line tools and internal functions expect data to be organized as follows:

-   **Assembly Statistics:** `data/raw/assembly_stats/`
    -   `assembly-stats.tsv`: Main assembly statistics file.
        -   *Schema*: A TSV file where one column contains the Sample ID. Other columns can be any assembly metrics (e.g., N50, contig count, total length). The `llm-qc-species` tool adds a species column to this file, typically named `Species` or similar, creating `assembly-stats.with_species.tsv`.
    -   `hq_set.removed_samples.tsv`: A single-column TSV file listing Sample IDs that are considered low-quality or should be excluded from the high-quality set. This file should not have a header.
        -   *Schema*:
            ```
            SampleID_A
            SampleID_B
            ...
            ```
-   **QC Data:** `data/raw/qc_data/`
    -   `checkm2.tsv`: Output from CheckM2.
        -   *Schema*: Standard CheckM2 output format, with at least a Sample ID column and QC metrics like `Completeness` and `Contamination`.
    -   `sylph.tsv`: Output from Sylph.
        -   *Schema*: Standard Sylph output format, with at least a Sample ID column and relevant Sylph metrics.
-   **Species Data:** `data/raw/species_data/`
    -   `species_calls.tsv`: File containing species assignments for Sample IDs.
        -   *Schema*: A TSV file with at least two columns: one for Sample ID and one for the assigned species name (e.g., `SampleID`, `Species_Name`).

### Processed Data:

Processed files, such as merged datasets or sampled outputs, are typically saved in the `data/processed/` directory by the tools. For example:

-   `data/processed/assembly-stats.with_species.tsv`: Output of `llm-qc-species`.
-   `data/processed/merged_qc_results.tsv`: Default output of `llm-qc-merge`.
-   `data/processed/assembly-stats.sampled.tsv`: Default output of `llm-qc-sample`.

Users should place their raw data files according to these paths and naming conventions or use the command-line arguments provided by the tools to specify custom file paths.

## Usage

### Command-line Tools

The package provides several command-line tools for common tasks:

#### Merge Assembly QC Data

Merge different QC metrics (CheckM2, Sylph, species information) into a comprehensive dataset:

```bash
# Run with default settings
llm-qc-merge

# Specify custom input/output files
llm-qc-merge --checkm2 custom_checkm2.tsv --output merged_results.tsv
```

#### Process Species Data

Add species information to assembly statistics:

```bash
# Run with default settings
llm-qc-species

# Specify custom input/output files
llm-qc-species --stats-file custom_stats.tsv --species-file custom_species.tsv --output stats_with_species.tsv
```

#### Create Balanced Samples

Sample high-quality and low-quality assemblies to create balanced datasets:

```bash
# Run with default settings
llm-qc-sample

# Filter by species and specify sample size
llm-qc-sample --species "Escherichia coli" --sample-size 400
```

### Python API

You can also use the package as a Python library:

```python
import pandas as pd
from llm_qc.core.merge import load_datasets, merge_all_data
from llm_qc.processing.species import add_species_to_assembly_stats
from llm_qc.processing.sampling import sample_balanced_dataset

# Load and merge QC data
datasets = load_datasets()
merged_data = merge_all_data(datasets)

# Add species information
stats_with_species = add_species_to_assembly_stats()

# Create balanced samples
good_samples, bad_samples = sample_balanced_dataset(stats_with_species, sample_size=500)
```

## Data Handling

### Input Data

The package expects data to be organized in specific directories:

- **Assembly statistics**: `data/raw/assembly_stats/` 
- **Species information**: `data/raw/species_data/`
- **QC metrics** (CheckM2, Sylph): `data/raw/qc_data/`

### Output Data

Processed files are saved to `data/processed/` by default.

### File Formats

All input and output files are expected to be tab-separated values (TSV) files. The main identifying column should be named 'sample' or be the first column in each file.

## Development

### Setting Up a Development Environment

```bash
# Clone the repository
git clone https://github.com/yourusername/llm_qc.git
cd llm_qc

# Install development dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run tests with pytest
pytest

# Run tests with coverage
pytest --cov=llm_qc
```

### Code Style

This project uses:
- Black for code formatting
- Flake8 for linting
- MyPy for type checking

```bash
# Run formatters and linters
black src tests
flake8 src tests
mypy src
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

