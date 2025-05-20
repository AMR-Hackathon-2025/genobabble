"""
Species data processing functionality for genome assembly quality control.

This module provides functions for adding species information to assembly statistics
and processing species-related data for genomic assemblies.
"""

import logging
from pathlib import Path
from typing import Optional, Tuple, Union

import pandas as pd

from ..utils.file_handling import (
    get_data_dir,
    get_sample_column,
    read_tsv,
    standardize_sample_ids,
    write_tsv
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_species_data(species_file: Optional[str] = None) -> pd.DataFrame:
    """
    Load species data from a TSV file.
    
    Args:
        species_file: Name of the species data file (relative to the species_data directory)
                      If None, defaults to 'species_calls.tsv'
    
    Returns:
        DataFrame containing species information
    
    Raises:
        FileNotFoundError: If the species file cannot be found
        ValueError: If the loaded data is empty or malformed
    """
    if species_file is None:
        species_file = "species_calls.tsv"
    
    species_path = get_data_dir("species_data", raw=True) / species_file
    
    try:
        df_species = read_tsv(species_path)
        logger.info(f"Loaded species data with {len(df_species)} rows")
        
        if df_species.empty:
            raise ValueError("Species data file is empty")
        
        return df_species
    except FileNotFoundError:
        logger.error(f"Species file not found: {species_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading species data: {e}")
        raise


def verify_species_columns(df: pd.DataFrame) -> None:
    """
    Verify that the species DataFrame contains expected columns.
    
    Args:
        df: DataFrame to verify
    
    Raises:
        ValueError: If required columns are missing
    """
    # Check for species column - name might vary but should have 'species' in it
    species_columns = [col for col in df.columns if 'species' in col.lower()]
    
    if not species_columns:
        raise ValueError("No species-related columns found in the species data")
    
    logger.info(f"Species columns found: {', '.join(species_columns)}")


def add_species_to_assembly_stats(
    stats_file: Optional[str] = None,
    species_file: Optional[str] = None,
    output_file: Optional[str] = None
) -> pd.DataFrame:
    """
    Add species information to assembly statistics data.
    
    Args:
        stats_file: Path to the assembly stats file (relative to assembly_stats directory)
                    If None, defaults to 'assembly-stats.tsv'
        species_file: Path to the species data file (relative to species_data directory)
                      If None, defaults to 'species_calls.tsv'
        output_file: Path to save the merged results (relative to processed directory)
                     If None, defaults to 'assembly-stats.with_species.tsv'
    
    Returns:
        DataFrame containing assembly stats with added species information
    
    Raises:
        FileNotFoundError: If input files cannot be found
        ValueError: If the data is malformed or incompatible
    """
    # Set default file names if not provided
    if stats_file is None:
        stats_file = "assembly-stats.tsv"
    if species_file is None:
        species_file = "species_calls.tsv"
    if output_file is None:
        output_file = "assembly-stats.with_species.tsv"
    
    # Construct file paths
    stats_path = get_data_dir("assembly_stats", raw=True) / stats_file
    output_path = get_data_dir(raw=False) / output_file
    
    # Load assembly stats
    try:
        logger.info(f"Loading assembly stats from {stats_path}")
        df_stats = read_tsv(stats_path)
        logger.info(f"Loaded assembly stats with {len(df_stats)} rows")
        
        if df_stats.empty:
            raise ValueError("Assembly stats file is empty")
    except FileNotFoundError:
        logger.error(f"Assembly stats file not found: {stats_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading assembly stats: {e}")
        raise
    
    # Load species data
    df_species = load_species_data(species_file)
    verify_species_columns(df_species)
    
    # Get sample column names from both datasets
    stats_sample_col = get_sample_column(df_stats)
    species_sample_col = get_sample_column(df_species)
    
    logger.info(f"Using sample columns: {stats_sample_col} (stats) and {species_sample_col} (species)")
    
    # Standardize sample columns to ensure consistent merging
    df_stats = standardize_sample_ids(df_stats, stats_sample_col)
    df_species = standardize_sample_ids(df_species, species_sample_col)
    
    # Merge datasets on sample column
    try:
        merged = pd.merge(
            df_stats, 
            df_species, 
            left_on=stats_sample_col, 
            right_on=species_sample_col, 
            how='left'
        )
        
        # Check if merge was successful
        if len(merged) != len(df_stats):
            logger.warning(
                f"Merged dataset size ({len(merged)}) differs from original stats ({len(df_stats)}). "
                "This may indicate duplicate samples."
            )
        
        # Check for missing species values
        missing_species = merged[merged[species_sample_col].isna()].shape[0]
        if missing_species > 0:
            logger.warning(f"{missing_species} entries have no matching species information")
        
        logger.info(f"Successfully merged assembly stats with species information")
    except Exception as e:
        logger.error(f"Error merging datasets: {e}")
        raise
    
    # Clean up duplicate columns if they exist
    if stats_sample_col != species_sample_col and species_sample_col in merged.columns:
        if all(merged[stats_sample_col] == merged[species_sample_col]):
            logger.info(f"Removing duplicate sample column: {species_sample_col}")
            merged = merged.drop(columns=[species_sample_col])
    
    # Save result
    try:
        logger.info(f"Writing merged data to {output_path}")
        write_tsv(merged, output_path)
        logger.info(f"Successfully wrote {len(merged)} rows to {output_path}")
    except Exception as e:
        logger.error(f"Error writing output file: {e}")
        raise
    
    return merged


def main() -> None:
    """Main entry point for the species processing module."""
    try:
        add_species_to_assembly_stats()
    except Exception as e:
        logger.error(f"Species processing failed: {e}")
        raise


if __name__ == "__main__":
    main()

