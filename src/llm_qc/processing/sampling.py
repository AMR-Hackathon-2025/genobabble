"""
Sample assembly statistics module for generating balanced datasets.

This module provides functionality for sampling high-quality and low-quality genome
assemblies to create balanced datasets for analysis or machine learning.
"""

import logging
from pathlib import Path
from typing import Optional, Set, Tuple, Union
import argparse # Added import

import pandas as pd

from ..utils.file_handling import (
    get_data_dir,
    get_sample_column,
    read_sample_set_from_file,
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


def load_removed_samples(removed_samples_file: Optional[str] = None) -> Set[str]:
    """
    Load the set of removed (low-quality) sample IDs from a file.
    
    Args:
        removed_samples_file: Path to the file containing removed sample IDs (relative to assembly_stats directory)
                             If None, defaults to 'hq_set.removed_samples.tsv'
    
    Returns:
        Set of removed sample IDs (as strings)
    
    Raises:
        FileNotFoundError: If the removed samples file cannot be found
    """
    if removed_samples_file is None:
        removed_samples_file = "hq_set.removed_samples.tsv"
    
    removed_samples_path = get_data_dir("assembly_stats", raw=True) / removed_samples_file
    
    try:
        removed_samples = read_sample_set_from_file(removed_samples_path)
        logger.info(f"Loaded {len(removed_samples)} removed samples")
        return removed_samples
    except FileNotFoundError:
        logger.error(f"Removed samples file not found: {removed_samples_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading removed samples: {e}")
        raise


def load_assembly_stats(assembly_stats_file: Optional[str] = None) -> pd.DataFrame:
    """
    Load assembly statistics from a TSV file.
    
    Args:
        assembly_stats_file: Path to the assembly stats file (relative to assembly_stats directory)
                            If None, defaults to 'assembly-stats.with_species.tsv'
    
    Returns:
        DataFrame containing assembly statistics
    
    Raises:
        FileNotFoundError: If the assembly stats file cannot be found
        ValueError: If the loaded data is empty or malformed
    """
    if assembly_stats_file is None:
        assembly_stats_file = "assembly-stats.with_species.tsv"
    
    stats_path = get_data_dir("assembly_stats", raw=True) / assembly_stats_file
    
    try:
        df_stats = read_tsv(stats_path)
        logger.info(f"Loaded assembly stats with {len(df_stats)} rows")
        
        if df_stats.empty:
            raise ValueError("Assembly stats file is empty")
        
        return df_stats
    except FileNotFoundError:
        logger.error(f"Assembly stats file not found: {stats_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading assembly stats: {e}")
        raise


def filter_by_species(
    df: pd.DataFrame, 
    species: str, 
    species_column: Optional[str] = None
) -> pd.DataFrame:
    """
    Filter a DataFrame to include only samples of a specific species.
    
    Args:
        df: DataFrame to filter
        species: Species name to filter by
        species_column: Name of the column containing species names
                       If None, attempts to find a column with 'species' in the name
    
    Returns:
        Filtered DataFrame containing only samples of the specified species
    
    Raises:
        ValueError: If no species column can be found in the DataFrame
    """
    if species_column is None:
        # Try to find a species column
        species_columns = [col for col in df.columns if 'species' in col.lower()]
        if not species_columns:
            raise ValueError("No species column found in the DataFrame")
        species_column = species_columns[0]
        logger.info(f"Using '{species_column}' as the species column")
    
    filtered_df = df[df[species_column] == species].copy()
    logger.info(f"Filtered to {len(filtered_df)} samples of species '{species}'")
    
    if filtered_df.empty:
        logger.warning(f"No samples found for species '{species}'")
    
    return filtered_df


def split_by_quality(
    df: pd.DataFrame, 
    removed_samples: Set[str],
    sample_col: str  # Added sample_col argument
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split a DataFrame
    into high-quality and low-quality samples based on a set of removed samples.
    
    Args:
        df: DataFrame containing assembly statistics (assumed to have standardized sample IDs)
        removed_samples: Set of removed sample IDs (as strings)
        sample_col: Name of the column containing sample IDs
    
    Returns:
        Tuple of DataFrames: (high_quality_df, low_quality_df)
    """
    # Sample IDs are assumed to be standardized by the caller (e.g., in main)
    # df = standardize_sample_ids(df) # Removed redundant call
    
    # Split into high-quality and low-quality samples using the provided sample_col
    high_quality_df = df[~df[sample_col].isin(removed_samples)].copy()
    low_quality_df = df[df[sample_col].isin(removed_samples)].copy()
    
    logger.info(f"Split into {len(high_quality_df)} high-quality and "
                f"{len(low_quality_df)} low-quality samples")
    
    return high_quality_df, low_quality_df


def sample_dataframe(df: pd.DataFrame, num_samples: int, random_state: Optional[int] = None) -> pd.DataFrame:
    """
    Sample a specified number of rows from a DataFrame.
    
    Args:
        df: DataFrame to sample from
        num_samples: Number of samples to draw
        random_state: Optional random seed for reproducibility
        
    Returns:
        Sampled DataFrame
    """
    if len(df) == 0:
        logger.warning("Attempting to sample from an empty DataFrame. Returning empty DataFrame.")
        return pd.DataFrame(columns=df.columns)
    return df.sample(n=min(num_samples, len(df)), random_state=random_state)


def main():
    """
    Main function to perform sampling of assembly statistics.
    This function orchestrates the loading of data, filtering, splitting by quality,
    sampling, and saving the results. It is designed to be called as a command-line script.
    """
    parser = argparse.ArgumentParser(
        description="Sample assembly statistics to create balanced datasets."
    )
    parser.add_argument(
        "--assembly-stats-file",
        type=str,
        default="assembly-stats.with_species.tsv",
        help="Name of the assembly stats file (relative to data/raw/assembly_stats directory)."
    )
    parser.add_argument(
        "--removed-samples-file",
        type=str,
        default="hq_set.removed_samples.tsv",
        help="Name of the file containing removed sample IDs (relative to data/raw/assembly_stats directory)."
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="assembly-stats.sampled.tsv",
        help="Name for the output sampled TSV file (will be saved in data/processed directory)."
    )
    parser.add_argument(
        "--species",
        type=str,
        default=None,
        help="Optional: Species name to filter by before sampling."
    )
    parser.add_argument(
        "--num-samples",
        type=int,
        default=500,
        help="Number of samples to draw from each category (high-quality, low-quality)."
    )
    parser.add_argument(
        "--random-seed-hq",
        type=int,
        default=43, # Matches legacy script random_state for good samples
        help="Random seed for sampling high-quality data."
    )
    parser.add_argument(
        "--random-seed-lq",
        type=int,
        default=42, # Matches legacy script random_state for removed samples
        help="Random seed for sampling low-quality data."
    )

    args = parser.parse_args()

    logger.info("Starting assembly statistics sampling process...")

    try:
        # Load data
        logger.info(f"Loading assembly statistics from: {args.assembly_stats_file}")
        df_stats = load_assembly_stats(args.assembly_stats_file)
        
        logger.info(f"Loading removed samples from: {args.removed_samples_file}")
        removed_samples = load_removed_samples(args.removed_samples_file)

        # Standardize sample IDs in the main dataframe before any operations
        sample_id_col = get_sample_column(df_stats)  # Get sample ID column
        df_stats = standardize_sample_ids(df_stats, sample_id_col)

        # Filter by species if specified
        if args.species:
            logger.info(f"Filtering for species: {args.species}")
            df_stats = filter_by_species(df_stats, args.species)
            if df_stats.empty:
                logger.warning(f"No samples found for species '{args.species}' after filtering. Exiting.")
                return

        # Split by quality
        logger.info("Splitting samples by quality...")
        # Ensure removed_samples are strings, as SampleID in df_stats will be standardized to string
        removed_samples_str = {str(s) for s in removed_samples}
        
        # The split_by_quality function expects SampleID column, which standardize_sample_ids ensures
        high_quality_df, low_quality_df = split_by_quality(df_stats, removed_samples_str, sample_id_col) # Pass sample_id_col

        # Sample from each category
        logger.info(f"Sampling {args.num_samples} from high-quality samples...")
        hq_sampled_df = sample_dataframe(high_quality_df, args.num_samples, args.random_seed_hq)
        hq_sampled_df['hq_set'] = 'good_samples'
        
        logger.info(f"Sampling {args.num_samples} from low-quality samples...")
        lq_sampled_df = sample_dataframe(low_quality_df, args.num_samples, args.random_seed_lq)
        lq_sampled_df['hq_set'] = 'removed_samples'

        # Combine and save
        logger.info("Combining sampled data...")
        combined_df = pd.concat([hq_sampled_df, lq_sampled_df], ignore_index=True)
        
        output_path = get_data_dir("processed") / args.output_file # Corrected path
        logger.info(f"Writing {len(combined_df)} combined sampled rows to {output_path}")
        write_tsv(combined_df, output_path)
        
        logger.info("Assembly statistics sampling process completed successfully.")

    except FileNotFoundError as e:
        logger.error(f"File not found during sampling process: {e}")
    except ValueError as e:
        logger.error(f"ValueError during sampling process: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
