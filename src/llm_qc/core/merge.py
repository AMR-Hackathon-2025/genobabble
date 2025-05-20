"""
Core functionality for merging different types of genome assembly QC data.

This module provides functions to combine data from various sources
(assembly stats, checkm2, sylph, species calls, etc.) into a single
comprehensive dataset.
"""

import logging
from typing import Dict, Optional

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


def load_datasets(
    assembly_stats_file: Optional[str] = None,
    checkm2_file: Optional[str] = None,
    sylph_file: Optional[str] = None,
    species_file: Optional[str] = None,
    no_hqset_file: Optional[str] = None  # Parameter kept for now
) -> Dict[str, pd.DataFrame]:
    """
    Load all the required datasets from their respective files.
    
    Args:
        assembly_stats_file: Path to assembly stats file (relative to data dir)
        checkm2_file: Path to checkm2 file (relative to data dir)
        sylph_file: Path to sylph file (relative to data dir)
        species_file: Path to species calls file (relative to data dir)
        no_hqset_file: Path to no_hqset file (relative to data dir)
        
    Returns:
        Dictionary containing all loaded DataFrames
    """
    # Set default file names if not provided
    if assembly_stats_file is None:
        assembly_stats_file = "assembly-stats.tsv"
    if checkm2_file is None:
        checkm2_file = "checkm2.tsv"
    if sylph_file is None:
        sylph_file = "sylph.tsv"
    if species_file is None:
        species_file = "species_calls.tsv"
    # Default for no_hqset_file is handled by its usage or lack thereof.
    # If it were to be loaded, a default name would be set here.
    # e.g., if no_hqset_file is None:
    # no_hqset_file = "assembly-stats.sampled.no_hqset.tsv"

    # Construct file paths
    data_dir_raw_assembly = get_data_dir("assembly_stats", raw=True)
    data_dir_raw_qc = get_data_dir("qc_data", raw=True)
    data_dir_raw_species = get_data_dir("species_data", raw=True)

    assembly_stats_path = data_dir_raw_assembly / assembly_stats_file
    checkm2_path = data_dir_raw_qc / checkm2_file
    sylph_path = data_dir_raw_qc / sylph_file
    species_path = data_dir_raw_species / species_file
    
    # The no_hqset_file parameter exists, but its path construction and loading
    # were commented out or incomplete. If it's to be used, it needs to be
    # properly integrated. For now, no_hqset_path is not created.
    # if no_hqset_file:
    #     no_hqset_path = data_dir_raw_assembly / no_hqset_file

    logger.info("Loading datasets...")
    datasets = {}
    
    try:
        datasets["stats"] = read_tsv(assembly_stats_path)
        logger.info(f"Loaded assembly stats: {len(datasets['stats'])} rows")
    except Exception as e:
        logger.error(f"Error loading assembly stats: {e}")
        raise
    
    try:
        datasets["checkm2"] = read_tsv(checkm2_path)
        logger.info(f"Loaded checkm2: {len(datasets['checkm2'])} rows")
    except Exception as e:
        logger.error(f"Error loading checkm2: {e}")
        raise
    
    try:
        datasets["sylph"] = read_tsv(sylph_path)
        logger.info(f"Loaded sylph: {len(datasets['sylph'])} rows")
    except Exception as e:
        logger.error(f"Error loading sylph: {e}")
        raise
    
    try:
        datasets["species"] = read_tsv(species_path)
        logger.info(f"Loaded species calls: {len(datasets['species'])} rows")
    except Exception as e:
        logger.error(f"Error loading species calls: {e}")
        raise
    
    # If no_hqset_file is intended to be loaded, it should be done here.
    # Example:
    # if no_hqset_file:
    #     try:
    #         no_hqset_path = data_dir_raw_assembly / no_hqset_file
    #         datasets["no_hqset"] = read_tsv(no_hqset_path)
    #         logger.info(f"Loaded no_hqset data: {len(datasets['no_hqset'])} rows")
    #     except Exception as e:
    #         logger.warning(f"Could not load no_hqset data from {no_hqset_file}: {e}")
    #         # Decide if this should be a critical error or just a warning
    
    return datasets


def merge_qc_data(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all loaded QC datasets into a single DataFrame.
    
    Args:
        datasets: Dictionary of DataFrames from load_datasets
        
    Returns:
        A single merged DataFrame.
    """
    logger.info("Merging datasets...")
    
    # Standardize sample IDs across all dataframes
    for name, df in datasets.items():
        if not df.empty:
            sample_col = get_sample_column(df)
            datasets[name] = standardize_sample_ids(df, sample_col)
            logger.info(f"Standardized sample IDs for {name} (col: '{sample_col}')")
        else:
            logger.warning(f"Dataset {name} is empty, skipping standardization.")

    # Start with assembly stats as the base
    merged_df = datasets.get("stats")
    if merged_df is None or merged_df.empty:
        msg = "Assembly stats data is missing/empty. Cannot merge."
        logger.error(msg)
        raise ValueError(msg)

    # Merge CheckM2 data
    checkm2_df = datasets.get("checkm2")
    if checkm2_df is not None and not checkm2_df.empty:
        merged_df = pd.merge(merged_df, checkm2_df, on="sample_id", how="left")
        logger.info("Merged CheckM2 data.")
    else:
        logger.warning("CheckM2 data not found or empty. Skipping merge.")

    # Merge Sylph data
    sylph_df = datasets.get("sylph")
    if sylph_df is not None and not sylph_df.empty:
        merged_df = pd.merge(merged_df, sylph_df, on="sample_id", how="left")
        logger.info("Merged Sylph data.")
    else:
        logger.warning("Sylph data not found or empty. Skipping merge.")

    # Merge Species data
    species_df = datasets.get("species")
    if species_df is not None and not species_df.empty:
        merged_df = pd.merge(merged_df, species_df, on="sample_id", how="left")
        logger.info("Merged Species data.")
    else:
        logger.warning("Species data not found or empty. Skipping merge.")
        
    # Handle 'no_hqset' data if it was loaded and is present
    no_hqset_df = datasets.get("no_hqset")
    if no_hqset_df is not None and not no_hqset_df.empty:
        # Assuming 'no_hqset' contains a list of sample_ids to flag
        # Ensure 'sample_id' column exists after standardization
        if 'sample_id' in no_hqset_df.columns:
            no_hqset_samples = set(no_hqset_df['sample_id'])
            merged_df['is_no_hqset'] = merged_df['sample_id'].isin(no_hqset_samples)
            logger.info("Flagged samples from 'no_hqset' data.")
        else:
            logger.warning("'sample_id' column not found in 'no_hqset' data after "
                           "standardization. Cannot flag samples.")
    elif "no_hqset" in datasets: # It was attempted to load but was empty
        logger.info("'no_hqset' data was loaded but is empty. No samples to flag.")
    # If "no_hqset" was not in datasets at all, no message is needed here.

    logger.info(f"Merge complete. Final shape: {merged_df.shape}")
    return merged_df


def main(
    output_file: str = "merged_qc_data.tsv",
    assembly_stats_file: Optional[str] = None,
    checkm2_file: Optional[str] = None,
    sylph_file: Optional[str] = None,
    species_file: Optional[str] = None,
    no_hqset_file: Optional[str] = None
) -> None:
    """
    Main function to run the full merge pipeline.
    
    Args:
        output_file: Name of the output file (relative to processed data dir)
        assembly_stats_file: Path to assembly stats file (relative to data dir)
        checkm2_file: Path to checkm2 file (relative to data dir)
        sylph_file: Path to sylph file (relative to data dir)
        species_file: Path to species calls file (relative to data dir)
        no_hqset_file: Path to no_hqset file (relative to data dir)
    """
    logger.info("Starting QC data merging process...")
    
    try:
        datasets = load_datasets(
            assembly_stats_file=assembly_stats_file,
            checkm2_file=checkm2_file,
            sylph_file=sylph_file,
            species_file=species_file,
            no_hqset_file=no_hqset_file
        )
        
        merged_df = merge_qc_data(datasets)
        
        output_path = get_data_dir(processed=True) / output_file
        write_tsv(merged_df, output_path)
        logger.info(f"Merged QC data written to {output_path}")
        
    except FileNotFoundError as e:
        logger.error(f"Input file not found: {e}")
        raise
    except ValueError as e:
        logger.error(f"Data error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in merge process: {e}")
        raise


if __name__ == "__main__":
    # Example of direct execution (e.g., for testing)
    # Consider using argparse for CLI argument parsing if run directly often.
    main()

