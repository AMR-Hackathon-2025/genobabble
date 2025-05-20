"""
Core functionality for merging different types of genome assembly QC data.

This module provides functions to combine data from various sources (assembly stats,
checkm2, sylph, species calls, etc.) into a single comprehensive dataset.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

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
    no_hqset_file: Optional[str] = None
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
    if no_hqset_file is None:
        no_hqset_file = "assembly-stats.sampled.no_hqset.tsv"
    
    # Construct file paths
    assembly_stats_path = get_data_dir("assembly_stats", raw=True) / assembly_stats_file
    checkm2_path = get_data_dir("qc_data", raw=True) / checkm2_file
    sylph_path = get_data_dir("qc_data", raw=True) / sylph_file
    species_path = get_data_dir("species_data", raw=True) / species_file
    no_hqset_path = get_data_dir("assembly_stats", raw=True) / no_hqset_file
    
    # Load DataFrames
    logger.info("Loading datasets...")
    datasets = {}
    
    try:
        datasets["stats"] = read_tsv(assembly_stats_path)
        logger.info(f"Loaded assembly stats with {len(datasets['stats'])} rows")
    except Exception as e:
        logger.error(f"Error loading assembly stats: {e}")
        raise
    
    try:
        datasets["checkm2"] = read_tsv(checkm2_path)
        logger.info(f"Loaded checkm2 with {len(datasets['checkm2'])} rows")
    except Exception as e:
        logger.error(f"Error loading checkm2: {e}")
        raise
    
    try:
        datasets["sylph"] = read_tsv(sylph_path)
        logger.info(f"Loaded sylph with {len(datasets['sylph'])} rows")
    except Exception as e:
        logger.error(f"Error loading sylph: {e}")
        raise
    
    try:
        datasets["species"] = read_tsv(species_path)
        logger.info(f"Loaded species calls with {len(datasets['species'])} rows")
    except Exception

