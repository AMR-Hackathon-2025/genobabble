"""
Utility functions for file handling and data processing in the llm_qc package.

This module provides common functionality for working with data files and pandas DataFrames,
including sample column detection, file path resolution, and common DataFrame operations.
"""

import os
from pathlib import Path
from typing import List, Optional, Set, Tuple, Union

import pandas as pd


def get_sample_column(df: pd.DataFrame) -> str:
    """
    Find the column name that represents sample IDs in a DataFrame.
    
    This function looks for a column named 'sample' (case-insensitive) and falls back
    to the first column if no 'sample' column is found.
    
    Args:
        df: The pandas DataFrame to analyze
        
    Returns:
        The name of the column containing sample identifiers
    """
    for col in df.columns:
        if col.lower() == 'sample':
            return col
    # If no column named 'sample', use the first column
    return df.columns[0]


def get_project_root() -> Path:
    """
    Get the absolute path to the project root directory.
    
    Returns:
        Path object representing the project root directory
    """
    # Assuming this module is in src/llm_qc/utils/file_handling.py
    return Path(__file__).parent.parent.parent.parent


def get_data_dir(data_type: str = "", raw: bool = True) -> Path:
    """
    Get the path to a data directory.
    
    Args:
        data_type: Optional subdirectory name within raw or processed data
                  (e.g., "assembly_stats", "species_data", "qc_data")
        raw: If True, return path to raw data, otherwise processed data
        
    Returns:
        Path object pointing to the requested data directory
    """
    root = get_project_root()
    base_data_dir = root / "data" / ("raw" if raw else "processed")
    
    if data_type:
        return base_data_dir / data_type
    return base_data_dir


def read_tsv(file_path: Union[str, Path]) -> pd.DataFrame:
    """
    Read a tab-separated value file into a pandas DataFrame.
    
    Args:
        file_path: Path to the TSV file
        
    Returns:
        DataFrame containing the file contents

    Raises:
        FileNotFoundError: If the file does not exist
        pd.errors.EmptyDataError: If the file is empty
        pd.errors.ParserError: If the file cannot be parsed
    """
    try:
        return pd.read_csv(file_path, sep='\t')
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        raise pd.errors.EmptyDataError(f"Empty file: {file_path}")
    except pd.errors.ParserError:
        raise pd.errors.ParserError(f"Failed to parse file: {file_path}")
    except Exception as e:
        raise RuntimeError(f"Error reading file {file_path}: {str(e)}")


def write_tsv(df: pd.DataFrame, file_path: Union[str, Path], 
              index: bool = False, create_dir: bool = True) -> None:
    """
    Write a pandas DataFrame to a tab-separated value file.
    
    Args:
        df: DataFrame to save
        file_path: Path where the file should be saved
        index: Whether to include the DataFrame index in the output file
        create_dir: Whether to create parent directories if they don't exist

    Raises:
        IOError: If the file cannot be written
    """
    path = Path(file_path)
    
    if create_dir and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        df.to_csv(path, sep='\t', index=index)
        print(f"Wrote {len(df)} rows to {path}")
    except Exception as e:
        raise IOError(f"Failed to write to {path}: {str(e)}")


def read_sample_set_from_file(file_path: Union[str, Path], 
                             column_index: int = 0, 
                             skip_header: bool = True) -> Set[str]:
    """
    Read a set of sample IDs from a file.
    
    Args:
        file_path: Path to the file containing sample IDs
        column_index: Index of the column containing sample IDs (0-based)
        skip_header: Whether to skip the first line of the file
        
    Returns:
        Set of sample IDs (as strings)
    """
    sample_ids = set()
    with open(file_path, encoding='utf-8') as f:
        if skip_header:
            next(f, None)  # Skip header
        
        for line in f:
            if line.strip():
                try:
                    sample_id = line.split('\t')[column_index].strip()
                    sample_ids.add(str(sample_id))
                except IndexError:
                    continue  # Skip malformed lines
    
    return sample_ids


def standardize_sample_ids(df: pd.DataFrame, sample_col: str) -> pd.DataFrame:
    """
    Standardize sample IDs by converting them to strings.
    
    Args:
        df: DataFrame to process
        sample_col: Name of the column containing sample IDs
        
    Returns:
        DataFrame with standardized sample IDs
    """
    df = df.copy()
    df[sample_col] = df[sample_col].astype(str)
    return df


def ensure_directory_exists(dir_path: Union[str, Path]) -> Path:
    """
    Ensure that a directory exists, creating it if necessary.
    
    Args:
        dir_path: Path to the directory
        
    Returns:
        Path object for the directory
    """
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def file_exists(file_path: Union[str, Path]) -> bool:
    """
    Check if a file exists.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if the file exists, False otherwise
    """
    return Path(file_path).is_file()


def merge_dataframes_on_sample(
    df1: pd.DataFrame, 
    df2: pd.DataFrame, 
    how: str = 'left',
    drop_duplicate_sample_col: bool = True
) -> pd.DataFrame:
    """
    Merge two DataFrames on their sample columns.
    
    Args:
        df1: First DataFrame (left side of the merge)
        df2: Second DataFrame (right side of the merge)
        how: Type of merge to perform ('left', 'right', 'inner', 'outer')
        drop_duplicate_sample_col: Whether to drop the duplicate sample column after merging
        
    Returns:
        Merged DataFrame
    """
    # Find sample columns in both DataFrames
    df1_sample_col = get_sample_column(df1)
    df2_sample_col = get_sample_column(df2)
    
    # Standardize sample IDs
    df1 = standardize_sample_ids(df1, df1_sample_col)
    df2 = standardize_sample_ids(df2, df2_sample_col)
    
    # Merge DataFrames
    merged = pd.merge(
        df1, df2,
        left_on=df1_sample_col,
        right_on=df2_sample_col,
        how=how
    )
    
    # Drop duplicate sample column if requested and if it exists
    if drop_duplicate_sample_col and df1_sample_col != df2_sample_col and df2_sample_col in merged.columns:
        merged = merged.drop(columns=[df2_sample_col])
    
    return merged

