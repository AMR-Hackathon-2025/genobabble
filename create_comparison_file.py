#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys

def create_comparison_file(prediction_file, complete_stats_file, output_file=None):
    """
    Extract matching samples from assembly-stats-complete.tsv based on a prediction file,
    add the QC_Prediction column, and save to a new comparison file.
    """
    # Load files
    print(f"Loading prediction file: {prediction_file}")
    df_pred = pd.read_csv(prediction_file)
    
    print(f"Loading complete stats file: {complete_stats_file}")
    df_stats = pd.read_csv(complete_stats_file, sep='\t')
    
    # Get sample column name from prediction file
    pred_sample_col = 'sample'
    if pred_sample_col not in df_pred.columns:
        pred_sample_col = df_pred.columns[0]
    
    # Get sample column name from stats file
    stats_sample_col = 'sample'
    if stats_sample_col not in df_stats.columns:
        stats_sample_col = df_stats.columns[0]
    
    # Convert sample columns to string for consistent merging
    df_pred[pred_sample_col] = df_pred[pred_sample_col].astype(str)
    df_stats[stats_sample_col] = df_stats[stats_sample_col].astype(str)
    
    # Get the QC prediction column name
    qc_pred_col = 'QC_Prediction'
    if qc_pred_col not in df_pred.columns:
        # Try to find a column containing 'QC' or 'prediction'
        for col in df_pred.columns:
            if 'qc' in col.lower() or 'prediction' in col.lower():
                qc_pred_col = col
                break
        else:
            # If not found, use the last column
            qc_pred_col = df_pred.columns[-1]
    
    # Extract only the needed columns from prediction file
    df_pred_subset = df_pred[[pred_sample_col, qc_pred_col]]
    
    # Merge stats with prediction
    print("Merging files...")
    merged = pd.merge(
        df_stats,
        df_pred_subset,
        left_on=stats_sample_col,
        right_on=pred_sample_col,
        how='inner'
    )
    
    # Remove duplicate sample column if it exists
    if pred_sample_col != stats_sample_col and pred_sample_col in merged.columns:
        merged = merged.drop(columns=[pred_sample_col])
    
    # Generate output filename if not provided
    if output_file is None:
        pred_basename = os.path.splitext(os.path.basename(prediction_file))[0]
        output_file = f"{pred_basename}_compare_stats.tsv"
    
    # Save the merged file
    print(f"Writing output to {output_file}...")
    merged.to_csv(output_file, sep='\t', index=False)
    print(f"Comparison file created with {len(merged)} rows and {len(merged.columns)} columns.")
    
    return output_file

if __name__ == "__main__":
    # Default file paths
    prediction_file = "E_coli_QC_Predictions.csv"
    complete_stats_file = "assembly-stats-complete.tsv"
    
    # Check for command line arguments
    if len(sys.argv) > 1:
        prediction_file = sys.argv[1]
    
    if len(sys.argv) > 2:
        complete_stats_file = sys.argv[2]
    
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    else:
        output_file = None
    
    # Run the main function
    create_comparison_file(prediction_file, complete_stats_file, output_file)
