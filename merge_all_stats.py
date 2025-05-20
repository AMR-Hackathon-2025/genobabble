import pandas as pd

# Define file paths
checkm2_file = 'checkm2.tsv'
sylph_file = 'sylph.tsv'
assembly_stats_file = 'assembly-stats.tsv'
species_calls_file = 'species_calls.tsv'
no_hqset_file = 'assembly-stats.sampled.no_hqset.tsv'
output_file = 'assembly-stats-complete.tsv'

# Load all files
print("Loading files...")
df_checkm2 = pd.read_csv(checkm2_file, sep='\t')
df_sylph = pd.read_csv(sylph_file, sep='\t')
df_stats = pd.read_csv(assembly_stats_file, sep='\t')
df_species = pd.read_csv(species_calls_file, sep='\t')
df_no_hqset = pd.read_csv(no_hqset_file, sep='\t')

# Function to standardize sample column names
def get_sample_col(df):
    for col in df.columns:
        if col.lower() == 'sample':
            return col
    # If no column named 'sample', use the first column
    return df.columns[0]

# Get sample column names from each dataframe
checkm2_sample_col = get_sample_col(df_checkm2)
sylph_sample_col = get_sample_col(df_sylph)
stats_sample_col = get_sample_col(df_stats)
species_sample_col = get_sample_col(df_species)
no_hqset_sample_col = get_sample_col(df_no_hqset)

# Start with assembly stats and merge others
print("Merging dataframes...")
merged = df_stats.copy()

# Convert sample columns to string for consistent merging
merged[stats_sample_col] = merged[stats_sample_col].astype(str)
df_checkm2[checkm2_sample_col] = df_checkm2[checkm2_sample_col].astype(str)
df_sylph[sylph_sample_col] = df_sylph[sylph_sample_col].astype(str)
df_species[species_sample_col] = df_species[species_sample_col].astype(str)
df_no_hqset[no_hqset_sample_col] = df_no_hqset[no_hqset_sample_col].astype(str)

# Merge with checkm2
merged = pd.merge(
    merged, df_checkm2, 
    left_on=stats_sample_col, right_on=checkm2_sample_col, 
    how='left'
)

# Merge with sylph
merged = pd.merge(
    merged, df_sylph, 
    left_on=stats_sample_col, right_on=sylph_sample_col, 
    how='left'
)

# Merge with species calls
merged = pd.merge(
    merged, df_species, 
    left_on=stats_sample_col, right_on=species_sample_col, 
    how='left'
)

# Create QC column based on presence in no_hqset
no_hqset_samples = set(df_no_hqset[no_hqset_sample_col])
merged['QC'] = merged[stats_sample_col].apply(
    lambda x: 'Pass' if x not in no_hqset_samples else 'Fail'
)

# Clean up duplicate columns if they exist
cols_to_drop = [c for c in merged.columns 
                if c != stats_sample_col and 
                c in [checkm2_sample_col, sylph_sample_col, species_sample_col] and
                c != stats_sample_col]

merged = merged.drop(columns=cols_to_drop, errors='ignore')

# Save the merged file
print(f"Writing output to {output_file}...")
merged.to_csv(output_file, sep='\t', index=False)
print(f"Merged file created with {len(merged)} rows and {len(merged.columns)} columns.")
