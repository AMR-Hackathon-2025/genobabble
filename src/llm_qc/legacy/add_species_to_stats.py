import pandas as pd

# File paths
stats_file = 'assembly-stats.tsv'
species_file = 'species_calls.tsv'
output_file = 'assembly-stats.with_species.tsv'

# Read files
df_stats = pd.read_csv(stats_file, sep='\t')
df_species = pd.read_csv(species_file, sep='\t')

# Try to find the sample column name in both files
def get_sample_col(df):
    for col in df.columns:
        if col.lower() == 'sample':
            return col
    return df.columns[0]

stats_sample_col = get_sample_col(df_stats)
species_sample_col = get_sample_col(df_species)

# Merge on sample column
merged = pd.merge(df_stats, df_species, left_on=stats_sample_col, right_on=species_sample_col, how='left')

# Save result
merged.to_csv(output_file, sep='\t', index=False)
print(f"Wrote merged table with species to {output_file}")
