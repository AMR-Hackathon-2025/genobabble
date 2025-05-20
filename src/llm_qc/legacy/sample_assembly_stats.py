import pandas as pd
import sys

# File paths
removed_samples_file = 'hq_set.removed_samples.tsv'
assembly_stats_file = 'assembly-stats.with_species.tsv'
output_file = 'assembly-stats.sampled.tsv'

# Read removed sample IDs (skip header, get first column only)
with open(removed_samples_file, encoding='utf-8') as f:
    next(f)  # skip header
    removed_samples = set(
        line.split('\t')[0].strip()
        for line in f if line.strip()
    )

# Read assembly stats
df = pd.read_csv(assembly_stats_file, sep='\t')

# Check for sample column
sample_col = 'sample' if 'sample' in df.columns else df.columns[0]

# Get all sample IDs from assembly stats
all_ids = set(df[sample_col].astype(str))
removed_samples = set(str(s) for s in removed_samples)

# Only keep removed samples that are present in assembly-stats.tsv
removed_ids = all_ids & removed_samples

df_removed = df[df[sample_col].astype(str).isin(removed_ids)].copy()
df_good = df[~df[sample_col].astype(str).isin(removed_ids)].copy()

# Ensure no overlap between sets
assert set(df_removed[sample_col]).isdisjoint(set(df_good[sample_col]))

# Optionally specify a species as a command-line argument
species = None
if len(sys.argv) > 1:
    species = sys.argv[1]
    print(f"Filtering for species: {species}")
    if 'Species' not in df.columns:
        raise ValueError("No 'species' column found in the input file.")
    df = df[df['Species'] == species]
    # Recompute all_ids, removed_ids, df_removed, df_good after filtering
    all_ids = set(df[sample_col].astype(str))
    removed_ids = all_ids & removed_samples
    df_removed = df[df[sample_col].astype(str).isin(removed_ids)].copy()
    df_good = df[~df[sample_col].astype(str).isin(removed_ids)].copy()
    assert set(df_removed[sample_col]).isdisjoint(set(df_good[sample_col]))

# Sample 500 from each (or all if less than 500)
df_removed_sampled = df_removed.sample(
    n=min(500, len(df_removed)), random_state=42
)
df_good_sampled = df_good.sample(
    n=min(500, len(df_good)), random_state=43
)

# Add hq_set column
df_removed_sampled['hq_set'] = 'removed_samples'
df_good_sampled['hq_set'] = 'good_samples'

# Combine and save
combined = pd.concat([df_removed_sampled, df_good_sampled], ignore_index=True)
print(f"Removed: {len(df_removed_sampled)}, "
      f"Good: {len(df_good_sampled)}, "
      f"Total: {len(combined)}")
combined.to_csv(output_file, sep='\t', index=False)
print(f"Wrote {len(combined)} rows to {output_file}")
