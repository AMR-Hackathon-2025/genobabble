sampled_path = 'assembly-stats.sampled.tsv'
no_hqset_path = 'assembly-stats.sampled.no_hqset.tsv'

import pandas as pd

df = pd.read_csv(sampled_path, sep='\t')
# Drop the last column (hq_set)
df_no_hqset = df.iloc[:, :-2]
df_no_hqset.to_csv(no_hqset_path, sep='\t', index=False)
print(f"Wrote {no_hqset_path} without the last column.")
