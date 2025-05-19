import pandas as pd

# File paths
qc_file = 'E_coli_QC_Predictions.csv'
sampled_file = 'assembly-stats.sampled.tsv'

# Read files
df_qc = pd.read_csv(qc_file)
df_sampled = pd.read_csv(sampled_file, sep='\t')


def map_hq(val):
    return (
        'good_samples' if str(val).strip().upper() == 'T'
        else 'removed_samples'
    )


def map_qc(val):
    return (
        'good_samples' if str(val).strip().lower() == 'pass'
        else 'removed_samples'
    )


df_sampled['truth'] = df_sampled['HQ'].apply(map_hq)
df_qc['pred'] = df_qc['QC_Prediction'].apply(map_qc)

# Merge on sample column
merged = pd.merge(
    df_sampled, df_qc[['sample', 'pred']],
    left_on='sample', right_on='sample', how='inner'
)

true_positives = (
    (merged['truth'] == 'good_samples') &
    (merged['pred'] == 'good_samples')
).sum()
false_positives = (
    (merged['truth'] == 'removed_samples') &
    (merged['pred'] == 'good_samples')
).sum()
true_negatives = (
    (merged['truth'] == 'removed_samples') &
    (merged['pred'] == 'removed_samples')
).sum()
false_negatives = (
    (merged['truth'] == 'good_samples') &
    (merged['pred'] == 'removed_samples')
).sum()

print(f"True Positives: {true_positives}")
print(f"False Positives: {false_positives}")
print(f"True Negatives: {true_negatives}")
print(f"False Negatives: {false_negatives}")
