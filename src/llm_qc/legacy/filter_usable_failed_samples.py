import pandas as pd
import argparse


def extract_and_merge_samples(assembly_stats_file, removed_samples_file,
                              output_file):
    """
    Extracts samples listed in removed_samples_file from assembly_stats_file
    and merges all columns from removed_samples_file.
    """
    try:
        removed_df = pd.read_csv(removed_samples_file, sep='\t', header=0,
                                 engine='python')
        if removed_df.empty:
            print(f"Error: {removed_samples_file} is empty.")
            return

        original_removed_id_col = removed_df.columns[0]
        removed_df.rename(columns={original_removed_id_col: 'SampleID'},
                          inplace=True)
        removed_df['SampleID'] = removed_df['SampleID'].astype(str)

        sample_ids_to_extract = set(removed_df['SampleID'])
        print(f"Found {len(sample_ids_to_extract)} sample IDs to extract from "
              f"{removed_samples_file}. "
              f"It has {len(removed_df.columns)} columns.")

        stats_df = pd.read_csv(assembly_stats_file, sep='\t', header=0)
        print(f"Read {len(stats_df)} rows from {assembly_stats_file}")

        if 'SampleID' not in stats_df.columns:
            if stats_df.columns.any():
                original_stats_id_col_name = stats_df.columns[0]
                stats_df.rename(
                    columns={original_stats_id_col_name: 'SampleID'},
                    inplace=True)
                print(f"Warning: 'SampleID' column not found in "
                      f"{assembly_stats_file}. Using first column "
                      f"'{original_stats_id_col_name}' as SampleID.")
            else:
                print(f"Error: {assembly_stats_file} has no columns or "
                      "'SampleID' column is missing.")
                return

        stats_df['SampleID'] = stats_df['SampleID'].astype(str)

        extracted_samples_df = stats_df[stats_df['SampleID'].isin(
            sample_ids_to_extract)].copy()
        print(f"Found {len(extracted_samples_df)} matching samples in "
              f"{assembly_stats_file}.")

        if extracted_samples_df.empty:
            print(f"No samples from {removed_samples_file} were found in "
                  f"{assembly_stats_file}. The output file will be empty or "
                  f"contain only headers if merging an empty frame.")

        merged_df = pd.merge(extracted_samples_df, removed_df, on='SampleID',
                             how='left', suffixes=('', '_removed'))

        print(f"Merged data has {len(merged_df)} rows and "
              f"{len(merged_df.columns)} columns.")

        merged_df.to_csv(output_file, sep='\t', index=False)
        print(f"Extracted and merged samples saved to {output_file}")

    except FileNotFoundError:
        print("Error: One of the input files was not found.")
    except pd.errors.EmptyDataError:
        print("Error: One of the input files is empty or could not be parsed.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Extracts samples listed in a 'removed samples' file "
                     "from a 'complete assembly stats' file and merges "
                     "columns from the 'removed samples' file.")
    )
    parser.add_argument("assembly_stats_complete_file",
                        help="Path to the main assembly statistics file "
                             "(e.g., assembly-stats-complete.tsv).")
    parser.add_argument("removed_samples_file",
                        help="Path to the file containing IDs of samples to "
                             "extract and their additional data (e.g., "
                             "hq_set.removed_samples.tsv).")
    parser.add_argument("output_file",
                        help="Path to save the filtered and merged samples.")

    args = parser.parse_args()

    extract_and_merge_samples(args.assembly_stats_complete_file,
                              args.removed_samples_file,
                              args.output_file)
