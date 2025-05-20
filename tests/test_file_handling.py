import pytest
import pandas as pd
from pathlib import Path
from llm_qc.utils.file_handling import (
    get_sample_column,
    get_project_root,
    get_data_dir,
    read_tsv,
    write_tsv,
    read_sample_set_from_file,
    standardize_sample_ids,
    ensure_directory_exists,
    file_exists,
    merge_dataframes_on_sample,
)

# Fixtures
@pytest.fixture
def sample_df_with_sample_col():
    return pd.DataFrame({'id': [1, 2], 'Sample': ['A', 'B'], 'value': [10, 20]})

@pytest.fixture
def sample_df_no_sample_col():
    return pd.DataFrame({'id': [1, 2], 'other_col': ['A', 'B'], 'value': [10, 20]})

@pytest.fixture
def temp_tsv_file(tmp_path):
    file_path = tmp_path / "test.tsv"
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    df.to_csv(file_path, sep='\t', index=False)
    return file_path

@pytest.fixture
def temp_sample_set_file(tmp_path):
    file_path = tmp_path / "samples.txt"
    with open(file_path, 'w') as f:
        f.write("SampleID\n")
        f.write("SampleA\n")
        f.write("SampleB\n")
        f.write("SampleC\n")
    return file_path

# Tests for get_sample_column
def test_get_sample_column_with_sample(sample_df_with_sample_col):
    assert get_sample_column(sample_df_with_sample_col) == 'Sample'

def test_get_sample_column_no_sample(sample_df_no_sample_col):
    assert get_sample_column(sample_df_no_sample_col) == 'id' # Falls back to first column

# Tests for get_project_root
def test_get_project_root():
    root = get_project_root()
    assert isinstance(root, Path)
    assert (root / 'pyproject.toml').exists() # Check for a known file/dir

# Tests for get_data_dir
def test_get_data_dir_raw():
    data_dir = get_data_dir(raw=True)
    assert isinstance(data_dir, Path)
    assert data_dir.name == 'raw'
    assert (get_project_root() / 'data' / 'raw').exists()

def test_get_data_dir_processed():
    data_dir = get_data_dir(raw=False)
    assert isinstance(data_dir, Path)
    assert data_dir.name == 'processed'
    assert (get_project_root() / 'data' / 'processed').exists()

def test_get_data_dir_with_type():
    data_dir = get_data_dir(data_type="assembly_stats", raw=True)
    assert isinstance(data_dir, Path)
    assert data_dir.name == 'assembly_stats'
    assert data_dir.parent.name == 'raw'
    assert (get_project_root() / 'data' / 'raw' / 'assembly_stats').exists()

# Tests for read_tsv
def test_read_tsv_success(temp_tsv_file):
    df = read_tsv(temp_tsv_file)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['col1', 'col2']
    assert len(df) == 2

def test_read_tsv_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_tsv(tmp_path / "non_existent.tsv")

def test_read_tsv_empty_file(tmp_path):
    file_path = tmp_path / "empty.tsv"
    file_path.touch()
    with pytest.raises(pd.errors.EmptyDataError):
        read_tsv(file_path)

# Tests for write_tsv
def test_write_tsv(tmp_path, sample_df_with_sample_col):
    file_path = tmp_path / "output.tsv"
    write_tsv(sample_df_with_sample_col, file_path)
    assert file_path.exists()
    df_read = pd.read_csv(file_path, sep='\t')
    pd.testing.assert_frame_equal(df_read, sample_df_with_sample_col.reset_index(drop=True))

def test_write_tsv_creates_dir(tmp_path, sample_df_with_sample_col):
    dir_path = tmp_path / "new_dir"
    file_path = dir_path / "output.tsv"
    write_tsv(sample_df_with_sample_col, file_path, create_dir=True)
    assert dir_path.exists()
    assert file_path.exists()

# Tests for read_sample_set_from_file
def test_read_sample_set_from_file(temp_sample_set_file):
    sample_set = read_sample_set_from_file(temp_sample_set_file)
    assert isinstance(sample_set, set)
    assert sample_set == {'SampleA', 'SampleB', 'SampleC'}

def test_read_sample_set_from_file_no_skip_header(tmp_path):
    file_path = tmp_path / "samples_no_header_skip.txt"
    with open(file_path, 'w') as f:
        f.write("SampleX\n")
        f.write("SampleY\n")
    sample_set = read_sample_set_from_file(file_path, skip_header=False)
    assert sample_set == {'SampleX', 'SampleY'}

def test_read_sample_set_from_file_different_column(tmp_path):
    file_path = tmp_path / "samples_col_idx.txt"
    with open(file_path, 'w') as f:
        f.write("OtherID\tSampleID\n")
        f.write("ID1\tSampleZ\n")
    sample_set = read_sample_set_from_file(file_path, column_index=1)
    assert sample_set == {'SampleZ'}

# Tests for standardize_sample_ids
def test_standardize_sample_ids():
    df = pd.DataFrame({'SampleID': [1, 2, '3'], 'value': [10, 20, 30]})
    standardized_df = standardize_sample_ids(df, 'SampleID')
    assert standardized_df['SampleID'].apply(type).eq(str).all()
    assert list(standardized_df['SampleID']) == ['1', '2', '3']

# Tests for ensure_directory_exists
def test_ensure_directory_exists(tmp_path):
    new_dir = tmp_path / "test_dir"
    assert not new_dir.exists()
    ensure_directory_exists(new_dir)
    assert new_dir.exists()
    ensure_directory_exists(new_dir) # Should not fail if exists

# Tests for file_exists
def test_file_exists(temp_tsv_file):
    assert file_exists(temp_tsv_file)
    assert not file_exists(temp_tsv_file.parent / "non_existent_file.txt")

# Tests for merge_dataframes_on_sample
@pytest.fixture
def df1_for_merge():
    return pd.DataFrame({'Sample': ['A', 'B', 'C'], 'data1': [1, 2, 3]})

@pytest.fixture
def df2_for_merge():
    return pd.DataFrame({'SampleID': ['A', 'B', 'D'], 'data2': [10, 20, 40]})

def test_merge_dataframes_on_sample_left_merge(df1_for_merge, df2_for_merge):
    merged_df = merge_dataframes_on_sample(df1_for_merge, df2_for_merge, how='left')
    assert list(merged_df.columns) == ['Sample', 'data1', 'data2']
    assert len(merged_df) == 3
    assert merged_df.loc[merged_df['Sample'] == 'C', 'data2'].isnull().all()
    assert merged_df.loc[merged_df['Sample'] == 'A', 'data2'].iloc[0] == 10

def test_merge_dataframes_on_sample_inner_merge(df1_for_merge, df2_for_merge):
    merged_df = merge_dataframes_on_sample(df1_for_merge, df2_for_merge, how='inner')
    assert len(merged_df) == 2
    assert 'C' not in merged_df['Sample'].values
    assert 'D' not in merged_df['Sample'].values # Because df1 is left, SampleID from df2 is dropped

def test_merge_dataframes_on_sample_different_sample_col_names(df1_for_merge, df2_for_merge):
    # df1 has 'Sample', df2 has 'SampleID'
    merged_df = merge_dataframes_on_sample(df1_for_merge, df2_for_merge, how='inner')
    assert 'Sample' in merged_df.columns
    assert 'SampleID' not in merged_df.columns # df2_sample_col (SampleID) should be dropped
    assert list(merged_df['Sample']) == ['A', 'B']

def test_merge_dataframes_on_sample_keep_duplicate_col(df1_for_merge):
    df2_same_col_name = pd.DataFrame({'Sample': ['A', 'B', 'D'], 'data2': [10, 20, 40]})
    merged_df = merge_dataframes_on_sample(df1_for_merge, df2_same_col_name, how='left', drop_duplicate_sample_col=False)
    # When sample columns have the same name, pandas suffixes them by default (_x, _y)
    # Our function merges on them, so only one 'Sample' column should remain.
    # The drop_duplicate_sample_col flag is for when original names differ.
    assert 'Sample' in merged_df.columns
    assert 'Sample_x' not in merged_df.columns # Should not happen with current logic
    assert 'Sample_y' not in merged_df.columns # Should not happen with current logic

def test_merge_dataframes_on_sample_no_drop_duplicate_col(df1_for_merge, df2_for_merge):
    merged_df = merge_dataframes_on_sample(df1_for_merge, df2_for_merge, how='left', drop_duplicate_sample_col=False)
    assert 'Sample' in merged_df.columns
    assert 'SampleID' in merged_df.columns # SampleID from df2 should be kept
    assert len(merged_df) == 3
