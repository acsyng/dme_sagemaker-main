"""
This function computes the genetic relationship matrix (GRM) from two tabular text datasets. Each of the datasets have inputs columns that include sample, variant_id, and genotype.

Then principal component analysis (PCA) is performed on the GRM. The number of components is user defined. Default is 20.

This function returns the following:
    pca_output_df --  output of PCA to be used to transform new data after calculating GRM and scaling
    train_variant_id -- variant ids present in the dataset when the GRM was calculated. To be referenced as data preprocessing set on new data
    scaler_vals - mean and variance of standarized data. To be referenced when scaling new data
    train_samples -- samples present in the sample dataset when the GRM was calculated. To be referenced as data preprocessing set on new data
"""


import pandas as pd
import numpy as np
import math as math
import os
import pyarrow.parquet as pq
from sklearn.decomposition import IncrementalPCA
import multiprocessing
from multiprocessing import Pool
from functools import partial
from libs.placement_lib.geno_queries import get_snp_data
from libs.placement_lib.lgbm_utils import save_object, generate_file_identifier

# Function to process sample dataset to be used in "vs" method to create grm and pca. Inputs include sample dataset.


def samp_comp_dataset_preprocess(dataF):
    # dataF['variant_id'] = list(map(str, dataF['variant_id']))
    # dataF['variant_id'] = dataF['variant_id'].astype(str)

    dataF_var_id = dataF['variant_id'].drop_duplicates()

    dataF_pivot = dataF.pivot_table(index='sample_id', columns='variant_id', values='geno_index').reset_index()
    dataF_samp = dataF_pivot['sample_id'].tolist()
    dataF_arr = dataF_pivot.iloc[:, 1:].to_numpy(dtype=np.int8)
    dataF_arr = np.nan_to_num(dataF_arr, nan=-15)

    # dataF_samp = dataF['sample'].unique()
    # dataF_sub = dataF.reindex(pd.MultiIndex.from_product([dataF_samp, dataF_var_id], names=['sample', 'variant_id']),fill_value=-15)
    # dataF_arr = np.reshape(dataF_sub['geno_index'].to_numpy(),(-1,dataF_samp.shape[0])).T

    return dataF_arr, dataF_var_id, dataF_samp

# Function to process second dataset to be used in "vs" method to create grm and pca. Inputs include new array and variant ids from sample array


def new_dataset_preprocess(new_dataF, var_id_to_include):
    # new_dataF['variant_id'] = list(map(str, new_dataF['variant_id']))
    # new_dataF['variant_id'] = new_dataF['variant_id'].astype(str)

    new_dataF_samples = new_dataF['sample_id'].unique()

    new_dataF_sub = new_dataF[new_dataF['variant_id'].isin(var_id_to_include)]

    new_dataF_pivot = new_dataF_sub.pivot_table(index='sample_id', columns='variant_id', values='geno_index').reindex(columns=var_id_to_include, fill_value=15).reset_index()
    new_dataF_arr = new_dataF_pivot.iloc[:, 1:].to_numpy(dtype=np.int8, na_value=15)

    # new_dataF_arr = new_dataF_pivot_arr[:,1:]
    # new_dataF_samples = new_dataF_pivot_arr[:,0]
    # new_dataF_arr1 = new_dataF_arr.astype(float)
    # new_dataF_arr_nan = np.nan_to_num(new_dataF_arr1, nan=-2)

    # new_dataF_sub = new_dataF.reindex(pd.MultiIndex.from_product([new_dataF_samples, var_id_to_include], names=['sample', 'variant_id']))
    # new_dataF_arr = np.reshape(new_dataF_sub['geno_index'].to_numpy(),(-1,new_dataF_samples.shape[0])).T

    return new_dataF_arr, new_dataF_samples


####################################################################################################
"""
compute_grm_between_two_dfs

Function to compute GRM between two datasets. Inputs include sample array and new array

Inputs:
    new_array: a 1-by-n_variants slice of a 2D array
    samp_array: a n_samples-by-n_variants 2D array

Outputs:
    A 1-by-n_samples (in samp_array) array describing shared allele similarity between new_array and each sample in samp_array.

Last update: Keri Rehm, 2023-Jun-13
"""
####################################################################################################


def compute_grm_between_two_dfs(new_array, samp_array):
    # t_0 = timeit.default_timer()

    arr_diff = samp_array - new_array

    matches_1 = np.count_nonzero(arr_diff == 0, axis=1).astype(np.uint16)
    m = np.maximum(np.count_nonzero(arr_diff > -10, axis=1), 1, dtype=np.int32)

    # t_1 = timeit.default_timer()
    # elapsed_time = round((t_1 - t_0), 3)
    # print(f"Total Elapsed time: {elapsed_time} s")

    return matches_1 / m


####################################################################################################
"""
compute_grm_parallel

Compute GRM after pivot via parallel processing. Return GRM.

Inputs:
    samp_array: a n_benchmark_samples-by-n_variants 2D array
    new_array: a n_samples-by-n_variants 2D array
    samp_array_samples: List of sample ID's corresponding to samp_array
    new_array_samples: List of sample ID's corresponding to new_array

Outputs:
    grm_df: a n_samples-by-n_benchmark_samples dataframe of genetic similarities

Last update: Keri Rehm, 2023-Jun-13
"""


####################################################################################################
def compute_grm_parallel(samp_array,
                         new_array,
                         samp_arr_samples,
                         new_array_samples):

    n_count = multiprocessing.cpu_count() - 1
    with Pool(processes=n_count) as pool:
        grm_between = pool.map(partial(compute_grm_between_two_dfs, samp_array=samp_array), [new_array[i:i+1, :] for i in range(0, len(new_array_samples), 1)])

    grm_between = np.round(np.stack(grm_between, axis=0), 8)
    grm_df = pd.DataFrame(grm_between, columns=samp_arr_samples, index=new_array_samples).reset_index().rename(columns={'index': 'sample_id'})

    return grm_df


####################################################################################################
"""
get_snp_data_transform

Steps included:
    1. Calls get_snp_data (in library file geno_queries) to get SNP data for samples in sample_df
    2. Perform translation from marker_id to variant_id as well as filtering to used variant_id's to save on memory.
    3. Calculates GRM between samples of interest and the benchmark/comparison set SNPs
    4. Returns a sample x bench-sample GRM df, with year appended.

Inputs:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    sample_df: df containing at least the names of the samples to pull in a column named "sample"
    variant_set_df: df containing at least "marker_id" and "variant_id" mapping
    bench_df: Comparison sample set, rows = samples, cols = variant_ids
    year: trial year associated with the samples in sample_df

Output:
    output_df: sample x bench-sample GRM df, with year appended.

Last update: Keri Rehm, 2023-Jun-07
"""
####################################################################################################


def get_snp_data_transform(sample_df, variant_id_df, bench_df, year, crop_id_numeric=9):

    # print(query_str)
    print("Executing query")
    hetpool_iter_df = get_snp_data(sample_df, crop_id_numeric)
    print("Join and pivot")
    hetpool_iter_df = hetpool_iter_df.merge(variant_id_df, on="marker_id", how="inner").drop(columns=["marker_id"]).reset_index(drop=True).groupby(["sample_id", "variant_id"]).first()
    print(hetpool_iter_df.head())
    print(hetpool_iter_df.index.unique('variant_id').shape)
    hetpool_iter_df = hetpool_iter_df.unstack(fill_value=15)
    hetpool_iter_df.columns = hetpool_iter_df.columns.droplevel(0)
    hetpool_iter_df = hetpool_iter_df.rename_axis(None, axis=1)

    print("Computing GRM")
    het_pool_df = compute_grm_parallel(bench_df.reindex(columns=hetpool_iter_df.columns.astype(str), fill_value=-15).to_numpy(dtype='int8', na_value=-15),
                                       hetpool_iter_df.to_numpy(dtype=np.int8, na_value=15),
                                       bench_df['sample_id'].tolist(),
                                       hetpool_iter_df.index.to_numpy())
    # print(het_pool_df.head())
    het_pool_df["year"] = year

    return het_pool_df


####################################################################################################
"""
get_snp_data_transform_batch

Calls get_snp_data_transform in batches and returns the GRM values for all batches in one df

Inputs:
    executor: SQLExecutor2 object specifying connection information for Denodo managed
    sample_df: df containing at least the names of the samples to pull in a column named "sample"
    variant_set_df: df containing at least "marker_id" and "variant_id" mapping
    bench_df: Comparison sample set, rows = samples, cols = variant_ids
    year: trial year associated with the samples in sample_df
    batch_size: number of samples to pull + process in a batch (optional)

Output:
    output_df: sample x bench-sample GRM df, with year appended, for all batches

Last update: Keri Rehm, 2023-Jun-07
"""
####################################################################################################


def get_snp_data_transform_batch(sample_df, variant_id_df, bench_df, year, batch_size=1000, crop_id_numeric=9):

    sample_n = sample_df.shape[0]
    sample_n_iter = int(math.ceil(sample_n/batch_size))

    output_list = []
    for i in range(sample_n_iter):

        het_pool_df = get_snp_data_transform(sample_df.iloc[i*batch_size:min((i+1)*batch_size-1, sample_n), :], variant_id_df, bench_df, year, crop_id_numeric)

        output_list.append(het_pool_df)

    output_df = pd.concat(output_list)

    return output_df

####################################################################################################


"""
compute_ipca

Computes the incremental PCA transform based on years prior to the analysis year, and then applies 
the model to all relevant years of data.

Inputs:
    dataset: Dataiku dataset object of GRM (not df!)
    analysis_year: Holdout test year
    n_comp: Number of components to calculate
    chunk_size: Number of rows of GRM to fit/predict on at once
    
Output:
    output_df: A df of the components, size n_samp-by-n_comp
    
Last updated: Keri Rehm, 2023-Jun-13
"""
####################################################################################################


def compute_ipca(s3_parquet_path, analysis_year, n_comp=20, chunk_size=3000, label_name='sample_id', dir_path=None):
    ipca = IncrementalPCA(n_components=n_comp, batch_size=chunk_size)

    dataset = pq.ParquetDataset(s3_parquet_path).files[0]
    print('dataset: ', os.path.join('s3://', dataset))
    parquet_file = pq.ParquetFile(os.path.join('s3://', dataset))
    for df in parquet_file.iter_batches(batch_size=chunk_size):
        df = df.to_pandas()
        print('df shape: ', df.shape)
        df["year"] = df["year"].astype('int')
        df = df.loc[df["year"] < int(analysis_year), ].drop(["year"], axis=1)
        print('df shape after filtering: ', df.shape)
        print('df info')
        df.info()
        print('max: ', df.max())

        print('nas', df.isna().sum().sum())
        print('end df info and show na by column')
        print('nas', df.isna().sum())

        if df.shape[0] > n_comp:
            ipca.partial_fit(df.drop([label_name], axis=1))

    print("IPCA fit")
    print(np.cumsum(ipca.explained_variance_ratio_[0:n_comp]))

    file_identifier = generate_file_identifier()

    model_file_name = "ipca_model_" + file_identifier + '.pkl'

    save_object(ipca, dir_path, model_file_name)

    pca_list = []
    for df in parquet_file.iter_batches(batch_size=chunk_size):
        df = df.to_pandas()
        df_labels = df[[label_name, "year"]]
        df["year"] = df["year"].astype('int')
        df = df.loc[df["year"] <= int(analysis_year), ]
        if df.shape[0] > 0:
            pca_df = ipca.transform(df.drop([label_name, 'year'], axis=1))
            pca_df = pd.concat([df_labels.reset_index(drop=True),
                                pd.DataFrame(pca_df.round(8)).add_prefix('comp_')], axis=1)
            pca_list.append(pca_df)

    output_df = pd.concat(pca_list).reset_index(drop=True).drop(["year"], axis=1).drop_duplicates(subset=[label_name], ignore_index=True)

    return output_df


####################################################################################################
"""
compute_pca

Compute the PCA of a GRM and return a formatted df of results plus the PCA model.
Not used since input data size necessitates usage of Incremental PCA.
"""
####################################################################################################
"""
def compute_pca(grm_df,
                n_comp=20):
    
    # Make an instance of the Model, fit, and format transformed output. Return transformed output and fitted model
    pca = PCA(n_components=n_comp)
    pca_output1 = pca.fit_transform(grm_df.drop(['sample'], axis = 1))
    pca_output1_df = pd.concat([pd.DataFrame(grm_df['sample'].drop_duplicates(), columns = ['sample']),
                               pd.DataFrame(pca_output1).add_prefix('comp_')], axis = 1)
    
    #pca_components_df = pd.DataFrame(pca.components_.T).add_prefix('comp_')
    
    return pca_output1_df, pca
"""

####################################################################################################
"""
get_transform_snps

Not sure if this is currently used
"""
####################################################################################################
"""
def get_transform_snps(executor, het_pool_df, samp_arr, variant_ids, samp_arr_samples, batch_size, year):
    het_pool_df = get_snp_data(executor, het_pool_df, variant_ids, batch_size)
    # print(het_pool_df.head())
    het_pool_arr, het_pool_samples = new_dataset_preprocess(het_pool_df, variant_ids)
    het_pool_df = compute_grm_parallel(samp_arr,
                 het_pool_arr,
                 samp_arr_samples,
                 het_pool_samples)
    het_pool_df["year"] = year
    
    return het_pool_df
"""


####################################################################################################
"""
Function to compute PCA on GRM. Inputs include sample array, new array, samples in sample array, 
variant ids in sample array, and number of components. 
# KR comment: This function has been broken out a little differently in other functions to better 
work in Dataiku - now GRM calculation is paired with SNP retrieval to save on storage, and PCA is done later.
"""
####################################################################################################
"""
def grm_with_pca(samp_array,
                 new_array,
                 samp_arr_samples,
                 new_array_samples,
                 n_comp=20):

    # Compute genetic relationship matrix on training data
    grm_between = np.apply_along_axis(compute_grm_between_two_dfs, 1, new_array, samp_array)
    grm_df = pd.DataFrame(grm_between, columns = samp_arr_samples)
    
    #scaler = StandardScaler()
    #grm_df_t = scaler.fit_transform(grm_df)
    
    #scaler_vals = pd.DataFrame([scaler.mean_,scaler.var_]).T
    #scaler_vals.columns = ['mean','variance']
    
    # Make an instance of the Model
    pca = PCA(n_components=n_comp)
    #grm_df_t1 = pca.fit_transform(grm_df_t)
    pca_output1 = pca.fit_transform(grm_df)
    
    pca_output1_df = pd.concat([pd.DataFrame(new_array_samples, columns = ['sample']),
                               pd.DataFrame(pca_output1).add_prefix('comp_')], axis = 1)
    
    pca_components_df = pd.DataFrame(pca.components_.T).add_prefix('comp_')
    
    return pca_output1_df, pca
"""
