import numpy as np
import pandas as pd

from sklearn.metrics import silhouette_score
from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering

"""
wrapper function to get bin values and bin edges for various binning methods. Call this function with the specified method. 

Inputs:
    x : A numpy array containing the data of the continuous variable. Can have shapes of (N,1) or (N,), each function converts the array to the correct shape
           There is also code that will check if x is a pandas series and convert it to a numpy array.
    n_bins : number of bins to use for binning. 
    method : method to use whe binning 
                    'equal' bins using equal width bins (0-1,1-2,2-3....)
                    'quantile' bins using equal frequency bins (0%-10%, 10%-20%,20%-30%, .....)
                    'KMeans' performs Kmeans clustering using the quantiles as starting clusters. By fixing the starting clusters, the output clusters is always the same 
                    'KMedians' performs Kmeans clustering, but uses median distance instead of mean distance when computing new cluster centers
                    'Agglomerative' performs hierarchical clustering
                    
    get_silhouette_score : boolean variable indicating whether to compute the silhouette score for the resulting clusters
    silhouette_n_samples : number of samples to use for computing the silhouette score. Using many samples can take a long time, but provide more accurate results
                            If None (or not given), uses at most 25,000 samples. Fewer if x has fewer data points
                            
    get_pred_err : boolean variable indicating whether to compute RMSE between actual values in x and the corresponding bin center.
    
Outputs:
    x_bin : bin labels for each entry in x
    bin_edges : edges for the bins. -- bin centers can be found via np.diff(bin_edges)/2+bin_edges[0:-1]
    sil_score : silhouette_score if requested, otherwise None. Computing this can take awhile.
    pred_err : RMSE between actual x and corresponding bin centers, if requested. Otherwise None
"""


def get_bins(x, n_bins=6, method='KMeans', get_silhouette_score=False, silhouette_n_samples=None, get_pred_err=False):
    # method could be KMeans, KMedians, Quantile, Agglomerative , Equal
    x_bin = None
    bin_edges = None
    sil_score = None
    pred_err = None

    # check if input is a series, if it is convert to numpy array
    if isinstance(x, pd.Series):
        x = x.values

    if method.lower() == 'kmeans':
        x_bin, bin_edges = get_kmeans_bins(x, n_bins=n_bins, method='mean')
    elif method.lower() == 'kmedians':
        x_bin, bin_edges = get_kmeans_bins(x, n_bins=n_bins, method='median')
    elif method.lower() == 'agglomerative':
        x_bin, bin_edges = get_agglomerative_bins(x, n_bins=n_bins)
    elif method.lower() == 'quantile':
        x_bin, bin_edges = get_quantile_bins(x, n_bins=n_bins)
    elif method.lower() == 'equal':
        x_bin, bin_edges = get_equal_width_bins(x, n_bins=n_bins)

    if get_silhouette_score:
        if silhouette_n_samples is not None:
            sil_score = get_silhouette_score(x, x_bin,silhouette_n_samples)
        else: # use default value
            sil_score = get_silhouette_score(x, x_bin)

    if get_pred_err:
        pred_err = get_prediction_error(x, x_bin, bin_edges)

    return x_bin, bin_edges, sil_score, pred_err


# 1D k-(means or medians), sklearns kmeans is faster than handwriting,
# kmedians is implemented by hand since sklearn doesn't have that.
# this function wraps around sklearns kmeans. Important things:
# the initial Kmeans cluster means are fixed to the quantile cluster means.
# Beacuse of this, this function will return the same clusters each time it is ran
# the output has the bin labels for each entry in x. Additionally,
# bin edges are outputted instead of bin centers

def get_kmeans_bins(x, n_bins, method='mean', random_state=None):
    # get initial bins using quantile method
    clusters = np.percentile(x, 100*(np.arange(0, n_bins, 1)/n_bins + 1/(2*n_bins)), axis=0).reshape((-1, 1))

    if len(x.shape) == 1:
            x = x.reshape((-1, 1))

    if method == 'median':
        max_iter = 25
        counter = 0
        while counter < max_iter:
            # get labels for each value in x
            dists = (x-clusters.reshape((1, -1)))**2
            label = np.argmin(dists, axis=1)

            # move cluster center
            # use median or mean based on method
            new_clusters = np.zeros_like(clusters)
            for i_clust in range(clusters.shape[0]):
                new_clusters[i_clust] = np.median(x[label == i_clust])

            clusters = new_clusters
            counter = counter + 1

    else:  # means
        # #Use the random_state parameter in KMeans initialization
        #kmeans = (KMeans(n_clusters=n_bins, init=clusters, n_init=1).fit(x))
        kmeans = KMeans(n_clusters=n_bins, init=clusters, n_init=1, random_state=random_state).fit(x)
        x_bin = kmeans.predict(x)
        clusters = np.zeros((n_bins,))
        for i in range(0, n_bins):
            clusters[i] = np.mean(x[x_bin == i])

    # get labels for new clusters
    dists = (x-clusters.reshape((1, -1)))**2
    x_bin = np.argmin(dists, axis=1)

    # convert clust cents to bin edges
    clusters = np.sort(clusters.reshape((-1,)))

    bin_edges = np.zeros((clusters.shape[0] + 1,))
    bin_edges[0] = np.min(x)

    for i in range(1, clusters.shape[0]):
        bin_edges[i] = clusters[i-1] + (clusters[i]-clusters[i-1])/2
    bin_edges[-1] = np.max(x)

    # output
    return x_bin, bin_edges


# quantile binning
def get_quantile_bins(x, n_bins):
    if len(x.shape) > 1:
        x = x.reshape((-1,))

    bin_edges = np.percentile(x, 100*np.arange(0, n_bins+1, 1)/n_bins)
    x_bin = np.digitize(x,bin_edges) # 1 is the first bin
    x_bin = x_bin-1
    # check bounds
    x_bin[x_bin > n_bins-1] = n_bins-1
    x_bin[x_bin < 0] = 0

    return x_bin, bin_edges


"""

"""


def get_prediction_error(x, x_bin, bin_edges):
    bin_centers = np.diff(bin_edges)/2+bin_edges[0:-1]

    if len(x.shape)>1:
        x = x.reshape((-1,))
    rmse = np.sqrt(np.sum((x-bin_centers[x_bin])**2)/x.shape[0])

    return rmse


"""

"""


def get_silhouette_score(x, x_bin, n_samp=25000,  random_state=None):
    # silhouette score wrapper
    # this uses the mean intra cluster distance and the mean nearest-cluster distance for each sample
    # (b-a)/max(a,b) : b is the distance between a sample and the nearest cluster that that sample is not a part of
    # a is the distance between a sample and its cluster center

    # best value is 1, worst is -1
    if len(x.shape) == 1:
        x = x.reshape(-1, 1)

    if n_samp > x.shape[0]:
        sil_score = silhouette_score(x, x_bin, random_state=random_state)  # use all the data
    else:
        sil_score = silhouette_score(x, x_bin, sample_size=n_samp, random_state=random_state)  # sample data

    return sil_score


"""

"""
# equal width bins


def get_equal_width_bins(x, n_bins=10, bin_edges=None):
    if len(x.shape) > 1:
        x = x.reshape((-1,))

    if bin_edges is None:
        bin_width = np.ptp(x)/n_bins  # ptp does np.max(x) - np.min(x)
        bin_edges = np.arange(np.min(x), np.max(x)+bin_width, bin_width)
        x_bin = np.digitize(x, bins=bin_edges)-1  # 1 is for the first bin, subtract 1 then check bounds
    else:
        x_bin = np.digitze(x, bins=bin_edges)

    x_bin = x_bin-1
    # if x_bin < 0, left of first edge, set to 0
    # if x_bin > n_bins-1, right of last edge, set to n_bins-1
    x_bin[x_bin < 0] = 0
    x_bin[x_bin > n_bins-1] = n_bins-1

    return x_bin, bin_edges


"""

"""
# hierarchical clustering -- very memory intensive. Take random sample of input data to build bin edges


def get_agglomerative_bins(x, n_bins, random_state=None):
    max_select = 10000
    if len(x.shape) > 1:
        x = x.reshape((-1,))
    #x_samp = np.random.choice(x, size=(np.minimum(max_select, x.shape[0]),), replace=False)
    # Create a numpy random number generator
    rng = np.random.default_rng(random_state)
    x_samp = rng.choice(x, size=np.minimum(max_select, x.shape[0]), replace=False)
    agg_clust = AgglomerativeClustering(n_clusters=n_bins).fit(x_samp.reshape((-1,1)))

    x_bin_samp = agg_clust.labels_
    max_vals = np.zeros((n_bins,))
    min_vals = np.zeros_like(max_vals)

    for i in range(n_bins):
        max_vals[i] = np.max(x_samp[x_bin_samp == i])
        min_vals[i] = np.min(x_samp[x_bin_samp == i])

    max_vals = np.sort(max_vals)
    min_vals = np.sort(min_vals)

    bin_edges = np.zeros((n_bins+1,))
    bin_edges[0] = np.min(x)-0.00001
    bin_edges[-1] = np.max(x)+0.00001
    for i in range(1,n_bins):
        bin_edges[i] = np.mean([max_vals[i-1], min_vals[i]])

    x_bin = np.digitize(x, bins=bin_edges) # 1 corresponds to first bin, subtract all by 1
    x_bin = x_bin-1
    # if x_bin < 0, left of first edge, set to 0
    # if x_bin > n_bins-1, right of last edge, set to n_bins-1
    x_bin[x_bin < 0] = 0
    x_bin[x_bin > n_bins-1] = n_bins-1

    return x_bin, bin_edges
