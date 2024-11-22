import numpy as np
from numpy.testing import assert_array_equal
from libs.binning_algorithms import get_bins, get_prediction_error, get_silhouette_score


def test_get_bins():
    x = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    x_bin, bin_edges, sil_score, pred_err = get_bins(x, 6, 'kmeans')
    assert_array_equal(x_bin, np.array([0, 0, 1, 1, 2, 3, 3, 4, 5, 5]))

    x_bin, bin_edges, sil_score, pred_err = get_bins(x, 6, 'kmedians')
    assert_array_equal(x_bin, np.array([0, 0, 1, 1, 2, 3, 3, 4, 5, 5]))

    x_bin, bin_edges, sil_score, pred_err = get_bins(x, 6, 'agglomerative')
    assert len(x_bin) == 10

    x_bin, bin_edges, sil_score, pred_err = get_bins(x, 6, 'quantile')
    assert len(x_bin) == 10

    x_bin, bin_edges, sil_score, pred_err = get_bins(x, 6, 'equal')
    assert len(x_bin) == 10


def test_get_prediction_error():
    x = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    x_bean = np.array([0, 0, 1, 1, 2, 3, 3, 4, 5, 5])
    bin_edges = np.array([1., 2.5, 4.25, 5.75, 7.25, 8.75, 10.])
    rmse = get_prediction_error(x, x_bean, bin_edges)
    assert np.isclose(rmse, 0.46770717334674267, rtol=1e-09, atol=1e-09)


def test_get_silhouette_score():
    x = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    x_bean = np.array([0, 0, 1, 1, 2, 3, 3, 4, 5, 5])
    sil_score = get_silhouette_score(x, x_bean)
    assert np.isclose(sil_score, 0.17666666666666667, rtol=1e-09, atol=1e-09)
