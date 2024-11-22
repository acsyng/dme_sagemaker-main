import multiprocessing

from multiprocessing import Pool
from sklearn.preprocessing import PolynomialFeatures
import statsmodels.api as sma
from statsmodels.stats.outliers_influence import OLSInfluence
from sklearn.linear_model import LinearRegression, HuberRegressor
from sklearn.metrics import explained_variance_score
from scipy import stats as ss
import pandas as pd
import numpy as np
from functools import partial


def data_prepare(dt, gr_cols):
    """
    data_prepare function prepares the original regression dataset to be used for reg_adjust_parallel/reg_adjust_parallel_rm by adding regression_id, and
    other columns to store the information generated during adjustment, such as p-value and slope;
    regression_id is a combination of: 'analysis_year', 'ap_data_sector' ,'analysis_type' +'market_seg', 'source_id', 'x', and 'y';
    input dt is the original input file for adjustment;
    output 1 is similar with dt but with several additonal columns, and output 2 is a list of unique regression_ids
    """
    dt["regression_id"] = dt.loc[:, gr_cols].astype(str).agg("_".join, axis=1)
    ids = list(set(dt["regression_id"].unique()))
    dt["adjusted_prediction"] = 0
    dt["adj_model"] = "None"
    dt["adj_outlier"] = "None"
    dt["p_value"] = 0
    dt["slope1"] = 0
    dt["slope2"] = 0
    dt["intercept"] = 0

    return dt, ids


def post_processing(results, alpha):
    """
    post_processing processes the output of reg_adjust_parallel/reg_adjust_parallel_rm to
    1) remove the adjustment if the model used for adjustment is not significant at alpha level, and
    2) add additonal column 'adjusted' to indicate whether the adjustment is conduted or not

    input results is the output of reg_adjust_parallel/reg_adjust_parallel_rm;
    input alpha is a float to indicate the significant alpha level;
    output is a similar dataframe as input but with additional column called 'adjusted'
    """
    results.loc[results["p_value"] > alpha, "adjusted_prediction"] = results.loc[
        results["p_value"] > alpha, "prediction"
    ]
    results["residual"] = results["adjusted_prediction"] - results["prediction"]
    results["adjusted"] = "Yes"
    results.loc[results["p_value"] > alpha, "adjusted"] = "No"
    results.loc[results["adj_outlier"] == "True", "adjusted"] = "outliers"
    results.loc[(results["slope1"].isna()) & (results["slope2"].isna()), "adjusted"] = (
        "No"
    )

    return results


def reg_adjust_parallel(regression_id, dt):
    """
    reg_adjust_parallel is for the old version adjustment, and it conducts adjustment for the given regression_id;
    the function fit 3 models, i.e. linear, quadratic, and logarithmic models, and chooses the valid model with the highest adjusted r-square as the final model;
    the function will choose linear model if all models are invalid, but the adjustment won't be conducted as the p-value of the un-valid linear regression is set to 2;
    input dt is the whole input data;
    input regression_id is a string, representing a unique decision group for trait adjsutment;
    output is the data frame for the given regression_id with adjusted y values
    """

    dt_regression_id = dt.loc[dt["regression_id"] == regression_id]
    dt_regression_id.index = range(dt_regression_id.shape[0])
    x, y = dt_regression_id[["prediction_x"]], dt_regression_id[["prediction"]]
    alpha = 0.3

    if dt_regression_id["prediction"].nunique() > 2:

        quadratic_fit = "Yes"
        n_data = dt_regression_id.shape[0]

        x2 = pd.DataFrame(
            PolynomialFeatures(
                degree=2, interaction_only=True, include_bias=False
            ).fit_transform(x)
        )
        x2 = x2.iloc[:, 1:3].copy()
        x3 = np.log(x).copy()

        if n_data >= 500:
            thr_ot = 6 / np.sqrt(n_data)
        else:
            thr_ot = 4 / np.sqrt(n_data)

        x1 = sma.add_constant(x)
        x3 = sma.add_constant(x3)

        ft1a = sma.OLS(y, x1).fit()
        ft3a = sma.OLS(y, x3).fit()

        outlier1 = pd.DataFrame(OLSInfluence(ft1a).dfbetas)
        outlier3 = pd.DataFrame(OLSInfluence(ft3a).dfbetas)
        outlier1 = outlier1[[1]]
        outlier3 = outlier3[[1]]
        outlier1.index = outlier3.index = dt_regression_id.index
        x1b, y1b = x1.loc[abs(outlier1[1]) < thr_ot], y.loc[abs(outlier1[1]) < thr_ot]
        x3b, y3b = x3.loc[abs(outlier3[1]) < thr_ot], y.loc[abs(outlier3[1]) < thr_ot]

        ft1 = sma.OLS(y1b, x1b).fit()
        ft3 = sma.OLS(y3b, x3b).fit()
        adj_r1 = ft1.rsquared_adj
        adj_r3 = ft3.rsquared_adj

        p_value1 = ft1.f_pvalue
        p_value3 = ft3.f_pvalue

        # turn off linear adjustment if slope is negative
        if ft1.params[1] < 0:
            p_value1 = 2
            adj_r1 = -1

        nd1 = x2[2].nunique()
        if nd1 >= 6:
            x2 = sma.add_constant(x2)
            ft2a = sma.OLS(y, x2).fit()
            outlier2 = pd.DataFrame(OLSInfluence(ft2a).dfbetas)
            outlier2 = outlier2[[2]]
            outlier2.index = dt_regression_id.index
            x2b, y2b = (
                x2.loc[abs(outlier2[2]) < thr_ot],
                y.loc[abs(outlier2[2]) < thr_ot],
            )
            nd2 = x2b[2].nunique()

            if nd2 >= 6:
                ft2 = sma.OLS(y2b, x2b).fit()
                a = ft2.params[2]
                b = ft2.params[1]
                max_x = -(b / (2 * a))
                x2c = x2.copy()
                x2c[1] = x2c[1].clip(upper=max_x)
                x2c[2] = x2c[2].clip(upper=max_x**2)
                adj_r2 = ft2.rsquared_adj
                p_value2 = ft2.f_pvalue

                if (max_x > (max(dt_regression_id["prediction_x"]) + 10)) | (
                    max_x < (min(dt_regression_id["prediction_x"]) - 10)
                ):
                    quadratic_fit = "No"
                    ft2 = "None"
                if (ft2 != "None") & ((p_value2 > alpha) | (ft2.params[2] > 0)):
                    quadratic_fit = "No"
                    ft2 = "None"

            else:
                quadratic_fit = "No"
                ft2 = "None"
        else:
            quadratic_fit = "No"
            ft2 = "None"

        if ft3.params[1] < 0:

            if quadratic_fit == "Yes":

                if adj_r2 == max(adj_r1, adj_r2):
                    p_value = p_value2
                    model = ft2
                else:
                    p_value = p_value1
                    model = ft1

            else:
                p_value = p_value1
                model = ft1

        elif quadratic_fit == "No":
            if adj_r1 == max(adj_r1, adj_r3):
                p_value = p_value1
                model = ft1
            else:
                p_value = p_value3
                model = ft3

        else:

            if adj_r2 == max(adj_r1, adj_r2, adj_r3):
                p_value = p_value2
                model = ft2
            if adj_r3 == max(adj_r1, adj_r2, adj_r3):
                p_value = p_value3
                model = ft3
            if adj_r1 == max(adj_r1, adj_r2, adj_r3):
                p_value = p_value1
                model = ft1

        if model == ft1:
            y_pred = model.predict(x1)
            adjust_model = "linear"
            outlier = outlier1
            slope1 = ft1.params[1]
            slope2 = 0.0
            intercept = ft1.params["const"]
            p_value = p_value1

        elif model == ft2:
            y_pred = model.predict(x2c)
            adjust_model = "quadratic"
            outlier = outlier2
            slope1 = ft2.params[1]
            slope2 = ft2.params[2]
            intercept = ft2.params["const"]
            p_value = p_value2

        elif model == ft3:
            y_pred = model.predict(x3)
            adjust_model = "log"
            outlier = outlier3
            slope1 = ft3.params[1]
            slope2 = 0.0
            intercept = ft3.params["const"]
            p_value = p_value3

        else:
            adjust_model = "NONE"

    else:
        adjust_model = "NONE"

    if adjust_model == "NONE":
        y_adj = y
        y_adj["adj_model"] = adjust_model
        y_adj["adj_outlier"] = "False"
        y_adj["p_value"] = np.nan
        y_adj["slope1"] = np.nan
        y_adj["slope2"] = np.nan
        y_adj["intercept"] = np.nan
        y_adj["residual"] = np.nan

    else:
        outlier.columns = ["outlier"]
        y_adj = pd.DataFrame(np.ravel(y) - np.ravel(y_pred) + y.mean()[0])
        y_adj["adj_model"] = adjust_model
        y_adj["adj_outlier"] = "False"
        y_adj["adj_outlier"].loc[abs(outlier["outlier"]) >= thr_ot] = "True"
        y_adj["p_value"] = p_value
        y_adj["slope1"] = slope1
        y_adj["slope2"] = slope2
        y_adj["intercept"] = intercept
        y_adj["residual"] = pd.DataFrame(np.ravel(y) - np.ravel(y_pred))

    y_adj.columns = [
        "adjusted_prediction",
        "adj_model",
        "adj_outlier",
        "p_value",
        "slope1",
        "slope2",
        "intercept",
        "residual",
    ]
    y_adj.index = dt.loc[dt["regression_id"] == regression_id].index
    dt["adjusted_prediction"].loc[dt["regression_id"] == regression_id] = y_adj[
        "adjusted_prediction"
    ]
    dt["adj_model"].loc[dt["regression_id"] == regression_id] = y_adj["adj_model"]
    dt["adj_outlier"].loc[dt["regression_id"] == regression_id] = y_adj["adj_outlier"]
    dt["p_value"].loc[dt["regression_id"] == regression_id] = y_adj["p_value"]
    dt["slope1"].loc[dt["regression_id"] == regression_id] = y_adj["slope1"]
    dt["slope2"].loc[dt["regression_id"] == regression_id] = y_adj["slope2"]
    dt["intercept"].loc[dt["regression_id"] == regression_id] = y_adj["intercept"]
    dt_regression_id_adj = dt.loc[dt["regression_id"] == regression_id]

    return dt_regression_id_adj


def reg_adjust_parallel_rm(regression_id, dt):
    """
    reg_adjust_parallel_rm is for the new version adjustment with robust model, and it conducts adjustment for the given regression_id;
    the function fit 3 models, i.e. linear, quadratic, and logarithmic models, and chooses the valid model with the highest adjusted r-square as the final model;
    the function will choose linear model if all models are invalid, but the adjustment won't be conducted as the p-value of the un-valid linear regression is set to 2;
    input dt is the whole input data;
    input regression_id is a string, representing a unique decision group for trait adjsutment,
    and regression_id is a combination of: 'analysis_year', 'ap_data_sector' ,'analysis_type' +'market_seg', 'source_id', 'x', and 'y';
    output is the data frame for the given regression_id with adjusted y values
    """

    # subset the whole dataset using regression_id to only include data for 1 regression_id
    dt_regression_id = dt.loc[dt["regression_id"] == regression_id]
    dt_regression_id.index = range(dt_regression_id.shape[0])

    # get dataframe for x and y
    x, y = (
        dt_regression_id[["prediction_x"]],
        dt_regression_id[["prediction"]].to_numpy().ravel(),
    )

    # set paramters for the huber regression
    ep = 1.65
    alpha2 = 0
    max_iter = 100

    # fit different regression models only if the unique y value (i.e. prediction) is more than 2, else don't conduct adjustment
    if dt_regression_id["prediction"].nunique() > 9:
        """Section 1: parepare data, fit model, and save p-value and adjusted r-square of models
        For each option:
         - Create the features
         - Fit huber regression for linear (ft1), quadratic (ft2), and logarithmic model (ft3)
         - Get the fitted y value from each model
         - Get the r-square of each fitted model
         - Get the adjusted r-square of each fitted model
         - Get the p_value of f-test of each model
        """

        # Linear
        ft1 = HuberRegressor(
            epsilon=ep,
            max_iter=max_iter,
            alpha=alpha2,
            warm_start=False,
            fit_intercept=True,
            tol=1e-05,
        ).fit(x, y)
        if (
            ft1.coef_[0] < 0
        ):  # turn off the linear regression if the slope <0 by setting p_value of the model to 2 and adjusted r-square to -1
            p_value1 = 2
            adj_r1 = -1
        else:
            y1_pred = ft1.predict(x)
            r1 = explained_variance_score(y, y1_pred)
            adj_r1 = cpt_adj_r2(r1, x)
            p_value1 = f_test(x, y, ft1)

        # Quadratic
        x2 = pd.DataFrame(
            PolynomialFeatures(
                degree=2, interaction_only=True, include_bias=False
            ).fit_transform(np.reshape(x, (-1, 1)))
        )
        x2 = x2.iloc[:, 1:3]
        nd1 = x2[2].nunique()
        ft2 = HuberRegressor(
            epsilon=ep,
            max_iter=max_iter,
            alpha=alpha2,
            warm_start=False,
            fit_intercept=True,
            tol=1e-05,
        ).fit(x2, y)
        if (nd1 < 6) | (ft2.coef_[1] > 0):
            ft2 = "None"
            p_value2 = 2
            adj_r2 = -1
        else:
            # if quadratic fit is valid, save the model paramters
            y2_pred = ft2.predict(x2)
            r2 = explained_variance_score(y, y2_pred)
            adj_r2 = cpt_adj_r2(r2, x2)
            p_value2 = f_test(x2, y, ft2)
            # flat the curve after peak is reached by setting the maximum x value of
            # data to the value where the peak is, this converted x is only used
            # internally for adjustment and not written to the output by creating x2c
            # variable
            max_x = -(
                ft2.coef_[0] / (2 * ft2.coef_[1])
            )  # peak is defined by max_x = -(b/(2*a))
            x2[1] = x2[1].clip(upper=max_x)
            x2[2] = x2[2].clip(upper=max_x**2)

        # Logarithmic option
        if np.min(x.values) > 0:
            x3 = np.log(x.copy())
            ft3 = HuberRegressor(
                epsilon=ep,
                max_iter=max_iter,
                alpha=alpha2,
                warm_start=False,
                fit_intercept=True,
                tol=1e-05,
            ).fit(x3, y)
            y3_pred = ft3.predict(x3)
            r3 = explained_variance_score(y, y3_pred)
            adj_r3 = cpt_adj_r2(r3, x3)
            p_value3 = f_test(x3, y, ft3)

            if (
                ft3.coef_[0] < 0
            ):  # turn off the logarithmic regression if the slope is < 0,
                ft3 = None
                p_value3 = 2
                adj_r3 = -1
        else:
            ft3 = None
            p_value3 = 2
            adj_r3 = -1

        # Section 2:  choose model based on 1) adjusted r-square and
        # 2) whether the model is valid or not
        # linear model will be selected if quadratic and logarithmic model are both invalid.
        # If the selected linear model is also invalid the adjustment wouldn't be conducted
        # as the p-value of the invalid linear model is 2
        # get the predicted/fitted y-value from the selected model, and save paramters and p-value of the selected model

        max_r = max(adj_r1, adj_r2, adj_r3)
        if max_r == -1:
            adjust_model = "NONE"
        elif adj_r1 == max_r:
            p_value = p_value1
            y_pred = ft1.predict(x)
            adjust_model = "linear"
            outlier = ft1.outliers_
            slope1 = ft1.coef_[0]
            slope2 = 0
            intercept = ft1.intercept_
        elif adj_r2 == max_r:
            p_value = p_value2
            y_pred = ft2.predict(x2)
            adjust_model = "quadratic"
            outlier = ft2.outliers_
            slope1 = ft2.coef_[0]
            slope2 = ft2.coef_[1]
            intercept = ft2.intercept_
        elif adj_r3 == max_r:
            p_value = p_value3
            y_pred = ft3.predict(x3)
            adjust_model = "log"
            outlier = ft3.outliers_
            slope1 = ft3.coef_[0]
            slope2 = 0
            intercept = ft3.intercept_
        else:
            adjust_model = "NONE"
    else:
        adjust_model = "NONE"

    # Section 4:  prepare data related to adjustment model to be added in the original data

    # use original y value  if no model is selected due to insufficient data size #
    if adjust_model == "NONE":
        y_adj = pd.DataFrame(
            index=dt.loc[dt["regression_id"] == regression_id].index,
            columns=[
                "adjusted_prediction",
                "adj_model",
                "adj_outlier",
                "p_value",
                "slope1",
                "slope2",
                "intercept",
                "residual",
            ],
        )
        y_adj["adjusted_prediction"] = y
        y_adj["adj_model"] = adjust_model
        y_adj["adj_outlier"] = "False"
        y_adj["p_value"] = None
        y_adj["slope1"] = None
        y_adj["slope2"] = None
        y_adj["intercept"] = None
        y_adj["residual"] = None

    # if adjustment is conducted, calculate adjusted_y = y - y_predicted + y_mean
    else:
        y_adj = pd.DataFrame(
            index=dt.loc[dt["regression_id"] == regression_id].index,
            columns=[
                "adjusted_prediction",
                "adj_model",
                "adj_outlier",
                "p_value",
                "slope1",
                "slope2",
                "intercept",
                "residual",
            ],
        )
        y_adj["adj_model"] = adjust_model
        y_adj["adjusted_prediction"] = y - np.ravel(y_pred) + np.mean(y)
        y_adj["adj_outlier"] = "False"
        y_adj.loc[outlier == True, "adj_outlier"] = "True"
        y_adj["p_value"] = p_value
        y_adj["slope1"] = slope1
        y_adj["slope2"] = slope2
        y_adj["intercept"] = intercept
        y_adj["residual"] = pd.DataFrame(y - np.ravel(y_pred))

    #  Section 5:  prepare data for output , which includes original data, information of adjustment model, and adjusted value

    # write the columns related to adjustment for the given regression_id back to the whole dataset
    dt_regression_id = dt.loc[(dt["regression_id"] == regression_id),].copy()
    dt_regression_id["adjusted_prediction"] = y_adj["adjusted_prediction"]
    dt_regression_id["adj_model"] = y_adj["adj_model"]
    dt_regression_id["adj_outlier"] = y_adj["adj_outlier"]
    dt_regression_id["p_value"] = y_adj["p_value"]
    dt_regression_id["slope1"] = y_adj["slope1"]
    dt_regression_id["slope2"] = y_adj["slope2"].astype(np.float32)
    dt_regression_id["intercept"] = y_adj["intercept"]

    # from the whole dataset, which already includes all the inforamtion/columns for adjustment, subset the data by the regression_id and return the subset data
    dt_regression_id = dt_regression_id.loc[
        (dt_regression_id["analysis_target_y"] == 1)
        & (dt_regression_id["adj_model"] != None),
    ]
    return dt_regression_id


def reg_adjust_parallel_pyspark(dt_regression_id, alpha=0.3):
    """
    reg_adjust_parallel is for the old version adjustment for pyspark, and it conducts
    adjustment for the given regression_id; the function fit 3 models, i.e. linear,
    quadratic, and logarithmic models, and chooses the valid model with the highest
    adjusted r-square as the final model; the function will choose linear model if all
    models are invalid, but the adjustment won't be conducted as the p-value of the
    un-valid linear regression is set to 2; input dt is the whole input data;

    input regression_id is a string, representing a unique decision group for trait adjustment;

    output is the data frame for the given regression_id with adjusted y values
    """
    # get dataframe for x and y
    x = dt_regression_id[["prediction_x"]]
    y = dt_regression_id[["prediction"]].to_numpy().ravel()

    if dt_regression_id["prediction"].nunique() > 2:

        quadratic_fit = "Yes"
        n_data = dt_regression_id.shape[0]

        x2 = pd.DataFrame(
            PolynomialFeatures(
                degree=2, interaction_only=True, include_bias=False
            ).fit_transform(x)
        )
        x2 = x2.iloc[:, 1:3].copy()
        if x.min().min() > 0:
            x3 = np.log(x).copy()
        else:
            x3 = x.copy()

        if n_data >= 500:
            thr_ot = 6 / np.sqrt(n_data)
        else:
            thr_ot = 4 / np.sqrt(n_data)

        x1 = sma.add_constant(x)
        x3 = sma.add_constant(x3)

        ft1a = sma.OLS(y, x1).fit()
        ft3a = sma.OLS(y, x3).fit()

        outlier1 = pd.DataFrame(OLSInfluence(ft1a).dfbetas)
        outlier3 = pd.DataFrame(OLSInfluence(ft3a).dfbetas)
        outlier1 = outlier1[[1]]
        outlier3 = outlier3[[1]]
        outlier1.index = outlier3.index = dt_regression_id.index
        x1b = x1.loc[(abs(outlier1[1].values) < thr_ot), :]
        y1b = y[(abs(outlier1[1]) < thr_ot)]
        x3b = x3.loc[(abs(outlier3[1].values) < thr_ot), :]
        y3b = y[(abs(outlier3[1]) < thr_ot)]

        ft1 = sma.OLS(y1b, x1b).fit()
        ft3 = sma.OLS(y3b, x3b).fit()
        adj_r1 = ft1.rsquared_adj
        adj_r3 = ft3.rsquared_adj

        p_value1 = ft1.f_pvalue
        p_value3 = ft3.f_pvalue

        # turn off linear adjustment if slope is negative
        if ft1.params[1] < 0:
            p_value1 = 2
            adj_r1 = -1

        nd1 = x2[2].nunique()
        if nd1 >= 6:
            x2 = sma.add_constant(x2)
            ft2a = sma.OLS(y, x2).fit()
            outlier2 = pd.DataFrame(OLSInfluence(ft2a).dfbetas)
            outlier2 = outlier2[[2]]
            outlier2.index = dt_regression_id.index
            x2b = x2.loc[(abs(outlier2[2].values) < thr_ot)]
            y2b = y[(abs(outlier2[2]) < thr_ot)]
            nd2 = x2b[2].nunique()

            if nd2 >= 6:
                ft2 = sma.OLS(y2b, x2b).fit()
                a = ft2.params[2]
                b = ft2.params[1]
                max_x = -(b / (2 * a))
                x2c = x2.copy()
                x2c[1] = x2c[1].clip(upper=max_x)
                x2c[2] = x2c[2].clip(upper=max_x**2)
                adj_r2 = ft2.rsquared_adj
                p_value2 = ft2.f_pvalue

                if (max_x > (max(dt_regression_id["prediction_x"]) + 10)) | (
                    max_x < (min(dt_regression_id["prediction_x"]) - 10)
                ):
                    quadratic_fit = "No"
                    ft2 = "None"
                if (ft2 != "None") & ((p_value2 > alpha) | (ft2.params[2] > 0)):
                    quadratic_fit = "No"
                    ft2 = "None"

            else:
                quadratic_fit = "No"
                ft2 = "None"
        else:
            quadratic_fit = "No"
            ft2 = "None"

        if ft3.params[1] < 0:

            if quadratic_fit == "Yes":

                if adj_r2 == max(adj_r1, adj_r2):
                    p_value = p_value2
                    model = ft2
                else:
                    p_value = p_value1
                    model = ft1

            else:
                p_value = p_value1
                model = ft1

        elif quadratic_fit == "No":
            if adj_r1 == max(adj_r1, adj_r3):
                p_value = p_value1
                model = ft1
            else:
                p_value = p_value3
                model = ft3

        else:

            if adj_r2 == max(adj_r1, adj_r2, adj_r3):
                p_value = p_value2
                model = ft2
            if adj_r3 == max(adj_r1, adj_r2, adj_r3):
                p_value = p_value3
                model = ft3
            if adj_r1 == max(adj_r1, adj_r2, adj_r3):
                p_value = p_value1
                model = ft1

        if model == ft1:
            y_pred = model.predict(x1)
            adjust_model = "linear"
            outlier = outlier1
            slope1 = ft1.params[1]
            slope2 = 0
            intercept = ft1.params[0]
            p_value = p_value1

        elif model == ft2:
            y_pred = model.predict(x2c)
            adjust_model = "quadratic"
            outlier = outlier2
            slope1 = ft2.params[1]
            slope2 = ft2.params[2]
            intercept = ft2.params["const"]
            p_value = p_value2

        elif model == ft3:
            y_pred = model.predict(x3)
            adjust_model = "log"
            outlier = outlier3
            slope1 = ft3.params[1]
            slope2 = 0
            intercept = ft3.params[0]
            p_value = p_value3

        else:
            adjust_model = "NONE"

    else:
        adjust_model = "NONE"

    if adjust_model == "NONE":
        y_adj = pd.DataFrame(
            {
                "adjusted_prediction": y,
                "adj_model": None,
                "adj_outlier": "False",
                "p_value": None,
                "slope1": None,
                "slope2": None,
                "intercept": None,
                "residual": None,
            }
        )

    # if adjustment is conducted, calculate adjusted_y = y - y_predicted + y_mean
    else:
        y_adj = pd.DataFrame(
            {
                "adjusted_prediction": np.ravel(y) - np.ravel(y_pred) + np.mean(y),
                "adj_model": adjust_model,
                "adj_outlier": "False",
                "p_value": p_value,
                "slope1": slope1,
                "slope2": slope2,
                "intercept": intercept,
                "residual": np.ravel(y) - np.ravel(y_pred),
            }
        )
        y_adj.loc[outlier == True, "adj_outlier"] = "True"

    y_adj.index = dt_regression_id.index
    dt_regression_id["adjusted_prediction"] = y_adj["adjusted_prediction"]
    dt_regression_id["adj_model"] = y_adj["adj_model"]
    dt_regression_id["adj_outlier"] = y_adj["adj_outlier"]
    dt_regression_id["p_value"] = y_adj["p_value"]
    dt_regression_id["slope1"] = y_adj["slope1"]
    dt_regression_id["slope2"] = y_adj["slope2"]
    dt_regression_id["intercept"] = y_adj["intercept"]
    dt_regression_id = dt_regression_id.loc[dt_regression_id["analysis_target_y"] == 1,]

    # apply post-processing
    dt_regression_id = post_processing(dt_regression_id, alpha=alpha)

    return dt_regression_id


def reg_adjust_parallel_rm_pyspark(dt_regression_id, alpha=0.3):
    """
    reg_adjust_parallel_rm_pyspark is for the new version adjustment with robust model for pyspark, and it conducts adjustment for the given regression_id;
    the function fit 3 models, i.e. linear, quadratic, and logarithmic models, and chooses the valid model with the highest adjusted r-square as the final model;
    the function will choose linear model if all models are invalid, but the adjustment won't be conducted as the p-value of the un-valid linear regression is set to 2;
    input dt is the whole input data;
    input regression_id is a string, representing a unique decision group for trait adjsutment,
    and regression_id is a combination of: 'analysis_year', 'ap_data_sector' ,'analysis_type' +'market_seg', 'source_id', 'x', and 'y';
    output is the data frame for the given regression_id with adjusted y values
    """
    # get dataframe for x and y
    x = dt_regression_id[["prediction_x"]].to_numpy()
    y = dt_regression_id[["prediction"]].to_numpy().ravel()

    # set paramters for the huber regression
    ep = 1.65
    alpha2 = 0
    max_iter = 100

    # fit different regression models only if the unique y value
    # (i.e. prediction) is more than 2, else don't conduct adjustment
    if dt_regression_id["prediction"].nunique() > 6:

        # Section 1: prepare data, fit model, and save p-value
        # and adjusted r-square of models
        # For each option:
        # Create the features
        # Fit huber regression for linear (ft1), quadratic (ft2), and logarithmic model (ft3)
        #  Get the fitted y value from each model
        # Get the r-square of each fitted model
        #  Get the adjusted r-square of each fitted model
        # Get the p_value of f-test of each model

        # Linear
        ft1 = HuberRegressor(
            epsilon=ep,
            max_iter=max_iter,
            alpha=alpha2,
            warm_start=False,
            fit_intercept=True,
            tol=1e-05,
        ).fit(x, y)
        if (
            ft1.coef_[0] < 0
        ):  # turn off the linear regression if the slope <0  by setting p_value
            # of the model to 2 and adjusted r-square to -1
            p_value1 = 2
            adj_r1 = -1
        else:
            y1_pred = ft1.predict(x)
            r1 = explained_variance_score(y, y1_pred)
            adj_r1 = cpt_adj_r2(r1, x)
            p_value1 = f_test(x, y, ft1)

        # Quadratic
        x2 = pd.DataFrame(
            PolynomialFeatures(
                degree=2, interaction_only=True, include_bias=False
            ).fit_transform(np.reshape(x, (-1, 1)))
        )
        x2 = x2.iloc[:, 1:3].copy()
        nd1 = x2[2].nunique()
        ft2 = HuberRegressor(
            epsilon=ep,
            max_iter=max_iter,
            alpha=alpha2,
            warm_start=False,
            fit_intercept=True,
            tol=1e-05,
        ).fit(x2, y)
        if (nd1 < 6) | (ft2.coef_[1] > 0):
            ft2 = "None"
            p_value2 = 2
            adj_r2 = -1
        else:
            y2_pred = ft2.predict(x2)
            r2 = explained_variance_score(y, y2_pred)
            adj_r2 = cpt_adj_r2(r2, x2)
            p_value2 = f_test(x2, y, ft2)
            # if quadratic fit is valid, save the model paramters
            a = ft2.coef_[1]
            b = ft2.coef_[0]
            # flat the curve after peak is reached by setting the maximum x value of
            # data to the value where the peak is, this converted x is only used
            # internally for adjustment and not written to the output by creating x2c
            # variable
            max_x = -(b / (2 * a))
            x2c = x2.copy()
            x2c[1] = x2c[1].clip(upper=max_x)
            x2c[2] = x2c[2].clip(upper=max_x**2)

        # Logarithmic option
        if np.min(x) > 0:
            x3 = np.log(x).copy()
            ft3 = HuberRegressor(
                epsilon=ep,
                max_iter=max_iter,
                alpha=alpha2,
                warm_start=False,
                fit_intercept=True,
                tol=1e-05,
            ).fit(x3, y)
            y3_pred = ft3.predict(x3)
            r3 = explained_variance_score(y, y3_pred)
            adj_r3 = cpt_adj_r2(r3, x3)
            p_value3 = f_test(x3, y, ft3)

        else:
            ft3 = None
            p_value3 = 2
            adj_r3 = -1

        # Section 2:  choose model based on 1) adjusted r-square and
        # 2) whether the model is valid or not
        # linear model will be selected if quadratic and logarithmic model are both invalid.
        # If the selected linear model is also invalid the adjustment
        # wouldn't be conducted as the p-value of the invalid linear model is 2
        # Section 3:  get the predicted/fitted y-value from the selected model,
        # and save paramters and p-value of the selected model

        max_r = max(adj_r1, adj_r2, adj_r3)
        if max_r == -1:
            adjust_model = "NONE"
        elif adj_r1 == max_r:
            p_value = p_value1
            y_pred = ft1.predict(x)
            adjust_model = "linear"
            outlier = ft1.outliers_
            slope1 = ft1.coef_[0]
            slope2 = 0.0
            intercept = ft1.intercept_
        elif adj_r2 == max_r:
            p_value = p_value2
            y_pred = ft2.predict(x2)
            adjust_model = "quadratic"
            outlier = ft2.outliers_
            slope1 = ft2.coef_[0]
            slope2 = ft2.coef_[1]
            intercept = ft2.intercept_
        elif adj_r3 == max_r:
            p_value = p_value3
            y_pred = ft3.predict(x3)
            adjust_model = "log"
            outlier = ft3.outliers_
            slope1 = ft3.coef_[0]
            slope2 = 0.0
            intercept = ft3.intercept_
        else:
            adjust_model = "NONE"
    else:
        adjust_model = "NONE"

    # Section 4:  prepare data related to adjustment model to be added in the original data

    # use original y value  if no model is selected due to insufficient data size
    if adjust_model == "NONE":
        y_adj = pd.DataFrame(
            {
                "adjusted_prediction": y,
                "adj_model": None,
                "adj_outlier": "False",
                "p_value": None,
                "slope1": None,
                "slope2": None,
                "intercept": None,
                "residual": None,
            }
        )

    # if adjustment is conducted, calculate adjusted_y = y - y_predicted + y_mean
    else:
        y_adj = pd.DataFrame(
            {
                "adjusted_prediction": np.ravel(y) - np.ravel(y_pred) + np.mean(y),
                "adj_model": adjust_model,
                "adj_outlier": "False",
                "p_value": p_value,
                "slope1": slope1,
                "slope2": slope2,
                "intercept": intercept,
                "residual": np.ravel(y) - np.ravel(y_pred),
            }
        )
        y_adj.loc[outlier == True, "adj_outlier"] = "True"

    # Section 5:  prepare data for output , which includes original data,
    # information of adjustment model, and adjusted value

    # write the columns related to adjustment for the given regression_id back to the whole dataset
    # columns needed for trial merge are:
    # barcode, y, adjusted, prediction, adjusted_prediction, p_value, adj_outlier, slope1, slope2
    # columns needed for pvs merge are:
    # ap_data_sector, analysis_year, analysis_type, source_id, decision_group_rm,
    # stage, breakouts, y, entry_id, material_type
    y_adj.index = dt_regression_id.index
    dt_regression_id["adjusted_prediction"] = y_adj["adjusted_prediction"]
    dt_regression_id["adj_model"] = y_adj["adj_model"]
    dt_regression_id["adj_outlier"] = y_adj["adj_outlier"]
    dt_regression_id["p_value"] = y_adj["p_value"]
    dt_regression_id["slope1"] = y_adj["slope1"]
    dt_regression_id["slope2"] = y_adj["slope2"]
    dt_regression_id["intercept"] = y_adj["intercept"]

    # apply post-processing
    dt_regression_id = post_processing(dt_regression_id, alpha=alpha)
    dt_regression_id = dt_regression_id.loc[
        (dt_regression_id["analysis_target_y"] == 1)
        & (dt_regression_id["adjusted"] != None),
    ]

    return dt_regression_id


"""
cpt_adj_r2 function calculates the adjusted_r_square, and is used within the reg_adjust_parallel_rm function;
input r2 is the r-square of a given model, and input x is x variable(s) of the regression model
output is a float for adjusted_r_square 
"""


def cpt_adj_r2(r2, x):
    n = x.shape[0]
    k = x.shape[1]

    adj_r = 1 - ((1 - r2) * (n - 1) / (n - k - 1))
    return adj_r


"""
f_test calculates the p_value of the f-test for the given model ft, and this function is used within the reg_adjust_parallel_rm function;
input x is a dataframe for x variable(s) of the regression model, y is a dataframe for observed y values, and ft is the regression model;
output is one p_value (float) Of the F-test of the model
"""


def f_test(x, y, ft):
    rsq = ft.score(x, y)
    fstat = (rsq / (1 - rsq)) * ((x.shape[0] - x.shape[1] - 1) / x.shape[1])
    k = x.shape[1] + 1
    n = x.shape[0]
    pvalue = 1 - ss.f.cdf(fstat, k - 1, n - k)

    return pvalue


"""
significance_pvalue calculates the p_value of each paramter for the given model lm;
input X is a dataframe for x variable(s) of the regression model, y is a dataframe for observed y values, and lm is the regression model;
output is the p_value(s) for each paramter of the given lm model.

this function is not used for now as the p_value of the f_test of the model is used to determine significance of the model
"""


def significance_pvalue(x, y, lm):
    params = np.append(lm.intercept_, lm.coef_)

    predictions = lm.predict(x)
    predictions = predictions.ravel()
    new_x = pd.DataFrame({"Constant": np.ones(len(x))}).join(
        pd.DataFrame(x), how="left", on=None, validate="many_to_many"
    )
    mse = (sum((y - predictions) ** 2)) / (len(new_x) - len(new_x.columns))

    var_b = mse * (np.linalg.inv(np.dot(new_x.T, new_x)).diagonal())
    sd_b = np.sqrt(var_b)
    ts_b = params / sd_b

    p_values = [
        2 * (1 - ss.t.cdf(np.abs(i), (len(new_x) - len(new_x.columns)))) for i in ts_b
    ]

    return p_values


def cor_test(regression_id, dt, column1="prediction_x", column2="prediction"):
    """
    cor_test function derives the correlation between column1 and column2, and also test the significance of the correlation;
    this function is called by parallel_cor_test function;
    input regression_id and dt are the same as reg_adjust_parallel function,
    input column1 and column2 are both strings to indicate which columns to calculate the correlation;
    output is a 1x3 dataframe including regression_id, correlation, and the p_value of the correlation test
    """

    dt_sb = dt.loc[dt["regression_id"] == regression_id]
    cor_ts = ss.pearsonr(dt_sb[column1], dt_sb[column2])
    cor = cor_ts[0]
    p_value = cor_ts[1]

    re = pd.DataFrame([regression_id, cor, p_value])
    re = re.T
    re.columns = ["regression_id", "cor", "p_value"]
    return re


def parallel_cor_test(core_test, dt, ids, column1="prediction_x", column2="prediction"):
    """
    parallel_cor_test function parallelizes the cor_test function

    input cor_test is the cor_test function itself,
    input dt, column1 and column2 are the same as cor_test function,
    and input ids is a list of all unique regression_ids in dt;
    output is a dataframe including regression_id, correlation, and the p_value of the correlation test for all regression_ids
    """
    n_count = multiprocessing.cpu_count() - 2
    pool = Pool(processes=n_count)
    dt_constant = partial(core_test, dt=dt, column1=column1, column2=column2)
    results = pool.map(dt_constant, ids)
    cols = (column1, column2)
    results = (
        pd.concat(results, axis=0) if len(results) > 0 else pd.DataFrame(columns=cols)
    )

    return results
