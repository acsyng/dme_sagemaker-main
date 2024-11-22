import numpy as np
from scipy.stats import f, norm

########################################################################################
# apply_p_direction
# takes a pval that is a raw evaluation of a zscore and returns a transform based on the
# direction specified
########################################################################################

NOT_EQUAL = "not equal"


def apply_p_direction(pval, direction):
    conditions = [
        (direction == "equal"),
        (direction == NOT_EQUAL),
        (np.isin(direction, ["left", "greater", "positive"])),
        (np.isin(direction, ["right", "less", "negative"])),
    ]

    choices = [
        1 - 2 * np.fmin(pval, 1 - pval),
        2 * np.fmin(pval, 1 - pval),
        pval,
        1 - pval,
    ]

    out = np.select(conditions, choices, default=(1 - pval))

    return out.astype(np.float64)


########################################################################################
# var_ratio_f_test
# Generates the p-value for a test of ratio of standard errors (scaled by means) for two normal
# distributions with known means and standard deviations
#
# input:
#    ent_pred: vector of means describing 1st distributions
#    chk_pred: vector of means describing 2nd distributions
#    ent_stddev: vector of standard deviations describing 1st distributions
#    chk_stddev: vector of standard deviations describing 2nd distributions
#    ent_count: vector of observation counts used to generate 1st distributions
#    chk_count: vector of observation counts used to generate 2nd distributions
#    spread_factor: scales the initial score to ease/tighten the comparison requirements
#
# output:
#    score: 100x the scaled score used in the f test
#    out: 100x (1-p-value), capped between 1 and 99
########################################################################################
def var_ratio_f_test(
    ent_pred, chk_pred, ent_stddev, chk_stddev, ent_count, chk_count, spread_factor=1
):
    # f_test
    # Generates the p-value for a test of the ratio of two standard deviations, assuming that lower variance is better

    # Set required thresholds
    thresh = 0.0001
    ent_pred = np.fmax(thresh, ent_pred)
    chk_pred = np.fmax(thresh, chk_pred)
    ent_stddev = np.fmax(thresh, ent_stddev)
    chk_stddev = np.fmax(thresh, chk_stddev)

    score = (ent_stddev / ent_pred / (chk_stddev / chk_pred)) ** 2 / spread_factor

    out = np.minimum(
        99, np.maximum(1, 100 * (1 - f.cdf(score, ent_count - 1, chk_count - 1)))
    )
    return (score**0.5) * 100, out


########################################################################################
# var_raw_f_test
# Generates the p-value for a test of ratio of standard errors for two normal
# distributions with known means and standard deviations
#
# input:
#    ent_stddev: vector of standard deviations describing 1st distributions
#    chk_stddev: vector of standard deviations describing 2nd distributions
#    ent_count: vector of observation counts used to generate 1st distributions
#    chk_count: vector of observation counts used to generate 2nd distributions
#    spread_factor: scales the initial score to ease/tighten the comparison requirements
#
# output:
#    score: 100x the scaled score used in the f test
#    out: 100x (1-p-value), capped between 1 and 99
########################################################################################
def var_raw_f_test(ent_stddev, chk_stddev, ent_count, chk_count, spread_factor=1):
    # f_test
    # Generates the p-value for a test of the ratio of two standard deviations, assuming that lower variance is better

    # Set required thresholds
    thresh = 0.0001
    ent_stddev = np.fmax(thresh, ent_stddev)
    chk_stddev = np.fmax(thresh, chk_stddev)

    score = (ent_stddev / chk_stddev) ** 2 / spread_factor

    out = np.minimum(
        99, np.maximum(1, 100 * (1 - f.cdf(score, ent_count - 1, chk_count - 1)))
    )
    return (ent_stddev / chk_stddev) * 100, (score**0.5) * 100, out, score


########################################################################################
# ratio_t_test
# Generates the p-value for a test of ratio of means for two normal
# distributions with known means and standard errors of the means
#
#  z_score = (mean_1/mean2 - 1)/
#            (mean_1/mean2 * sqrt((sem_1/mean_1)^2+(sem_2/mean_2)))
#
# input:
#    ent_pred: vector of means describing 1st distributions
#    chk_pred: vector of means describing 2nd distributions
#    ent_stddev: vector of standard deviations describing 1st distributions
#    chk_stddev: vector of standard deviations describing 2nd distributions
#    ent_count: vector of observation counts used to generate 1st distributions
#    chk_count: vector of observation counts used to generate 2nd distributions
#    threshold_factor: target value against which the mean should be compared against
#    spread_factor: relaxation factor that modifies how quickly a p-value should move away from 0.5
#    direction: direction of the test
#
# output:
#    100x percent check calculation (ent_pred/chk_pred)
#    denom: denominator of the z-score, indicating pooled stddev
#    out: 100x (1-p-value), capped between 1 and 99
#    zscore: zscore used to calculate p-value
#
########################################################################################
def norm_ratio_t_test(
    ent_pred,
    chk_pred,
    ent_stddev,
    chk_stddev,
    ent_count,
    chk_count,
    threshold_factor,
    spread_factor,
    direction,
):
    # Set required thresholds
    thresh = 0.0001
    ent_pred = np.fmax(thresh, ent_pred)
    chk_pred = np.fmax(thresh, chk_pred)
    ent_stddev = np.fmax(thresh, ent_stddev)
    chk_stddev = np.fmax(thresh, chk_stddev)

    numer = ent_pred / chk_pred - threshold_factor
    denom = (
        spread_factor
        * ent_pred
        / chk_pred
        * (
            (ent_stddev / (ent_count**0.5 * ent_pred)) ** 2
            + (chk_stddev / (chk_count**0.5 * chk_pred)) ** 2
        )
        ** 0.5
    )

    # Calculate zscore & base p value
    pval = norm.cdf(numer / denom)

    # Transform pval based on test type
    out = apply_p_direction(pval, direction)

    return (
        np.minimum(1000, (ent_pred / chk_pred) * 100),
        denom,
        np.minimum(99, np.maximum(1, 100 * out)),
        numer / denom,
    )


########################################################################################
# norm_diff_t_test() (welch_test)
# Generates the p-value for a test of difference of means for two normal
# distributions with known means and standard errors of the means
#
#  z_score = (mean_1 - mean_2)/ sqrt((sem_1)^2+(sem_2)^2)
#
# input:
#    ent_pred: vector of means describing 1st distributions
#    chk_pred: vector of means describing 2nd distributions
#    ent_stddev: vector of standard deviations describing 1st distributions
#    chk_stddev: vector of standard deviations describing 2nd distributions
#    ent_count: vector of observation counts used to generate 1st distributions
#    chk_count: vector of observation counts used to generate 2nd distributions
#    threshold_factor: target value against which the mean should be compared against
#    spread_factor: relaxation factor that modifies how quickly a p-value should move away from 0.5
#    direction: direction of the test
#
# output:
#    zmat: zscore used in the calculation
#    denom: denominator of the z-score, indicating pooled stddev
#    out: 100x (1-p-value), capped between 1 and 99
#
########################################################################################
def norm_diff_t_test(
    ent_pred,
    chk_pred,
    ent_stddev,
    chk_stddev,
    ent_count,
    chk_count,
    threshold_factor,
    spread_factor,
    direction,
):
    # Set required thresholds
    thresh = 0.0001
    ent_stddev = np.fmax(thresh, ent_stddev)
    chk_stddev = np.fmax(thresh, chk_stddev)

    numer = ent_pred - (chk_pred * threshold_factor)
    denom = (
        spread_factor * (ent_stddev**2 / ent_count + chk_stddev**2 / chk_count) ** 0.5
    )

    zmat = numer / denom

    pval = norm.cdf(zmat)

    # Transform pval based on test type
    out = apply_p_direction(pval, direction)

    pctchk = np.full(ent_pred.shape, 1000, dtype=float)
    pctchk[chk_pred != 0] = np.minimum(
        1000, (ent_pred[chk_pred != 0] / chk_pred[chk_pred != 0]) * 100
    )

    return pctchk, denom, np.minimum(99, np.maximum(1, 100 * out)), zmat


#######################################################################################
# poisson_test
# Generates the p-val for a test of the difference of means (Poisson
# parameters) for two poisson distributions with known rate (mean/100) and
# sample size counts. Use in the case that we don't have sem for Wald test
# - poisson distribution approximates a negative binomial distribution
#
# References: Tests for the Difference Between Two Poisson Rates,
#    PASS Sample Size Software documentation, NCSS.
#    Aban, Cutter, and Mavinga 2008
#
# input:
#    ent_pred: vector of rates describing 1st distributions
#    chk_pred: vector of rates describing 2nd distributions
#    ent_count: vector of observation counts used to generate 1st distributions
#    chk_count: vector of observation counts used to generate 2nd distributions
#    threshold_factor: target value against which the mean should be compared against
#    spread_factor: relaxation factor that modifies how quickly a p-value should move away from 0.5
#    direction: direction of the test
#
# output:
#    100x percent check calculation (ent_pred/chk_pred)
#    zmat_denom: denominator of the z-score, indicating dispersion
#    out: 100x (1-p-value), capped between 1 and 99
#    zmat: z-score used to generate p-value
#
#######################################################################################
def zinb_ratio_poisson_test(
    ent_pred, chk_pred, ent_count, chk_count, threshold_factor, spread_factor, direction
):
    # Set required thresholds
    thresh = 0
    ent_pred = np.fmax(thresh, ent_pred)
    chk_pred = np.fmax(thresh, chk_pred)
    threshold_factor = np.fmax(thresh, threshold_factor)

    # Calculate zscore & base p value
    zmat_numer = (ent_pred**0.5 - (chk_pred * threshold_factor) ** 0.5) / 100
    zmat_denom = 0.5 * (1 / ent_count + 1 / chk_count) ** 0.5
    zmat = zmat_numer / (zmat_denom * spread_factor)

    pval = norm.cdf(zmat)

    # Transform pval based on test type
    out = apply_p_direction(pval, direction)

    pctchk = np.full(ent_pred.shape, 1000, dtype=float)
    pctchk[chk_pred != 0] = np.minimum(
        1000, (ent_pred[chk_pred != 0] / chk_pred[chk_pred != 0]) * 100
    )

    return pctchk, zmat_denom, np.minimum(99, np.maximum(1, 100 * out)), zmat


##################################################################################################
# wald_test
# Generates the pval for a test of the difference of means for two negative
# binomial distributions with known rate, sample size, and standard error
# NOTE: Wald test assumes equal overdispersion parameters for both
# distributions, which is probably not true - but this test is relatively
# robust when this assumption is false anyway.
#
# References: Tests for the Ratio of Two Negative Binomial Rates,
#    PASS Sample Size Software documentation, NCSS
#    Aban, Cutter, and Mavinga 2008
#
# input:
#    ent_pred: vector of rates describing 1st distributions
#    chk_pred: vector of rates describing 2nd distributions
#    ent_stddev: vector of standard deviations describing 1st distributions
#    chk_stddev: vector of standard deviations describing 2nd distributions
#    ent_count: vector of observation counts used to generate 1st distributions
#    chk_count: vector of observation counts used to generate 2nd distributions
#    threshold_factor: target value against which the mean should be compared against
#    spread_factor: relaxation factor that modifies how quickly a p-value should move away from 0.5
#    direction: direction of the test
#
# output:
#    100x percent check calculation (ent_pred/chk_pred)
#    second argument: stddev of beta, indicating dispersion
#    out: 100x (1-p-value), capped between 1 and 99
#    wmat: zscore used in generating p-value
#
##################################################################################################
def zinb_ratio_wald_test(
    ent_pred,
    chk_pred,
    ent_stddev,
    chk_stddev,
    ent_count,
    chk_count,
    threshold_factor,
    spread_factor,
    direction,
):
    # Set required thresholds
    thresh = 0.0001
    thresh2 = 1e-8
    ent_pred = np.fmax(thresh, ent_pred)
    chk_pred = np.fmax(thresh, chk_pred)
    ent_stddev = np.fmax(thresh, ent_stddev)
    chk_stddev = np.fmax(thresh, chk_stddev)

    # Calculate wscore & base p value
    disp_x = ent_stddev**2 / ent_count / ent_pred
    disp_ch = chk_stddev**2 / chk_count / chk_pred
    disp_calc = (disp_x + disp_ch) / 2

    beta = np.log(np.fmax(ent_pred / (chk_pred * threshold_factor), thresh2))

    nratio = ent_count / chk_count
    var_beta = (
        1
        / chk_count
        * (1 / chk_pred + 1 / (nratio * ent_pred) + (1 + nratio) * disp_calc / nratio)
    )
    wmat = beta / (var_beta**0.5 * spread_factor)

    pval = norm.cdf(wmat)

    # Transform pval based on test type
    out = apply_p_direction(pval, direction)

    return (
        np.minimum(1000, (ent_pred / chk_pred) * 100),
        var_beta**0.5,
        np.minimum(99, np.maximum(1, 100 * out)),
        wmat,
    )


########################################################################################
# rating_diff_t_test
#
# Generates the p-value for a test of difference of a mean of a normal
# distribution with a known standard deviation versus a known value
#
#  z_score = (mean_1 - mean_2)/ sqrt((sem_1)^2+(sem_2)^2)
#
#  input:
#    ent_pred: vector of means describing distributions
#    ent_stddev: vector of standard deviations describing distributions
#    ent_count: vector of observation counts used to generate distributions
#    threshold_factor: target value against which the mean should be compared against
#    spread_factor: relaxation factor that modifies threshold_factor
#    direction: direction of the test
#
# output:
#    first argument: percent of threshold
#    denom: denominator used in z-score calculation
#    out: 100x (1-p-value), capped between 1 and 99
########################################################################################
def rating_diff_t_test(
    ent_pred,
    ent_stddev,
    ent_count,
    threshold_factor,
    spread_factor=1,
    direction="right",
):
    # Set required thresholds
    thresh = 0.0001
    ent_stddev = np.fmax(thresh, ent_stddev)

    diff_low = ent_pred - threshold_factor + spread_factor
    diff_med = ent_pred - threshold_factor
    diff_hi = ent_pred - threshold_factor - spread_factor
    denom = ent_stddev / (ent_count**0.5)

    zmat_low = diff_low / denom
    zmat_med = diff_med / denom
    zmat_hi = diff_hi / denom

    pval_low = norm.cdf(zmat_low)
    pval_med = norm.cdf(zmat_med)
    pval_hi = norm.cdf(zmat_hi)

    # Transform pval based on test type
    conditions = [
        (direction == "equal"),
        (direction == NOT_EQUAL),
        (np.isin(direction, ["left", "greater", "positive"])),
        (np.isin(direction, ["right", "less", "negative"])),
    ]

    z_choices = [zmat_hi, zmat_hi, zmat_med, zmat_med]

    prob_choices = [
        (pval_hi - pval_low),
        2 * np.fmin(pval_low, 1 - pval_hi),
        pval_med,
        1 - pval_med,
    ]

    out = np.select(conditions, prob_choices, default=None).astype(np.float64)
    z_out = np.select(conditions, z_choices, default=None).astype(np.float64)

    return (
        np.minimum(1000, (ent_pred / threshold_factor) * 100),
        denom,
        np.minimum(99, np.maximum(1, 100 * out)),
        z_out,
    )


########################################################################################
# text_tests
#
# Checks if two strings are equal or not equal
#
# input: ent_text: entry text values
#        target_text: target text value
#        direction: direction of the test's H0
#
# output: 1 if test failed, 99 if succeeded (padded to work in np.selects)
#
########################################################################################
def text_tests(ent_text, target_text, direction):
    conditions = [
        (direction == "equal"),
        (direction == NOT_EQUAL),
        (direction == "does not contain"),
        (np.isin(direction, ["contain", "contains"])),
    ]
    choices = [
        (ent_text != target_text),
        (ent_text == target_text),
        (ent_text in target_text),
        (ent_text not in target_text),
    ]

    out = np.select(conditions, choices, default=None).astype(np.float64)

    return out, 1 + 98 * out, out, out


def case_equal_predictions():
    """
    Return metrics in the case that ent_pred = chk_pred
    """
    return 100, 0, 50, 0


#######################################################################################
# mean_tests
# specify mapping of what stat tests to use in which cases for absolute observations
#######################################################################################
def trial_mean_tests(
    ent_pred,
    chk_pred,
    ent_stddev,
    chk_stddev,
    ent_count,
    chk_count,
    metric,
    threshold_factor,
    spread_factor,
    direction,
    distribution_type,
):
    conditions = [
        (metric == "stability"),
        ((distribution_type == "normal") | (distribution_type == "norm")),
        (distribution_type == "rating"),
        ((distribution_type == "zinb") & (np.isnan(ent_stddev) | np.isnan(chk_stddev))),
        (distribution_type == "zinb"),
    ]

    choices = [
        var_raw_f_test(ent_stddev, chk_stddev, ent_count, chk_count, spread_factor),
        norm_diff_t_test(
            ent_pred,
            chk_pred,
            ent_stddev,
            chk_stddev,
            ent_count,
            chk_count,
            threshold_factor,
            spread_factor,
            direction,
        ),
        rating_diff_t_test(
            ent_pred, ent_stddev, ent_count, threshold_factor, spread_factor, direction
        ),
        zinb_ratio_poisson_test(
            ent_pred,
            chk_pred,
            ent_count,
            chk_count,
            threshold_factor,
            spread_factor,
            direction,
        ),
        zinb_ratio_wald_test(
            ent_pred,
            chk_pred,
            ent_stddev,
            chk_stddev,
            ent_count,
            chk_count,
            threshold_factor,
            spread_factor,
            direction,
        ),
    ]

    pctchk, sem, prob, zscore = np.select(conditions, choices, default=None)
    method = np.select(
        conditions,
        [
            "norm_f",
            "norm_diff_t",
            "abs_rating_diff_t",
            "zinb_ratio_poisson",
            "zinb_ratio_wald",
        ],
        default="failure",
    )

    return (
        pctchk.astype(np.float64),
        zscore.astype(np.float64),
        sem.astype(np.float64),
        prob.astype(np.float64),
        method,
    )
