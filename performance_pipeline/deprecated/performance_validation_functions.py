# function to get any variable shared across multiple recipes (such as the grouping columns)
import numpy as np
import pandas as pd

from sklearn.metrics import roc_auc_score

def getGlobalValidationVars():
    # group cols will match in all python and pyspark recipes
    group_cols = ['ap_data_sector', 'analysis_year', 'analysis_type', 'breakout_level_1', 'breakout_level_1_value', \
                  'decision_group_rm', 'current_stage', 'technology', 'material_type', \
                  'entry_id', 'metric_method']

    ml_traits_to_use = ['YGSMN', 'LRTLP', 'ERTLP', 'GRSNP', 'ERHTN', 'PLHTN', 'TWSMN', 'GMSTP']

    return [group_cols, ml_traits_to_use]


# helper functions to only read years in common, this is used in the notebook not in the job
def getPartitionFromYears(years, parts):
    parts_out = []
    years = np.array(years)
    for part in parts:
        part_split = part.split('|')
        for p in part_split:
            if np.any(p == years):
                parts_out.append(part)

    return parts_out


# function used to combine trait scores (if not using dme_mn_prob)
def computeWeightedTraitScores(df_2yrs, merge_cols, trait_list=None, \
                               trait_eval_weights=None, trait_check_dir=None, score_col=None):
    if trait_list is None:
        trait_list = ['YGSMN', 'LRTLP', 'ERTLP', 'GRSNP', 'ERHTN', 'PLHTN', 'TWSMN']
    if trait_eval_weights is None:
        trait_eval_weights = {'YGSMN': 0.5, 'LRTLP': 0.1, 'ERTLP': 0.1, 'GRSNP': 0.1, \
                              'ERHTN': 0.2, 'PLHTN': 0.2, 'TWSMN': 0.05}
    if trait_check_dir is None:
        trait_check_dir = {'YGSMN': 'high', 'LRTLP': 'low', 'ERTLP': 'low', 'GRSNP': 'low', \
                           'ERHTN': 'low', 'PLHTN': 'low', 'TWSMN': 'low'}
    if score_col is None:
        score_col = 'prediction_next'

    # compute validation score for each material first, then merge recommendation
    df_valid = df_2yrs.dropna(subset=['next_year'])
    keep_mask = np.any(df_valid['trait'].values.reshape((-1, 1)) == np.array(trait_list).reshape((1, -1)), axis=1)
    df_valid = df_valid.iloc[keep_mask == 1, :]

    # get mean of score col for each trait and material
    group_with_trait_cols = merge_cols.copy()
    group_with_trait_cols.extend(['metric', 'trait'])
    df_valid = df_valid.groupby(by=group_with_trait_cols).mean().reset_index()

    dirs = np.array([trait_check_dir[trait] for trait in df_valid['trait'].values])
    weights = np.array([trait_eval_weights[trait] for trait in df_valid['trait'].values])

    df_valid['score'] = df_valid[score_col].values * weights
    df_valid['valid_weight'] = weights
    # adjust score based on trait direction
    if np.any(dirs == 'low'):
        df_valid['score'][dirs == 'low'] = df_valid['score'][dirs == 'low'] * -1
    if np.any(dirs == 'center'):
        df_valid['score'][dirs == 'center'] = -1 * np.abs(df_valid['score'][dirs == 'center'])
    df_valid['score'][np.isnan(df_valid['score'])] = 0

    # dot product between score and weights, normalize by weights to handle missing trait vals
    keep_cols = merge_cols.copy()
    keep_cols.append('score')
    keep_cols.append('valid_weight')

    df_valid_grouped = df_valid[keep_cols].groupby(by=merge_cols).sum().reset_index()
    df_valid_grouped['valid_score'] = df_valid_grouped['score'] / df_valid_grouped['valid_weight']
    df_valid_grouped = df_valid_grouped.sort_values(by='valid_score', ascending=False)

    return df_valid, df_valid_grouped


################################# FUNCTIONS TO COMPUTE VALIDATION METRICS
def computeAdvancementRate(df_rec_valid, recommendation_methods, adv_col='was_adv', rec_col_suffix='_normRec'):
    AUC_list, pred_power_list, false_positive_list, notrec_power_list = [], [], [], []
    recommendation_methods_with_reference = np.append(np.array(recommendation_methods), 'reference')
    adv_result = df_rec_valid[adv_col]

    for method in recommendation_methods:
        was_rec = (df_rec_valid['meth_' + method + rec_col_suffix] == True).values
        was_notrec = was_rec == False

        n_rec = np.sum(was_rec)
        n_notrec = np.sum(was_notrec)

        n_rec_adv = np.sum((was_rec) & (adv_result))
        n_notrec_adv = np.sum((was_notrec) & (adv_result))

        # number of true positives divided by number of recommended to be positive
        pred_power_list.append(100 * n_rec_adv / n_rec)
        # proportion of not recommended materials that were advanced
        notrec_power_list.append(100 * n_notrec_adv / n_notrec)
        # number of false positives divided by total negatives
        false_positive_list.append(100 * (n_rec - n_rec_adv) / (adv_result.shape[0] - np.sum(adv_result)))

        # compute AUC to remove dependency on cutoff...
        y_true = adv_result
        y_score = df_rec_valid['meth_' + method + '_score'].values
        nan_mask = (np.isnan(y_true)) | (np.isnan(y_score))
        if np.any(y_true == False) and np.any(y_true == True):  # if only one class, auc is not defined
            AUC_score = roc_auc_score(y_true[nan_mask == False], y_score[nan_mask == False])
        else:
            AUC_score = np.nan
        AUC_list.append(AUC_score)

    # compute expected predictive power, assuming independence between recommendations and advancement decisions
    # this simplifies to the probablity of advancing a material in the following year
    pred_power_list.append(100 * np.sum(adv_result) / adv_result.shape[0])
    notrec_power_list.append(100 * np.sum(adv_result) / adv_result.shape[0])
    false_positive_list.append(100 * n_rec / was_rec.shape[0])

    AUC_list.append(0.5)  # mean value

    df_metrics = pd.DataFrame(data={'method': recommendation_methods_with_reference, \
                                    'rec_prob_adv': pred_power_list, 'notrec_prob_adv': notrec_power_list, \
                                    'false_positive_rate': false_positive_list, \
                                    'AUC': AUC_list})

    return df_metrics


# compute median and standard error on traits in trait list for recommended materials
def computeTraitStatistics(df_rec_valid, recommendation_methods, trait_list, \
                           adv_col='was_adv', score_pref='prediction_delta', rec_col_suffix='_normRec'):
    adv_result = df_rec_valid[adv_col].values
    # set output dataframe
    df_trait_metrics = pd.DataFrame()
    # add 'reference' to recommendation methods
    recommendation_methods_with_reference = np.append(np.array(recommendation_methods), 'reference')

    for trait in trait_list:
        score_col = score_pref + '_' + trait

        rec_med = []
        rec_stderr = []

        for method in recommendation_methods:
            was_rec = df_rec_valid['meth_' + method + rec_col_suffix].values == True
            trait_scores_true = df_rec_valid[was_rec == True][score_col].values

            rec_med.append(np.nanmedian(trait_scores_true))
            rec_stderr.append(np.nanstd(trait_scores_true) / np.sqrt(np.sum(np.isnan(trait_scores_true) == False)))

        # compute reference trait value (median of actually advanced materials)
        rec_med.append(np.nanmedian(df_rec_valid[adv_result == True][score_col].values))
        rec_stderr.append(np.nanstd(df_rec_valid[adv_result == True][score_col].values) / np.sqrt(
            np.sum(np.isnan(df_rec_valid[adv_result == True][score_col].values) == False)))

        df_trait_metrics_temp = pd.DataFrame(data={'method': recommendation_methods_with_reference, \
                                                   trait + '_rec_delta_check': rec_med,
                                                   trait + '_rec_delta_check_stderr': rec_stderr})
        if df_trait_metrics.shape[0] == 0:
            df_trait_metrics = df_trait_metrics_temp.copy()
        else:
            df_trait_metrics = df_trait_metrics.merge(df_trait_metrics_temp, on='method')

    return df_trait_metrics


################################# HELPER FUNCTIONS

# method to aggregate boolean variables within a dataframe when nan's are present.
def booleanNanProtectedAgg(df):
    if np.sum(df.isna()) == df.shape[0]:
        return False
    else:
        return np.nanmax(df)


def checkSameStage(df_in, current_stage, next_stage):
    return (df_in[current_stage] == df_in[next_stage]) & \
        (df_in[current_stage] < 7) & (df_in[next_stage] < 7)


def checkAdvancement(df_in, current_stage, next_stage):
    return (df_in[current_stage] < df_in[next_stage]) & \
        (df_in[current_stage] < 7) & (df_in[next_stage] < 7)


def countPredictions(df_in, truth_col, pred_col):
    n_tp = np.sum((df_in[truth_col] == True) & (df_in[pred_col] == True))
    n_fp = np.sum((df_in[truth_col] == False) & (df_in[pred_col] == True))
    n_tn = np.sum((df_in[truth_col] == False) & (df_in[pred_col] == False))
    n_fn = np.sum((df_in[truth_col] == True) & (df_in[pred_col] == False))

    return n_tp, n_fp, n_tn, n_fn


def computeConfusionMatrixStats(df_in, methods, adv_col='was_adv'):
    # useful vars
    n_tp_list, n_fp_list, n_fn_list, n_tn_list, n_list = [], [], [], [], []

    sensitivity_list = []  # recall
    specificity_list = []  # true negative rate
    false_positive_rate_list = []
    balanced_acc_list = []

    # prefix and suffix for recommendation column
    col_pref = 'meth'
    col_suf = 'normRec'

    for meth in methods:
        # compute stats for this method
        rec_col = col_pref + '_' + meth + '_' + col_suf
        n_tp, n_fp, n_tn, n_fn = countPredictions(df_in, adv_col, rec_col)
        # append to output
        n_tp_list.append(n_tp)
        n_fp_list.append(n_fp)
        n_tn_list.append(n_tn)
        n_fn_list.append(n_fn)
        # compute useful stats
        n_list.append(n_tp + n_fp + n_tn + n_fn)
        sensitivity_list.append(n_tp / (n_tp + n_fn))
        specificity_list.append(n_tn / (n_tn + n_fp))
        false_positive_rate_list.append(n_fp / (n_fp + n_tn))
        balanced_acc_list.append((n_tp / (n_tp + n_fn) + n_tn / (n_tn + n_fp)) / 2)

    df_count = pd.DataFrame(data={'method': methods,
                                  'n': n_list,
                                  'n_tp': n_tp_list,
                                  'n_fp': n_fp_list,
                                  'n_tn': n_tn_list,
                                  'n_fn': n_fn_list,
                                  'balanced acc': balanced_acc_list,
                                  'sensitivity': sensitivity_list,
                                  'specificity': specificity_list,
                                  'false positive rate': false_positive_rate_list})
    return df_count
