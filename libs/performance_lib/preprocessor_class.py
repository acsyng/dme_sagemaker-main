import pandas as pd
import numpy as np
import re  # for text trait processing
from sklearn.linear_model import LinearRegression

class PredAdvPreprocessor:
    """
    Class to preprocess data for predictive advancement.

    """
    def __init__(self,
                 preprocess_steps=[],
                 out_col='was_adv',
                 year_col='analysis_year',
                 cols_to_norm=[],
                 cols_to_fill=[],
                 missing_func=np.nanmedian,
                 corr_traits=[],
                 rm_col='rm_estimate',
                 to_agg=[],
                 agg_names=[],
                 agg_funcs=[],
                 text_traits=[],
                 text_suff=[],
                 text_pref=[],
                 clean_text=[],
                 qualifier_traits=[],
                 qualifier_trait_group_exp={},
                 qualifier_trait_group_not={},
                 make_dummies=[]
                 ):  #

        # set preprocessing steps, other useful vars for preprocessing
        self.preprocess_steps = preprocess_steps
        self.train_flag = True
        self.out_col = out_col
        self.year_col = year_col

        # vars for normalizing
        self.norm_vals = {}
        self.cols_to_norm = cols_to_norm

        # vars for filling with median (or other func)
        self.missing_vals = {}
        self.missing_func = missing_func
        self.cols_to_fill = cols_to_fill

        # vars for running qualifier tests
        if len(qualifier_traits) == 0 and len(qualifier_trait_group_exp) == 0 and len(qualifier_trait_group_not) == 0:
            qualifier_traits = ['dic_t', 'll55_t', 'rr2_t', 'e3_t']
            qualifier_trait_group_exp = {'conventional': {
                # 'dic_t':'dic_s',
                # 'll55_t':'ll55_s',
                # 'rr2_t':'rr2_s',
                # 'e3_t':'e3_s'
            },
                'xtend': {
                    'rr2_t': 'rr2_r',
                    'e3_t': 'e3_s'
                },
                'e3': {
                    'dic_t': 'dic_s',
                    'll55_t': 'll55_s',
                    'rr2_t': 'rr2_s',
                    'e3_t': 'e3_r'
                }}

            qualifier_trait_group_not = {'conventional': {
                'dic_t': 'dic_r',
                'll55_t': 'll55_r',
                'rr2_t': 'rr2_r',
                'e3_t': 'e3_r'
            },
                'xtend': {
                    'dic_t': ['dic_x', 'dic_s'],  # can be u or r
                    'll55_t': ['ll55_s', 'll55_u'],  # can be x or r
                },
                'e3': {}}

        self.qualifier_traits = qualifier_traits
        self.qualifier_trait_group_exp = qualifier_trait_group_exp
        self.qualifier_trait_group_not = qualifier_trait_group_not

        # list columns to make dummy variables out of
        self.make_dummies = make_dummies

        # vars for removing correlations
        # corr_traits = [
        #    [['PLHTN','ERHTN','GMSTP'],['YGSMN']],
        #    [['ERHTN'],['PLHTN']],
        #    [['GMSTP'],['YGSMN']]
        #    ]
        self.corr_traits = corr_traits
        self.sub_corr_lm = {}

        # vars related to processing rm data
        self.rm_col = rm_col
        self.rm_adv_rate = None
        self.rm_centers = None

        # vars related to aggregating data across columns
        self.to_agg = to_agg
        self.agg_names = agg_names
        self.agg_funcs = agg_funcs

        # vars for text trait processing (soy specific)
        self.text_traits = text_traits
        self.text_suff = text_suff
        self.text_pref = text_pref
        self.clean_text = clean_text

    def set_train_flag(self, train_flag: bool):
        self.train_flag = train_flag

    def train_preprocessing(self, df_in):
        # reset preprocessing values
        self.reset_missing_vals()
        self.reset_norm_vals()
        self.reset_rm_vals()

        self.train_flag = True
        df_out = self.preprocess(df_in)
        self.required_cols = list(df_out.columns)
        self.train_flag = False
        return df_out

    def run_preprocessing(self, df_in):
        self.train_flag = False
        df_out = self.preprocess(df_in)
        return df_out
        # if len(set(self.required_cols).difference(set(df_out.columns))) == 0:
        #    return df_out
        # else:
        #    raise Exception("output columns do not match expected output columns")

    def preprocess(self, df_in):
        df_out = df_in.copy()
        for step in self.preprocess_steps:
            if step == 'normalize':
                df_out = self.norm_cols(df_out, self.cols_to_norm)
            if step == 'fill missing':
                df_out = self.fill_missing_cols(df_out, self.cols_to_fill)
            if step == 'subtract corr':
                df_out = self.sub_corr_cols(df_out, self.corr_traits)
            if step == 'process rm':
                df_out = self.process_rm(df_out)
            if step == 'aggregate cols':
                for cols_to_agg, new_col_name, agg_func in zip(self.to_agg, self.agg_names, self.agg_funcs):
                    df_out = self.aggregate_cols(df_out, cols_to_agg, new_col_name, agg_func)
            if step == 'process text traits':
                df_out = self.text_trait_step(df_out, make_copy=False)
            if step == 'get qualifiers':
                df_out = self.get_qualifiers(df_out, make_copy=False)
            if step == 'make dummies':
                df_out = self.make_dummy_columns(df_out, make_copy=False)

        return df_out

        # functions to reset computed values, like column means.

    def reset_missing_vals(self):
        self.missing_vals = {}

    def reset_norm_vals(self):
        self.norm_vals = {}

    def reset_rm_vals(self):
        self.rm_adv_rate = None
        self.rm_centers = None

    # preprocessing functions
    # agg func should be numpy functions, preferably ones that handle nan's
    def aggregate_cols(self, df_in, cols_to_agg, new_col_name, agg_func, make_copy=False):
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        df_out[new_col_name] = agg_func(df_out[cols_to_agg].values, axis=1)

        return df_out

    def fill_missing_cols(self, df_in, cols):
        df_out = df_in.copy()
        for col in cols:
            if col in df_out.columns:
                df_out = self.fill_missing_col(df_out, col)
        return df_out

    # this function fills missing data within a single column
    def fill_missing_col(self, df_in, col, make_copy=False):
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        # find col median if not in dict
        if col not in self.missing_vals.keys() and self.train_flag == True:
            if pd.api.types.is_numeric_dtype(df_out[col]):
                col_val = self.missing_func(df_out[col].values)
            else:
                col_val = df_out[col].mode()[0]

            self.missing_vals[col] = col_val
        elif col not in self.missing_vals.keys() and self.train_flag == False:
            print(col)
            raise RuntimeError("can't find correct data, might need to train preprocessor")

        # otherwise fill vals
        df_out[col][df_out[col].isna()] = self.missing_vals[col]

        return df_out

    # this function normalizes values in multiple columns by finding mean and std in each column
    def norm_cols(self, df_in, cols):
        df_out = df_in.copy()
        for col in cols:
            df_out = self.norm_col(df_out, col)

        return df_out

    # this function normalizes the values in a column
    # if col and year is not in norm_yrs, find col mean and col std, then normalize
    def norm_col(self, df_in, col, make_copy=False):
        # avoid making a copy of the input, since self.norm_cols makes a copy...
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        if col not in self.norm_vals.keys() and self.train_flag == True:  # find and store col mean and std
            col_mean = np.nanmean(df_out[col].values)
            col_std = np.maximum(np.nanstd(df_out[col].values),
                                 0.00001)  # prevent division by 0
            self.norm_vals[col] = [col_mean, col_std]
        elif col not in self.norm_vals.keys() and self.train_flag == False:
            raise RuntimeError("can't find correct data, might need to train preprocessor")

        df_out[col] = (df_out[col] - self.norm_vals[col][0]) / self.norm_vals[col][1]

        return df_out

    def get_qualifiers(self, df_in, make_copy=False):
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        qualifier_out = np.ones((df_out.shape[0],), dtype=bool)

        for trait in self.qualifier_traits:
            for trait_group in self.qualifier_trait_group_exp:
                is_group = df_out['technology'].str.lower().values == trait_group.lower()
                notnan = df_out[trait].notna().values
                # check for expected trait value
                if np.sum(is_group & notnan) > 0 and trait in self.qualifier_trait_group_exp[trait_group]:
                    qualifier_out[is_group & notnan] = qualifier_out[is_group & notnan] & \
                                                       (df_out.iloc[is_group & notnan][trait].str.lower().values ==
                                                        self.qualifier_trait_group_exp[trait_group][trait])

                # check for values the trait should not be
                if np.sum(is_group & notnan) > 0 and trait in self.qualifier_trait_group_not[trait_group]:
                    if isinstance(self.qualifier_trait_group_not[trait_group][trait], list):
                        for val in self.qualifier_trait_group_not[trait_group][trait]:
                            qualifier_out[is_group & notnan] = qualifier_out[is_group & notnan] & \
                                                               (df_out.iloc[is_group & notnan][
                                                                    trait].str.lower().values !=
                                                                self.qualifier_trait_group_not[trait_group][trait])
                    else:
                        qualifier_out[is_group & notnan] = qualifier_out[is_group & notnan] & \
                                                           (df_out.iloc[is_group & notnan][trait].str.lower().values !=
                                                            self.qualifier_trait_group_not[trait_group][trait])

        df_out['qualifiers'] = qualifier_out
        return df_out

    # include features that remove correlations between traits.
    # do this per year as effect could change across years
    # corr_cols = [
    #    [['PLHTN','ERHTN','GMSTP'],['YGSMN']],
    #    [['ERHTN'],['PLHTN']],
    #    [['GMSTP'],['YGSMN']]
    #    ]
    # x = PLHTN, ERHTN, GMSTP, y= YGSMN
    # x = ERHTN, y = PLHTN
    # x = GMSTP, y = YGSMN
    def sub_corr_cols(self, df_in, corr_traits):
        df_out = df_in.copy()
        for corr_trait in corr_traits:
            df_out = self.sub_corr_col(df_out, x_traits=corr_trait[0], y_trait=corr_trait[1])

        return df_out

    # remove correlations by subtracting effect of one (or many) vars on another
    # y = mx*b
    def sub_corr_col(self, df_in, x_traits, y_trait, make_copy=False):
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        # new col name = y_col - x_cols
        new_col = y_trait[0]
        for trait in x_traits:
            new_col = new_col + '-' + ''.join(trait.split('_')[1:])# new col name = y_col - x_cols

        if self.train_flag == True:
            # get inputs and outputs
            x = df_out[x_traits].values
            y = df_out[y_trait[0]].values

            # drop nan's
            is_nan_mask = (np.any(np.isnan(x), axis=1)) | (np.isnan(y))

            lm = LinearRegression()
            lm.fit(x[is_nan_mask == False, :], y[is_nan_mask == False])
            self.sub_corr_lm[new_col] = lm
        elif new_col not in self.sub_corr_lm and self.train_flag == False:
            raise RuntimeError("linear regression model not trained, try training preprocessor")
        else:
            lm = self.sub_corr_lm[new_col]

        # only output prediction when all inputs are present
        # otherwise, set output as nan
        if np.all([col in df_out.columns for col in x_traits]) and y_trait[0] in df_out.columns: # check for all columns during inference.
            x = df_out[x_traits].values # get x and y
            y = df_out[y_trait[0]].values

            if x.shape[0] > 0 and y.shape[0] > 0:
                is_nan_mask = (np.any(np.isnan(x), axis=1)) | (np.isnan(y)) # remove nan's

                x[is_nan_mask == True] = 0
                y_sub = y - lm.predict(x) # make predictions

                y_sub[is_nan_mask == True] = np.nan # if nan mask, set output as nan
                df_out[new_col] = y_sub
            else:
                df_out[new_col] = np.nan
        else:
            df_out[new_col] = np.nan

        return df_out

    def make_dummy_columns(self, df_in, make_copy=True):
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        df_out = pd.concat(
            (
                df_out,
                pd.get_dummies(df_out[self.make_dummies],prefix='dummy',dtype=int)
            ),
            axis=1
        )

        return df_out

    # RM manipulation
    def process_rm(self, df_in, make_copy=False):
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        # convert RM to an advancement rate per RM. Use as feature. Use decision_group_rm if predicted RM is missing
        df_out['rm_estimate'][df_out['rm_estimate'].isna()] = df_out['decision_group_rm'][df_out['rm_estimate'].isna()]
        if (self.rm_adv_rate is None or self.rm_centers is None) and self.train_flag == True:
            self.rm_adv_rate, self.rm_centers, _ = self.compute_pdf(df_out[self.rm_col], df_out[self.out_col])
        elif (self.rm_adv_rate is None or self.rm_centers is None) and self.train_flag == False:
            raise RuntimeError("can't find correct data, might need to train preprocessor")

        advancement_rate_est = self.interpolate_pdf(self.rm_adv_rate, self.rm_centers, df_out[self.rm_col])

        df_out[self.rm_col + '_proc'] = advancement_rate_est

        return df_out

    # helper function to get gaussian weights based on distance from the mean
    def get_gaussian_weight(self, dists, std):
        return (1) / (std * np.sqrt(2 * np.pi)) * np.exp(-0.5 * dists ** 2 / std ** 2)

    # estimate a PDF across values within x based on the output variable y.
    # example use case: get advancement rates at many estimated_rms. Use this function to "train" an advancement rate function
    # then use interpolatePDF to get output for each input in dataset
    def compute_pdf(self, x, y, std=1):
        notnan_mask = (np.isnan(x) == False) & (np.isnan(y) == False)
        x = x[notnan_mask]
        y = y[notnan_mask]

        x_centers = np.linspace(np.min(x) * 0.9, np.max(x) * 1.1, 250)
        pdf = []
        total_weight = []
        for x_center in x_centers:
            dists = x - x_center
            # get weights for each data point based on distance to mean
            gauss_weights = self.get_gaussian_weight(dists, std=std)
            # advancement rate is weighted average of y based on distance to mean
            pdf.append(np.dot(y, gauss_weights) / np.sum(gauss_weights))
            # also store the total weight
            total_weight.append(np.sum(gauss_weights))

        return np.array(pdf), x_centers, total_weight

    # use this function to get PDF value at x_test.
    # example use case: function to get advancement rate for any arbitrary rm_estimate (simple interpolation)
    # use this function to generate new feature for training and testing data
    def interpolate_pdf(self, pdf, x_centers, x_test):
        return np.interp(x_test, x_centers, pdf)

    def clean_dummy_columns(self, df_dummies, text_trait):
        # this function forces a missing column to exist,
        # forces new categories during testing to show up as missing,
        # and makes sure all expected categories are present, even if filled with 0's

        # force there to be a missing columns
        if text_trait + '_nan' not in df_dummies.columns:
            df_dummies[text_trait + '_nan'] = 0

        # if training, store acceptable columns
        if self.train_flag == True:
            text_trait_cols = list(df_dummies.columns[[text_trait+'_' in col for col in df_dummies.columns]])
            self.text_trait_cols[text_trait] = text_trait_cols
        # if not training, force columns not in the acceptable column list to be nan
        elif self.train_flag == False:
            text_trait_cols = list(df_dummies.columns[[text_trait+'_' in col for col in df_dummies.columns]])
            for col in text_trait_cols:
                if col not in self.text_trait_cols[text_trait]:
                    df_dummies[text_trait + '_nan'][df_dummies[col] != 0] = 1  # write values when present to nan column
                    df_dummies = df_dummies.drop(columns=col)  # drop column

        return df_dummies

    def text_trait_step(self, df_in, make_copy=False):
        # basic preprocessing of text_traits (cleaning up prefixes, suffixes, etc.)
        # make dummies
        if make_copy:
            df_out = df_in.copy()
        else:
            df_out = df_in

        # initialize dictionary storing acceptable cols for each trait
        if self.train_flag == True:
            self.text_trait_cols = {}

        for text_trait in self.text_traits:
            df_out[text_trait] = df_out[text_trait].apply(lambda x: self.text_trait_simple_process(x, text_trait))
            # simple dummy columns for non-troublesome traits
            if text_trait not in ['cn3_t', 'hilct', 'rps_t']:
                df_out = pd.get_dummies(
                    df_out,
                    prefix=text_trait,
                    dummy_na=True,
                    drop_first=True,
                    columns=[text_trait]
                )
                df_out = self.clean_dummy_columns(df_out, text_trait)

        # process troublesome traits
        if 'cn3_t' in self.text_traits and 'cn3_t' in df_out.columns:
            # process troublesome traits
            df_cn3 = self.processCN3_T(df_out['cn3_t'])

            # convert cols to dummy vars, replace empty with susceptible, convert S < MS < MR < R scale to integers
            df_cn3_dummies = pd.get_dummies(
                df_cn3,
                prefix='cn3_t_pre',
                dummy_na=True,
                drop_first=False,
                columns=['cn3_t_pre']
            )
            df_cn3_dummies = self.clean_dummy_columns(df_cn3_dummies, text_trait='cn3_t_pre')

            for col in df_cn3_dummies.columns:
                if 'pre' not in col:
                    df_cn3_dummies[col][df_cn3_dummies[col] == ''] = np.nan
                    df_cn3_dummies[col][df_cn3_dummies[col] == 's'] = 0
                    df_cn3_dummies[col][df_cn3_dummies[col] == 'ms'] = 1
                    df_cn3_dummies[col][df_cn3_dummies[col] == 'mr'] = 2
                    df_cn3_dummies[col][df_cn3_dummies[col] == 'r'] = 3
            df_cn3_dummies = self.clean_dummy_columns(df_cn3_dummies, text_trait='cn3_t')

            # rows should match, just port columns over
            # joining based on index was doing something weird, all of these joins need to be rethought anyway.
            for col in df_cn3_dummies.columns:
                df_out[col] = df_cn3_dummies[col]

        if 'hilct' in self.text_traits and 'hilct' in df_out.columns:
            df_hilct = self.processHILCT(df_out['hilct'])
            # if training, remove columns where count is < 100. Set as Nan column
            if self.train_flag == True:
                cols_to_remove = df_hilct.columns[df_hilct.sum() < 100]
                if 'hilct_nan' not in df_hilct.columns:
                    df_hilct['hilct_nan'] = False
                df_hilct['hilct_nan'] = (df_hilct['hilct_nan']) | (np.any(df_hilct[cols_to_remove].values, axis=1))
                df_hilct['hilct_nan'] = df_hilct['hilct_nan'].astype(int)
                df_hilct = df_hilct.drop(columns=cols_to_remove)

            # clean up dummy columns, store columns
            df_hilct = self.clean_dummy_columns(df_hilct, text_trait='hilct')
            if 'hilct_n' in df_hilct.columns:
                df_hilct['hilct_nan'][df_hilct['hilct_n'] == 0] = 1  # if we didn't find anything, set as missing

            for col in df_hilct.columns:
                df_out[col] = df_hilct[col]

        if 'rps_t' in self.text_traits and 'rps_t' in df_out.columns:
            # replace empty with susceptible except for U, which is a boolean indicator column, convert S < X < R to integers
            df_rps = self.processRPS_T(df_out['rps_t'])
            for col in df_rps:
                if col != 'rps_t_u':
                    df_rps[col][df_rps[col] == ''] = np.nan

                df_rps[col][df_rps[col] == 's'] = 0
                df_rps[col][df_rps[col] == 'x'] = 1
                df_rps[col][df_rps[col] == 'u'] = 1
                df_rps[col][df_rps[col] == 'r'] = 2

            if 'rps_t_u' in df_rps:
                df_rps = df_rps.drop(columns='rps_t_u')

            for col in df_rps.columns:
                df_out[col] = df_rps[col]

        # if not training, make sure all required columns are present. If not, make column of 0's
        # some should be nan's instead of 0's....
        if self.train_flag == False:
            for trait in self.text_trait_cols:
                for col in self.text_trait_cols[trait]:
                    if col not in df_out.columns:
                        df_out[col] = 0

        return df_out

    def text_trait_simple_process(self, x, text_trait):
        # make sure input is a string, and not nan (usually)
        if not isinstance(x, str):
            return x

        if x == '""':
            return np.nan

        x = x.replace('"', '')
        x = x.lower()
        # remove duplicated entries. Pre and post '--' are the same, take the first
        out = x.split('--')[0]

        # these 3 traits are troublesome, deal with in separate functions.
        # might call here later, currently called afterwards
        if text_trait == 'cn3_t':
            # some entries separate markers with space, some with a dash.
            # replace '-' with a space to keep consistent
            out = out.replace('-', ' ')
            out = out.replace(';', ' ')
            return out
        if text_trait == 'hilct':
            return out
        if text_trait == 'rps_t':
            return out

        # else, do basic preprocessing like remove suffixes and prefixes, or strip some whitespace
        if text_trait in self.text_suff:
            out = out.replace(self.text_suff[text_trait], '').strip()

        if text_trait in self.text_pref:
            out = out.split('_')[-1]
            if len(self.text_pref[text_trait]) > 0:
                out = self.text_pref[text_trait] + '_' + out

        if text_trait in self.clean_text:
            out = out.replace(self.clean_text[text_trait], '').strip()

        return out

    def processCN3_T(self, df_tr_in):
        df_tr = df_tr_in.copy()
        # basic preprocessing of each str
        df_tr = df_tr.apply(lambda x: self.text_trait_simple_process(x, 'cn3_t'))

        # split into multiple columns based on space
        # pre space = one column to be one-hot encoded (future function)
        df_pre = df_tr.apply(lambda x: self.splitCN3_T(x, 'pre'))
        df_pre_out = pd.DataFrame(data=df_pre.values, columns=['cn3_t_pre'], index=df_tr_in.index)

        # post space: get resistance entry for each race.
        df_post = df_tr.apply(lambda x: self.splitCN3_T(x, 'post'))

        df_post_out = pd.DataFrame(index=df_tr_in.index)
        for i in range(df_post.shape[0]):
            if isinstance(df_post.iloc[i], str) == 1 and len(df_post.iloc[i]) > 0:
                # if not nan and not empty, split by '/'
                x_split = df_post.iloc[i].split('/')
                # iterate through split, grab text before number and number
                for x in x_split:
                    race_list = re.findall(r'\d+', x)
                    if len(race_list) > 0:  # apparently there are some 'MR' entries (without a race)
                        race_num = race_list[0]
                        text = x.replace(race_num, '').strip()
                        # if number is not a column, make it a column. Initialize as all empty.
                        # put text in column corresponding to number
                        col_name = 'cn3_t_' + race_num
                        if col_name not in df_post_out.columns:
                            df_post_out[col_name] = np.empty((df_post.shape[0],), dtype=str)
                        df_post_out[col_name].iloc[i] = text

        df_out = pd.concat((df_pre_out, df_post_out), axis=1)
        return df_out

    def has_numbers(self, in_str):
        return any(char.isdigit() for char in in_str)

    def splitCN3_T(self, x, return_mode='pre'):
        # split trait by space, either return pre or post space
        if not isinstance(x, str):
            return x

        x_split = x.split(' ')
        for temp in x_split:
            has_digits = self.has_numbers(temp)

            if return_mode == 'pre' and not has_digits:
                return temp
            if return_mode == 'post' and has_digits:
                return temp

        return np.nan


    def getRacesCN3_T(self, x):
        if not isinstance(x, str):
            return []
        return re.findall(r'\d+', x)

    # process rps_t
    def processRPS_T(self, df_tr_in):
        df_tr = df_tr_in.copy()
        # basic preprocessing of each str
        df_tr = df_tr.apply(lambda x: self.text_trait_simple_process(x, 'rps_t'))

        # split into multiple columns based on ';'
        df_split = df_tr.apply(lambda x: self.splitRPS_T(x))

        df_out = pd.DataFrame(index=df_tr_in.index)
        for i in range(df_split.shape[0]):
            if isinstance(df_split.iloc[i], list) == 1:
                # iterate through list, grab marker before and resistance after '_'
                for x in df_split.iloc[i]:
                    x_split = x.split('_')
                    marker = x_split[0]
                    if len(x_split) > 1:
                        resistance = x_split[1]
                    else:
                        resistance = 1
                    col_name = 'rps_t_' + marker
                    if col_name not in df_out.columns:
                        df_out[col_name] = np.empty((df_split.shape[0],), dtype=str)
                    df_out[col_name].iloc[i] = resistance

        return df_out

    def splitRPS_T(self, x):
        # split trait by space, either return pre or post space
        if not isinstance(x, str):
            return x

        # remove 'm', as some entries have 'm3a' while others are just '3a'
        x = x.replace('m', '')

        return x.split(';')

    # process hilct
    def processHILCT(self, df_tr_in):
        df_tr = df_tr_in.copy()
        # basic preprocessing of each str
        df_tr = df_tr.apply(lambda x: self.text_trait_simple_process(x, 'hilct'))
        # split into multiple columns based on ';'
        df_split = df_tr.apply(lambda x: self.splitHILCT(x))
        df_out = pd.DataFrame(data=np.zeros((df_split.shape[0],)), columns=['hilct_n'], index=df_tr_in.index)
        for i in range(df_split.shape[0]):
            if isinstance(df_split.iloc[i], list) == 1:
                # get number of hilct entries
                df_out['hilct_n'].iloc[i] = len(df_split.iloc[i])
                # iterate through list, move any i's to the front, grab marker
                for marker in df_split.iloc[i]:
                    if marker != '':  # sometimes we get 'br/'
                        marker = marker.strip()
                        if 'i' in marker:
                            marker = marker.replace('i', '')
                            marker = 'i' + marker

                        col_name = 'hilct_' + marker
                        if marker == 'seg' or marker == 'mixed':
                            df_out['hilct_n'].iloc[i] = 2
                        else:
                            if col_name not in df_out.columns:
                                df_out[col_name] = np.zeros((df_split.shape[0],)).astype(bool)
                            df_out[col_name].iloc[i] = True

        return df_out

    def splitHILCT(self, x):
        # split trait by space, either return pre or post space
        if not isinstance(x, str):
            return x

        # perform a number of replacements
        # commmas, /, + all separate. Replace with /
        # remove extra text and numbers
        # replace imp to i. i = imperfect
        to_replace = {str(i): '' for i in range(0, 10)}
        to_replace[','] = '/'
        to_replace['+'] = '/'
        to_replace['-'] = '/'
        to_replace['few'] = '/'

        to_replace['mix'] = ''
        to_replace['abc'] = ''

        to_replace['gr'] = 'g'
        to_replace['imp'] = 'i'

        for key in to_replace:
            x = x.replace(key, to_replace[key])

        # remove any extra whitespace
        x = x.strip()
        # split by /
        return x.split('/')
