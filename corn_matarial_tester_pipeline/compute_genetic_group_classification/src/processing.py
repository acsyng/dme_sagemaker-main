"""
This recipe gets fada group classifications via a sql query

"""

import json
import os

from libs.cmt_lib import cmt_sql_recipes


def compute_genetic_group_classification(write_outputs=1):
    # crop id = 9 by default, which is corn
    # call sql query
    df = cmt_sql_recipes.get_rv_v_p_genetic_groups() # default is 9
    # write output
    if write_outputs == 1:
        out_dir = '/opt/ml/processing/data/'
        out_fname = 'genetic_group_classification.csv'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        df.to_csv(os.path.join(out_dir, out_fname), index=False)


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)
        compute_genetic_group_classification()


if __name__ == '__main__':
    main()
