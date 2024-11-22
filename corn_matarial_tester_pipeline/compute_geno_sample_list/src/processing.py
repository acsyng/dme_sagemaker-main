"""
This recipe gets geno sample list via a sql query

"""

import json
import os

from libs.cmt_lib import cmt_sql_recipes


def compute_geno_sample_list(crop_id=9, write_outputs=1):
    print("environment variable value is")
    print(os.getenv('ENVIRONMENT'))
    # crop id = 9 by default, which is corn
    # call sql query
    df = cmt_sql_recipes.get_geno_sample_list(crop_id=crop_id) # default is 9
    # write output
    if write_outputs == 1:
        out_dir = '/opt/ml/processing/data/'
        out_fname = 'geno_sample_list.csv'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        df.to_csv(os.path.join(out_dir, out_fname), index=False)


def main():
    with open('/opt/ml/processing/input/input_variables/query_variables.json', 'r') as f:
        data = json.load(f)

        # currently removed, this could allow us to run cmt for different crops, but it's not needed so not implemented
        #crop_id = data['crop_id']

        compute_geno_sample_list()


if __name__ == '__main__':
    main()
