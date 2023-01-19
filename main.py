'''
Created on Jan 19, 2023

@author: Douglas Jacobson
'''


import src.data.data_reader as dr
import src.data.data_reduction_pandas as dr_pandas

def run_pipeline(name):

    # Initialize data object with two data files.
    data_reader = dr.DataReader()
    data_reader.add_df_to_dict('dataset1')
    data_reader.add_df_to_dict('dataset2')

    data_reducer_pandas = dr_pandas.DataReductionPandas(data_reader)
    data_reducer_pandas.run_data_reduction()



if __name__ == '__main__':
    run_pipeline('Running Python data pipeline.')


