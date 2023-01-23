'''
Created on Jan 19, 2023

@author: Douglas Jacobson
'''


import src.data.data_helper as data_helper
import src.data.data_reduction_pandas as dr_pandas
import src.data.data_reduction_apache_beam as dr_beam

def run_pipeline(name):

    # Initialize data object with two data files.
    dh = data_helper.DataHelper()
    dh.add_df_to_dict('dataset1')
    dh.add_df_to_dict('dataset2')

    # Generate csv file using Pandas
    data_reducer_pandas = dr_pandas.DataReductionPandas(dh)
    # data_reducer_pandas.run_data_reduction()


    # Generate csv files using Apache Beam
    dh2 = data_helper.DataHelper()
    dh2.add_csv('ds1')
    dh2.add_csv('ds2')

    data_reducer_beam = dr_beam.DataReductionApacheBeam(dh2)
    data_reducer_beam.run_beam_pipeline()



if __name__ == '__main__':
    run_pipeline('Running Python data pipeline.')


