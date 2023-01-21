'''
Created on Jan 19, 2023

@author: douglasjacobson

Performs data operation using pandas
'''

import pandas as pd

class DataReductionPandas(object):
    def __init__(self, data_helper):
        self.data_helper = data_helper


    def run_data_reduction(self):
        df_dict = {}
        for k, v in self.data_helper.df_dict.items():
            df_dict[k] = v

        # For each legal entity, group by counterparty's max rating, and the sum of each associated status
        df_merged = self.combine_data_frames(df_dict["dataset1"], df_dict['dataset2'])
        df_legal_entity = self.generate_df_for_legal_enties(df_merged)
        self.data_helper.generate_csv(df_legal_entity, 'legal-entities')








    def generate_df_for_legal_enties(self, df):
        '''
        Generates a dataframe with max counterparty results grouped by legal entity. Status sums added for each
        :param df:
        :return: df
        '''

        df_rating = df.groupby(['legal_entity', 'counter_party', 'tier'], as_index=False).agg(
            {'rating': 'max'})
        df_rating = df_rating.rename(columns={"rating": "max_rating_by_counterparty"})
        df_grouped_by_legal_entity = df.groupby(['legal_entity', 'status'], as_index=False).agg({'value': 'sum'})
        df_status = df_grouped_by_legal_entity.pivot(index='legal_entity', columns='status', values='value').reset_index()
        df_status = self.swap_columns(df_status,'ACCR', 'ARAP')
        result = pd.merge(df_rating, df_status, on="legal_entity")

        return result


    def swap_columns(self, df, col1, col2):
        '''
        Helper method to swap a column
        :param df:
        :param col1:
        :param col2:
        :return: df
        '''
        col_list = list(df.columns)
        x, y = col_list.index(col1), col_list.index(col2)
        col_list[y], col_list[x] = col_list[x], col_list[y]
        df = df[col_list]
        return df

    def combine_data_frames(self, df1, df2):
        df_merged = pd.merge(df1, df2, on='counter_party')
        df_merged = df_merged.drop(['invoice_id'], axis=1)
        return df_merged








