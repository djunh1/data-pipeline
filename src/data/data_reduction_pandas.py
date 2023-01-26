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

        df_merged = self.combine_data_frames(df_dict["dataset1"], df_dict['dataset2'])

        # [A] For each legal entity, group by counterparty's max rating, and the sum of each associated status
        df_legal_entity = self.generate_df_for_legal_enties(df_merged)
        self.data_helper.generate_csv(df_legal_entity, 'legal-entities')


        # [B] Totals for legal_entities, counterparties and tiers
        df_full = self.generate_full_data_df(df_merged)
        self.data_helper.generate_csv(df_full, 'full-data')

        print('All data has been generated and saved to %s' % self.data_helper.save_path)


    def generate_full_data_df(self, df):
        '''
        Aggregates the data against legal_entities, counter_parties, and tiers. status are added for each
        column type.

        :param df:
        :return: df
        '''

        # Each respective column will have status's aggregated

        df_status_legal_entity = self.create_status_aggregate(df, "legal_entity")
        df_status_counter_party = self.create_status_aggregate(df, "counter_party")
        df_status_tier = self.create_status_aggregate(df, "tier")


        df1 = df.groupby(['legal_entity'], as_index=False)\
                .agg({'counter_party': 'size', 'tier': 'size', 'rating': 'max'})
        df1 = pd.merge(df1, df_status_legal_entity, on="legal_entity")

        df2 = df.groupby(['legal_entity', 'counter_party'], as_index=False).agg({ 'tier': 'size', 'rating': 'max'})
        df2 = pd.merge(df2, df_status_legal_entity, on="legal_entity")


        df3 = df.groupby(['counter_party'], as_index=False)\
                .agg({'legal_entity': 'size', 'tier': 'size', 'rating': 'max'})
        df3 = self.swap_columns(df3, 'counter_party', 'legal_entity')
        df3 = pd.merge(df3, df_status_counter_party, on="counter_party")

        df4 = df.groupby(['tier'], as_index=False)\
                .agg({'legal_entity': 'size', 'counter_party': 'size', 'rating': 'max'})
        df4 = self.swap_columns(df4, 'legal_entity', 'tier')
        df4 = self.swap_columns(df4, 'counter_party', 'tier')
        df4 = pd.merge(df4, df_status_tier, on="tier")

        result = pd.concat([df1, df2, df3, df4])

        return result


    def generate_df_for_legal_enties(self, df):
        '''
        Generates a dataframe with max counterparty results grouped by legal entity. Status sums added for each
        :param df:
        :return: df
        '''

        df_rating = df.groupby(['legal_entity', 'counter_party', 'tier'], as_index=False)\
                      .agg({'rating': 'max'})\
                      .rename(columns={"rating": "max_rating_by_counterparty"})
        df_status_by_legal_entity = self.create_status_aggregate(df, "legal_entity")
        result = pd.merge(df_rating, df_status_by_legal_entity, on="legal_entity")

        return result

    def create_status_aggregate(self, df, aggregate_type):
        '''
        Status is aggregated commonly against the appropriate column such as legal_entity etc.
        :param df:
        :param aggregate_type:
        :return: df (with each status summed up)
        '''
        df_status = df.groupby(['%s' % aggregate_type, 'status'], as_index=False)\
                                       .agg({'value': 'sum'})\
                                       .pivot(index='%s' % aggregate_type, columns='status',  values='value')\
                                       .reset_index()
        df_status = self.swap_columns(df_status, 'ACCR', 'ARAP')
        return df_status


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








