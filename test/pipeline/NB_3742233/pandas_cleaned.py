import pandas as pd
import pickle

data = pd.read_csv('Eviction_Notices.csv', parse_dates=['File Date'], low_memory=False)
#pickle.dump(data, open('Eviction_Notices.pickle','wb'))

data.rename(columns={col: col.lower().replace(' ', '_') for col in data.columns}, inplace=True)

to_drop = ['constraints_date','address','city','state', 'eviction_notice_source_zipcode','constraints_date','supervisor_district','neighborhoods_-_analysis_boundaries','location']

subset = data.drop(to_drop, axis=1)
subset['year'] = subset.apply(lambda row: row.file_date.year, axis=1)
subset = subset[subset.file_date.dt.year < 2019]
melted = subset.melt(id_vars=['eviction_id','year'], \
    value_vars=['non_payment', 'breach', 'nuisance', 'illegal_use', 'failure_to_sign_renewal', 'access_denial', 'unapproved_subtenant', 'owner_move_in', 'demolition', 'capital_improvement', 'substantial_rehab', 'ellis_act_withdrawal', 'condo_conversion', 'roommate_same_unit', 'other_cause', 'late_payments', 'lead_remediation', 'development', 'good_samaritan_ends'],\
    var_name='eviction_reason', value_name='status')
filtered = melted[melted.status == True]

target_vars = ['eviction_reason', 'year']
grouped = filtered.groupby(by=target_vars).eviction_id.count().reset_index().sort_values(target_vars)
top3 = grouped[grouped.eviction_reason.isin(['owner_move_in', 'breach', 'nuisance'])]
top3_pivoted = top3.pivot(
    index='year',
    columns='eviction_reason',
    values='eviction_id',
)
print(top3_pivoted)