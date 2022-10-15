import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

data = InitTable('Eviction_Notices.pickle')
data1 = Rename(data, {'Eviction ID': 'eviction_id', 'Address': 'address', 'City': 'city', 'State': 'state', 'Eviction Notice Source Zipcode': 'eviction_notice_source_zipcode', 'File Date': 'file_date', 'Non Payment': 'non_payment', 'Breach': 'breach', 'Nuisance': 'nuisance', 'Illegal Use': 'illegal_use', 'Failure to Sign Renewal': 'failure_to_sign_renewal', 'Access Denial': 'access_denial', 'Unapproved Subtenant': 'unapproved_subtenant', 'Owner Move In': 'owner_move_in', 'Demolition': 'demolition', 'Capital Improvement': 'capital_improvement', 'Substantial Rehab': 'substantial_rehab', 'Ellis Act WithDrawal': 'ellis_act_withdrawal', 'Condo Conversion': 'condo_conversion', 'Roommate Same Unit': 'roommate_same_unit', 'Other Cause': 'other_cause', 'Late Payments': 'late_payments', 'Lead Remediation': 'lead_remediation', 'Development': 'development', 'Good Samaritan Ends': 'good_samaritan_ends', 'Constraints Date': 'constraints_date', 'Supervisor District': 'supervisor_district', 'Neighborhoods - Analysis Boundaries': 'neighborhoods_-_analysis_boundaries', 'Location': 'location'})
subset = DropColumns(data1, ['address','city','state', 'eviction_notice_source_zipcode','constraints_date','supervisor_district','neighborhoods_-_analysis_boundaries','location'])
subset1 = SetItem(subset, 'year', 'lambda row: row["file_date"].year')
subset2 = Filter(subset1, BinOp(Field('year'),'<',Constant(2019)))
melted = UnPivot(subset2, ['eviction_id','year'], ['non_payment', 'breach', 'nuisance', 'illegal_use', 'failure_to_sign_renewal', 'access_denial', 'unapproved_subtenant', 'owner_move_in', 'demolition', 'capital_improvement', 'substantial_rehab', 'ellis_act_withdrawal', 'condo_conversion', 'roommate_same_unit', 'other_cause', 'late_payments', 'lead_remediation', 'development', 'good_samaritan_ends'],\
    var_name='eviction_reason', value_name='status')
filtered = Filter(melted, BinOp(Field('status'),'==',Constant(True)))

grouped = GroupBy(filtered, ['eviction_reason', 'year'], {'eviction_id':'count'},{'eviction_id':'eviction_id'})
grouped1 = SortValues(grouped, ['eviction_reason', 'year'])
top3 = Filter(grouped1, BinOp(Field('eviction_reason'),'in',Constant(['owner_move_in', 'breach', 'nuisance'])))
top3_pivoted = Pivot(top3, ['year'],'eviction_reason','eviction_id',None,{'breach':'int', 'nuisance':'int', 'owner_move_in':'int'})

