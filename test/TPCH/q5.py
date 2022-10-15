import sys
sys.path.append("../../../")
from interface import *

"""
select
  n_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
where
  c_custkey = o_custkey k
  and l_orderkey = o_orderkey k
  and l_suppkey = s_suppkey k
  and c_nationkey = s_nationkey k
  and s_nationkey = n_nationkey k
  and n_regionkey = r_regionkey k
  and r_name = 'ASIA' (f) k
  and o_orderdate >= '1994-01-01' (f) k
  and o_orderdate < '1995-01-01' (f) k 
group by
  n_name
order by
  revenue desc
;
"""
op1 = InitTable('data/region.csv')
op3 = InitTable('data/orders.csv')
op6 = InitTable('data/customer.csv')
op7 = InitTable('data/lineitem.csv')
op8 = InitTable('data/supplier.csv')
op9 = InitTable('data/nation.csv')
op10 = InnerJoin(op6, op3, ['c_custkey'],['o_custkey'])
op11 = InnerJoin(op7, op10, ['l_orderkey'],['o_orderkey'])
op12 = InnerJoin(op11, op8, ['l_suppkey'],['s_suppkey'])
op13 = Filter(op12, BinOp(Field('c_nationkey'), '==', Field('s_nationkey')))
op14 = InnerJoin(op13, op9, ['s_nationkey'],['n_nationkey'])
op15 = InnerJoin(op14, op1, ['n_regionkey'],['r_regionkey'])
op16 = Filter(op15, BinOp(Field('o_orderdate'),'subset',Constant('1994-01-01')))
op17 = Filter(op16, BinOp(Field('o_orderdate'),'subset',Constant('1995-01-01')))
op18 = Filter(op17, BinOp(Field('r_name'),'==',Constant('ASIA')))
op19 = GroupBy(op18, ['n_name'], \
    {
     'revenue':(Value(0), 'lambda row: row["l_extendedprice"]*(1-row["l_discount"])')}, 
	 {'revenue':'revenue'})
op20 = SortValues(op19, ['revenue'])