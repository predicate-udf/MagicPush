import sys
sys.path.append("../../../")
from interface import *

"""
select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  customer,
  orders,
  lineitem
where
  o_orderkey in (
    select
      l_orderkey
    from
      lineitem
    group by
      l_orderkey
    having
      sum(l_quantity) > 300
    )  Yin: this operation is not supported
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate,
  o_orderkey
limit 100
"""


customer = InitTable('data/customer.csv')
orders = InitTable('data/orders.csv')
lineitem = InitTable('data/lineitem.csv')
op1 = InnerJoin(customer, orders, ["c_custkey"],["o_custkey"])
op2 = InnerJoin(op1, lineitem, ["o_orderkey"],["l_orderkey"])
op3 = GroupBy(op2, ['c_name','c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'], \
    {
      'l_quantity':(Value(0),'sum')}, 
	 {'l_quantity':'sum_quantity'})
op4 = TopN(op3, 100, ["o_totalprice","o_orderdate","o_orderkey"])



# op1 = InitTable('data/lineitem.csv')
# op2 = GroupBy(op1, ['l_orderkey'], \
#     {
#       'l_quantity':(Value(0),'sum')}, 
# 	 {'l_quantity':'sum_quantity'})
# op3 = Filter(op2, BinOp(Field('sum_quantity'), '>', Constant(300)))
# op4 = DropColumns(op3, ["l_orderkey"])
# op5 = InitTable('data/customer.csv')
# # in xxx query result not supported
# op6 = InitTable('data/orders.csv')
# op7 = InitTable('data/lineitem.csv')
# op8 = InnerJoin(op5, op6, ["c_custkey"],["o_custkey"])
# op9 = InnerJoin(op8, op7, ["o_orderkey"],["l_orderkey"])
# op10 = Filter(op9, BinOp(Field('l_commitdate'), 'in', op4))
# op11 = GroupBy(op10, ['c_name','c_custkey', 'o_orderkey', 'o_orderdate', 'o_totalprice'], \
#     {
#       'l_quantity':(Value(0),'sum')}, 
# 	 {'l_quantity':'sum_quantity'})

# op12 = TopN(op11, 100, ["o_totalprice","o_orderdate","o_orderkey"])




