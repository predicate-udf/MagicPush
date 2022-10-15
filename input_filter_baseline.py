from operator import index, is_
import z3
import dis
from util import *
from predicate import *
from pandas_op import *
from interface import *
from table_constraint import *


def is_conjunctive_normal_form(op, and_above=True):
    if and_above and isinstance(op, And):
        return is_conjunctive_normal_form(op.lh, True) and is_conjunctive_normal_form(op.rh, True)
    elif isinstance(op, Or):
        return is_conjunctive_normal_form(op.lh, False) and is_conjunctive_normal_form(op.rh, False)
    elif isinstance(op, BinOp):
        return True
    else:
        return False

def split_conjunctive_normal_form(op):
    if isinstance(op, BoolOp):
        lh,is_disjunctive_lh = split_conjunctive_normal_form(op.lh)
        rh,is_disjunctive_rh = split_conjunctive_normal_form(op.rh)
        if isinstance(op, And): # return a list of list
            if is_disjunctive_lh and is_disjunctive_rh:
                return [lh, rh], False
            elif is_disjunctive_lh:
                return [lh] + rh, False
            elif is_disjunctive_rh:
                return lh + [rh], False
            else:
                return lh + rh, False
        elif isinstance(op, Or): # return a list
            return op, True
    elif isinstance(op, BinOp):
        return op, True
    elif isinstance(op, UnaryOp):
        return op, True
    elif type(op) is bool or isinstance(op, Constant):
        return None, True
    else:
        print(op)
        print(type(op))
        assert(False)
        


# FilterInference
# InnerJoinInference, LeftOuterJoinInference
# GroupByInference -- 
# RenameInference
# PivotInference, GetDummiesInference
# SetItemInference, ChangeTypeInference
# SortValueInference, CopyInference, AppendInference, InitTableInference
# UnPivotInference
# SplitInference -- special lambda
# DropColumnsInference
# TopNInference -- nothing for now
def get_input_filter_baseline(output_filter, op, op_inference, assertion=False):
    if isinstance(op_inference, FilterInference):
        return [And(output_filter, op.condition)]
    elif isinstance(op_inference, InitTableInference) or isinstance(op_inference, SortValuesInference) or isinstance(op_inference, CopyInference):
        return [output_filter]
    elif isinstance(op_inference, AppendInference):
        return [output_filter for i in op.dependent_ops]
    elif isinstance(op_inference, InnerJoinInference):
        cols_left = set({k for k,v in op.input_schema_left.items()})
        cols_right = set({k for k,v in op.input_schema_left.items()})
        if all([col in cols_left for col in get_columns_used(output_filter)]):
            return [output_filter, True]
        elif all([col in cols_right for col in get_columns_used(output_filter)]):
            return [True, output_filter]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True, True]
        """
        cols_left = set({k for k,v in op.input_schema_left.items()})
        cols_right = set({k for k,v in op.input_schema_left.items()})
        cnf = split_conjunctive_normal_form(output_filter)
        if all([col in cols_left for col in get_columns_used(output_filter)]):
            return [output_filter, True]
        elif all([col in cols_right for col in get_columns_used(output_filter)]):
            return [True, output_filter]
        elif is_conjunctive_normal_form(output_filter) and \
            all([clause is None or all([col in cols_left for col in get_columns_used(clause)]) or all([col in cols_right for col in get_columns_used(clause)]) \
                for clause in cnf]):
            left = list(filter(lambda clause: clause is not None and all([col in cols_left for col in get_columns_used(clause)]), cnf))
            right = list(filter(lambda clause: clause is not None and all([col in cols_right for col in get_columns_used(clause)]), cnf))
            return [AllAnd(*left), AllAnd(*right)]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True, True]
        """
    elif isinstance(op_inference, LeftOuterJoinInference):
        cols_left = set({k for k,v in op.input_schema_left.items()})
        # print("cols_left = {}".format(cols_left))
        # print("all cols = {}".format(get_columns_used(output_filter)))
        if all([col in cols_left for col in get_columns_used(output_filter)]):
            return [output_filter, True]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True, True]
    elif isinstance(op_inference, GroupByInference):
        if all([col in op_inference.groupby_cols for col in get_columns_used(output_filter)]):
            return [output_filter]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True]

    elif isinstance(op_inference, SetItemInference):
        # TODO: only replace once
        #return [True]
        if 'uninterpre' in str(eval_lambda(op_inference.apply_func, op_inference.return_type, op_inference.input_tables[0][0].values).v):
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True]
        else:
            return [get_filter_replacing_field(output_filter, {op_inference.new_col: Expr(op_inference.apply_func)})]
    elif isinstance(op_inference, RenameInference):
        reversed_rename = dict([v,k] for k, v in op.name_map.items())
        return [get_filter_replacing_field(output_filter, reversed_rename)]
    elif isinstance(op_inference, ChangeTypeInference):
        return [get_filter_replacing_field(output_filter, {op.new_col: Expr('lambda x: {}(x)'.format(op.target_type))})]
    elif isinstance(op_inference, DropColumnsInference):
        col_used = get_columns_used(output_filter)
        if not any([col in op.cols for col in col_used]):
            return [output_filter]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True]
    elif isinstance(op_inference, PivotInference):
        index_cols = op_inference.index_col if type(op_inference.index_col) is list else [op_inference.index_col]
        if all([col in index_cols for col in get_columns_used(output_filter)]):
            return [output_filter]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True]
    elif isinstance(op_inference, UnpivotInference):
        index_cols = op_inference.id_vars if type(op_inference.id_vars) is list else [op_inference.id_vars]
        if all([col in index_cols for col in get_columns_used(output_filter)]):
            return [output_filter]
        else:
            if assertion:
                assert(False), "Fail to pushdown at {}".format(type(op))
            else:
                return [True]
    else:
        if assertion:
            assert(False), "Fail to pushdown at {}".format(type(op))
        else:
            if isinstance(op_inference, CrosstableUDFInference):
                return [True, True]
            return [True]
