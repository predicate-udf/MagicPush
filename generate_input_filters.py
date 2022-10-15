import z3
import dis
from util import *
from predicate import *
# from constraint import *
from pandas_op import *
#from infer_schema import *
from interface import *
from table_constraint import *
import copy

def get_previous_op(ops, op):
    res = []
    for o in ops:
        if op in o.dependent_ops:
            res.append(o)
    return res

def generate_output_filter_from_previous(op, ops):
    previous_ops = get_previous_op(ops, op)
    if len(previous_ops) == 0: # the last operator in the data pipeline.
        return {None:None}
    else:
        output_filters = {}
        for p_op in previous_ops:
            idx = p_op.dependent_ops.index(op)
            output_filters[p_op] = p_op.inference.input_filters[idx]
        return output_filters


def get_component_from_Op(op, infer, F):
    # InitTable
    # Filter
    # InnerJoin
    # LeftOuterJoin
    # GroupBy
    # DropDuplicate
    # Pivot
    # SetItem
    # SortValue
    # DropNA
    # FillNA
    # Rename
    # Copy
    # DropColumns
    # ChangeType
    # Append
    # UnPivot
    # Split
    # TopN
    # GetDummies
    # AllAggregate
    # ScalarComputation
    # SetItemWithDependency
    # SubPipeline
    comp = []
    if isinstance(infer, FilterInference) :
        cond = get_filter_removing_nonfieldorconst(infer.condition)
        comp.append(cond)
    elif isinstance(infer, GroupByInference):
        lambdas = []
        if type(infer.aggr_func_map) is dict:
            for col, aggr_func_pair in infer.aggr_func_map.items():
                init_value, aggr_func = aggr_func_pair
                lambdas.append(aggr_func)
        else:
            init_value, aggr_func = infer.aggr_func_map
            lambdas.append(aggr_func)
        for aggr_func in lambdas:
            if aggr_func.startswith('lambda'):
                    try:
                        preds = get_predicate_from_lambda(aggr_func)
                        if len(preds) > 0:
                            comp.extend(preds)
                    except:
                        continue
    return comp

def get_component_from_F(op, infer, F):
    # InitTable -- check
    # Filter -- check
    # InnerJoin -- check
    # LeftOuterJoin -- check
    # GroupBy
    # DropDuplicate
    # Pivot
    # SetItem -- check
    # SortValue -- check
    # DropNA -- check
    # FillNA -- check
    # Rename -- checke
    # Copy -- check
    # DropColumns -- check
    # ChangeType -- check
    # Append -- check
    # UnPivot
    # Split -- check
    # TopN -- check
    # GetDummies
    # AllAggregate
    # ScalarComputation
    # SetItemWithDependency -- check
    # SubPipeline

    if isinstance(infer, FilterInference) or isinstance(op, Copy) or isinstance(op, InitTable) or isinstance(op, SortValues) or isinstance(op, Append) or isinstance(op, TopN) or isinstance(op, SubpipeInput):
        F = get_filter_removing_nonfieldorconst(F)
        if isinstance(op, Append):
            return [F for i in range(len(op.dependent_ops))]
        else:
            return [F]

    elif isinstance(op, InnerJoin) or isinstance(op, LeftOuterJoin):
        columns_left = set(op.input_schema_left.keys())
        columns_right = set(op.input_schema_right.keys())
        columns_left_new =[]
        columns_right_new =[] 
        for i in columns_left:
            if i in columns_right and i not in infer.merge_cols_left:
                columns_left_new.append(i+"_x")
            else:
                columns_left_new.append(i)
        for i in columns_right:
            if i in columns_left and i not in infer.merge_cols_right:
                columns_right_new.append(i+"_y")
            else:
                columns_right_new.append(i)
        columns_left = set(columns_left_new)
        columns_right = set(columns_right_new)
        columns_used_in_left = set(get_columns_used(F)).intersection(columns_left).difference(set(infer.merge_cols_left))
        columns_used_in_right = set(get_columns_used(F)).intersection(columns_right).difference(set(infer.merge_cols_right)) 
        if len(columns_used_in_right) == 0:
            input_filter_left = F
        else:
            input_filter_left = get_filter_removing_unused(F, columns_used_in_right)
        if len(columns_used_in_left) == 0:
            input_filter_right = F
        else:
            input_filter_right = get_filter_removing_unused(F, columns_used_in_left)
        replace_name_left = {}
        replace_name_right = {}
        for i in columns_used_in_left:
            if "_x" in i:
                new_col = i
                original_col = i[:-2]
                replace_name_left[new_col] = original_col
        for i in columns_used_in_right:
            if "_y" in i:
                new_col = i
                original_col = i[:-2]
                replace_name_right[new_col] = original_col
        if len(replace_name_left) >0:
            input_filter_left = get_filter_replacing_field(input_filter_left, replace_name_left)
        if len(replace_name_right) > 0:
            input_filter_right = get_filter_replacing_field(input_filter_right, replace_name_right)
        return [input_filter_left, input_filter_right]

    elif isinstance(infer, SetItemInference): # changetype, fillna, setitem, setitemwithdependency
        if isinstance(op, SetItemWithDependency):
            return [True] # TODO
        else:
            return [get_filter_replacing_field(F, {infer.new_col: Expr(infer.apply_func, infer.return_type)})]
    elif isinstance(infer, RenameInference):
        reversed_rename = dict([v,k] for k, v in op.name_map.items())
        return [get_filter_replacing_field(F, reversed_rename)]
    elif isinstance(op, Split): # same as setitem, TODO: replace using SetItem
        cols_used = get_columns_used(F)
        for c in cols_used:
            if c in op.new_col_names:
                pos = op.new_col_names.index(c)
                if op.regex is None and op.by is not None:
                    split_lambda = 'lambda x:x["{}"].split("{}")[{}]'.format(op.column_to_split, op.by, pos)
                elif op.regex is not None:
                    split_lambda = 'lambda x:list(filter(None, re.split(r"{}", x["{}"])))[{}]'.format(op.regex, op.column_to_split, pos)
                F = get_filter_replacing_field(F, {c: Expr(split_lambda, 'str')})
        return [F]
    elif isinstance(op, DropColumns):
        return [get_filter_removing_unused(F, op.cols)]

    # Pivot
    elif isinstance(op, Pivot):
        cols_used = get_columns_used(F) 
        # index_col -- list, same
        # header_col -- one-col, output cannot 
        # value_col -- one-col, one-to-many
        # col1==v1 -> header_col==col1 && value_col==v1
        if all([c in op.index_col for c in cols_used]):
            return [F]
        else:
            new_pred = get_filter_replacing_for_pivot(F, op.index_col, op.header_col, op.value_col)
            return [new_pred]

    # UnPivot
    elif isinstance(op, UnPivot): 
        # id_vars -- list, same
        # value_vars -- all cols in value_vars map to var_name/value_name
        # var_name
        # value_name
        # var_name==v1 -> ignore
        # value_name==v1 -> (col1==v1 | col2==v1 | ...) for coli in value_vars
        columns_used = get_columns_used(F)
        columns_in_idvar = set(columns_used).intersection(op.id_vars)
        if all([c in op.id_vars for c in cols_used]):
            return [F]
        else:
            new_pred = get_filter_replacing_for_unpivot(F, columns_in_idvar, op.value_name, op.var_name, op.value_vars)
            return [new_pred]

    # GetDummies
    # TODO: only deal with one-col for now
    # col1==v1 (v1==1/0) -> (dummy_col==v1) if v1==1 else (dummy_col!=v1)
    elif isinstance(op, GetDummies):
        dummy_col = op.cols[0]
        cols_used = get_columns_used(F)
        columns_in_idvar = set(columns_used).difference(set([dummy_col]))
        if all([c!=dummy_col for c in cols_used]):
            return [F]
        else:
            new_pred = get_filter_replacing_for_getdummies(F, dummy_col)
            return [new_pred]

    elif isinstance(infer, GroupByInference):
        cols_used = get_columns_used(F)
        if all([c in infer.groupby_cols for c in cols_used]):
            return [F]
        # aggr_func_map #{column_name, (init_value, aggr_func)}
        # new_column_names #{old_column_name, new_column_name}
        # convert_to_tuple # lambda 
        
        # candidate 1: remove unused
        candidates = [get_filter_removing_unused(F, set(cols_used).difference(set(infer.groupby_cols)))]
        if type(infer.aggr_func_map) is dict:
            p = F
            replaced = False
            for col, aggr_func_pair in infer.aggr_func_map.items():
                init_value, aggr_func = aggr_func_pair
                if col in infer.new_column_names and infer.new_column_names[col] in cols_used:
                    p = get_filter_replacing_field(p, {infer.new_column_names[col]:col})
                    replaced = True
                elif col in cols_used and col not in op.input_schema: # a new column that may map to many other columns
                    cols_involved = get_column_used_from_lambda(aggr_func)
                    for orig_col in cols_involved:
                        candidates.append(get_filter_replacing_field(p, {col:orig_col}))
            if replaced:
                candidates.append(p)
        else:
            new_col = 'new_aggr_col'
            init_value, aggr_func = infer.aggr_func_map
            cols_involved = get_column_used_from_lambda(aggr_func)
            for col in cols_involved:
                candidates.append(get_filter_replacing_field(F, {new_col: col}))
        return [candidates]


    elif isinstance(op, ScalarComputation):
        return [True for i in range(len(op.dependent_op_to_var_map))] # TODO

    elif isinstance(op, CrosstableUDF):
        cols_used = get_columns_used(F)
        sub_pipe = op.subpipeline
        inference_map = [{op:infer.inferences[j].sub_pipeline[i] for i,op in enumerate(path.operators)} for j,path in enumerate(op.subpipeline.paths)]
        if len(sub_pipe.paths) == 1:
            if all([c != op.new_col for c in cols_used]):
                F_sub = True
                F_direct = F
            else:
                # TODO: F_sub should keep pred like op.new_col==old_col
                F_sub = get_filter_removing_unused(F, set(cols_used).difference(set([op.new_col])))
                F_direct = get_filter_removing_unused(F, [op.new_col])
            print("\t----F_sub = {}".format(F_sub))
            for op_sub in reversed(sub_pipe.paths[0].operators):
                infer_sub = inference_map[0][op_sub]
                if(op_sub == sub_pipe.paths[0].operators[-1]):
                    output_filter_i = {None:F_sub}
                else:
                    output_filter_i = generate_output_filter_from_previous(op_sub, sub_pipe.paths[0].operators)
                output_filter = AllOr(*list(output_filter_i.values()))
                if output_filter is None:
                    output_filter = True
                candidates = generate_input_filters_general(op_sub, infer_sub, output_filter)
                #for candidate in candidates:
                infer_sub.input_filters = candidates[0] # TODO: try multiple
                print("\t----subpipeline {}:\n\t---output = {}, input = {}".format(type(infer_sub), output_filter, ' / '.join([str(x) for x in candidates[0]])))
            
            if type(F_direct) is not bool:
                infer.inferences[0].sub_pipeline[0].input_filters[0] = And(F_direct, infer.inferences[0].sub_pipeline[0].input_filters[0])
            infer.inferences[0].input_filters = [\
                infer.inferences[0].sub_pipeline[0].input_filters[0], \
                infer.inferences[0].sub_pipeline[1].input_filters[0], \
                ] # TODO: need to set to row/table filters in case they're not the 1st/2nd op in the subpipeline
        return infer.inferences[0].input_filters

    #elif isinstance(op, GroupedMap):
    

import itertools
def generate_input_filters_general(op, infer, F, output_schemas = {}):
    comps1 = get_component_from_Op(op, infer, F)
    #print("comps1 = {}".format(comps1))
    comps2 = get_component_from_F(op, infer, F)
    #print("comps2 = {}".format(comps2))
    candidates_per_table = []
    all_candidates = []
    if all([type(x) is not list for x in comps2]): # single candidate for each table
        if len(comps1) > 0:
            for add_pred in comps1:
                all_candidates.append([And(add_pred, candidate_F) for candidate_F in comps2])
        else:
            all_candidates.append(comps2)
    else:
        for candidate_F in itertools.product(*[x if type(x) is list else [x] for x in comps2]):
            if len(comps1) > 0:
                for add_pred in comps1:
                    all_candidates.append([And(add_pred, candidate_F_) for candidate_F_ in list(candidate_F)])
            else:
                all_candidates.append(list(candidate_F))
    return all_candidates
    
    # if len(candidates_per_table) == 1:
    #     return candidates_per_table
    # elif all([len(x)==1 for x in candidates_per_table]):
    #     return candidates_per_table
    # else:
    #     assert(False) # TODO
