SYMBOLIC_MODE=False
import z3

global_uninterpreted_functions = {}

subset_string_f = z3.Function('string_subset', z3.StringSort(), z3.StringSort(), z3.BoolSort())
