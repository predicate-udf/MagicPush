import operator
import z3
import sys
sys.path.append('../')
from util_type import *

def is_symbolic_expr(a):
    return isinstance(a, Value) or isinstance(a, z3.ExprRef)

def is_symbolic(*arg):
    for a in list(arg):
        if is_symbolic_expr(a):
            return True
        if type(a) is list or type(a) is tuple or type(a) is set:
            return any([is_symbolic(x) for x in list(a)])
    return False


name_mapping = {
    'POSITIVE': '+',
    'NEGATIVE': '-',
    'NOT': '~',
    'MULTIPLY': '*',
    'DIVIDE': '/',
    'MODULO': '%',
    'ADD': '+',
    'SUBTRACT': '-',
    'SUBSCR': 'scr',
    'AND': '&',
    'OR': '|',
    0: '<',
    1: '<=',
    2: '==',
    3: '!=',
    4: '>',
    5: '>=',
    6: 'in',
    7: 'not in',
    'INT': 'int',
} 

def symb_unaryop(x, op):
    if is_symbolic(x):
        if op == 'pos':
            ret = z3.If(getv(x)>=0, getv(x), 0-getv(x))
        elif op == 'neg':
            ret = z3.If(getv(x)<0, getv(x), 0-getv(x))
        elif op == 'not':
            ret = z3.Not(getv(x))
        elif op == 'repr':
            ret = str(getv(x))
        elif op == 'invert':
            ret = z3.Not(getv(x))
        elif op == 'int':
            ret = z3.StrToInt(getv(x))
        elif op == 'str':
            ret = z3.IntToStr(getv(x))
        return Value(ret, get_isnull(x))
    else:
        if op == 'pos':
            return operator.pos(x)
        elif op == 'neg':
            return operator.neg(x)
        elif op == 'not':
            return not x
        elif op == 'repr':
            return repr(x)
        elif op == 'invert':
            return operator.invert(x)
        elif op == 'int':
            return int(x)
        elif op == 'str':
            return str(x)
    
UNARY_OPERATORS = {
        'POSITIVE': lambda x: symb_unaryop(x,'pos'), #operator.pos,
        'NEGATIVE': lambda x: symb_unaryop(x,'neg'), #operator.neg,
        'NOT':      lambda x: symb_unaryop(x,'not'), #operator.not_,
        'CONVERT':  lambda x: symb_unaryop(x,'repr'), #repr,
        'INVERT':   lambda x: symb_unaryop(x,'invert'), #operator.invert,
        'INT': lambda x: symb_unaryop(x,'int'), #lambda x: int(x),
        'STR': lambda x: symb_unaryop(x,'str'), #lambda x: str(x),
    }

def get_default_value_by_type(x):
    if type(x) is str:
        return 'str'
    else:
        return -1

def symb_getitem(x, y):
    #if is_symbolic(x):
    #    assert(False)
    if is_symbolic(x) or is_symbolic(y):
        if type(x) in [list, tuple]:
            ret = get_default_value_by_type(x[0])
            for i,v in enumerate(x):
                ret = z3.If(getv(y)==i, getv(v), ret)
            return ret
        elif type(x) is dict:
            ret = get_default_value_by_type([v for k,v in x.items()][0])
            for k,v in x.items():
                ret = z3.If(getv(y)==k, getv(v), ret)
            return ret
        else:
            print(type(x))
            assert(False)
    else:
        return x[y]

def symb_binop(x, y, op):
    if is_symbolic(x) or is_symbolic(y):
        if op=='+':
            ret = getv(x)+getv(y)
        elif op == '-':
            ret = getv(x)-getv(y)
        elif op == '*':
            ret = getv(x)*getv(y)
        elif op == '/':
            ret = getv(x)/getv(y)
        elif op == 'mod':
            ret = getv(x)%getv(y)
        elif op == 'lshift':
            ret = getv(x) << getv(y)
        elif op == 'rshift':
            ret = getv(x) >> getv(y)
        elif op == '&&':
            ret = z3.And(getv(x),getv(y))
        elif op == '||':
            ret = z3.Or(getv(x),getv(y))
        elif op == 'xor':
            ret = z3.Xor(getv(x), getv(y))
        elif op == 'pow':
            ret = pow(getv(x), getv(y))
        nullv = z3.Or(get_isnull(x), get_isnull(y))
        return Value(ret, nullv)
    else:
        if op=='+':
            return x+y
        elif op == '-':
            return x-y
        elif op == '*':
            return x*y
        elif op == '/':
            return x/y
        elif op == 'mod':
            return x%y
        elif op == 'lshift':
            return x << y
        elif op == 'rshift':
            return x >> y
        elif op == '&&':
            return x and y
        elif op == '||':
            return x or y
        elif op == 'xor':
            return operator.xor(x, y)
        elif op == 'pow':
            return pow(x,y)

BINARY_OPERATORS = {
        'POWER':    lambda x,y: symb_binop(x,y,'pow'), #pow,
        'MULTIPLY': lambda x,y: symb_binop(x,y,'*'),#operator.mul,
        'DIVIDE':   lambda x,y: symb_binop(x,y,'/'),#getattr(operator, 'div', lambda x, y: None),
        'FLOOR_DIVIDE': lambda x,y: symb_binop(x,y,'/'),#operator.floordiv,
        'TRUE_DIVIDE':  lambda x,y: symb_binop(x,y,'/'),#operator.truediv,
        'MODULO':   lambda x,y: symb_binop(x,y,'mod'),#operator.mod,
        'ADD':      lambda x,y: symb_binop(x,y,'+'),#operator.add,
        'SUBTRACT': lambda x,y: symb_binop(x,y,'-'),#operator.sub,
        'SUBSCR':   lambda x,y: symb_getitem(x, y), #operator.getitem,
        'LSHIFT':   lambda x,y: symb_binop(x,y,'lshift'),#operator.lshift,
        'RSHIFT':   lambda x,y: symb_binop(x,y,'rshift'),#operator.rshift,
        'AND':      lambda x,y: symb_binop(x,y,'&&'),#operator.and_,
        'XOR':      lambda x,y: symb_binop(x,y,'||'),#operator.xor,
        'OR':       lambda x,y: symb_binop(x,y,'xor'),#operator.or_,
    }

def symb_in(x, y):
    if is_symbolic(x) or is_symbolic(y):
        if type(y) in [list, tuple]:
            return z3.Or(*[getv(x)==getv(y1) for y1 in y])
        elif type(y) is str or type(x) is str:
            return z3.Contains(getv(y), getv(x))
    else:
        return x in y

def symb_compop(x,y,op):
    if is_symbolic(x) or is_symbolic(y):
        if op == '<':
            ret = getv(x)<getv(y)
        elif op == '>':
            ret = getv(x)>getv(y)
        elif op == '<=':
            ret = getv(x)<=getv(y)
        elif op == ">=":
            ret = getv(x)>=getv(y)
        elif op == '!=':
            ret = getv(x)!=getv(y)
        elif op == '==':
            ret = getv(x)==getv(y)
        nullv = z3.Or(get_isnull(x), get_isnull(y))
        return Value(ret, nullv)
    else:
        if op == '<':
            return x < y
        elif op == '>':
            return x > y
        elif op == '<=':
            return x <= y
        elif op == ">=":
            return x >= y
        elif op == '!=':
            return x != y
        elif op == '==':
            return x == y

COMPARE_OPERATORS = [
        lambda x,y: symb_compop(x, y, '<'), #operator.lt, #0
        lambda x,y: symb_compop(x, y, '<='), #operator.le, #1
        lambda x,y: symb_compop(x, y, '=='), #operator.eq, #2
        lambda x,y: symb_compop(x, y, '!='), #operator.ne, #3
        lambda x,y: symb_compop(x, y, '>'), #operator.gt, #4
        lambda x,y: symb_compop(x, y, '>='), #operator.ge, #5
        lambda x, y: symb_in(x, y), #x in y, #6
        lambda x, y: not symb_in(x, y), #x not in y, #7
        lambda x, y: x is y, #8
        lambda x, y: x is not y, #9
        lambda x, y: issubclass(x, Exception) and issubclass(x, y), #10
    ]

def symb_ite(x, y, z):
    if is_symbolic(x) or is_symbolic(y) or is_symbolic(z):
        ret = z3.If(getv(x),  y, z)
        is_null = get_isnull(x)
        return Value(ret, is_null)

TRINARY_OPERATORS = {
    'IF-THEN-ELSE': lambda x, y, z: y if x else z,
}


def symbolic_func_exec(func, posargs, kwargs):
    func_name = func.__name__
    if func_name == 'in':
        return symb_in(posargs[0], posargs[1])
    elif func_name == 'str':
        return Value(z3.IntToStr(posargs[0]), get_isnull(posargs[0]))
    elif func_name == 'isnull':
        if is_symbolic(posargs[0].isnull):
            return Value(posargs[0].isnull, False)
        else:
            return False
    else:
        # try to get the output type
        new_posargs = []
        new_kwargs = {} # TODO: kwargs not handled for now
        arg_types = []
        symb_args = []
        for v in new_posargs:
            if is_symbolic(v):
                symb_args.append(v)
                if isinstance(v, Value) and isinstance(v.v, z3.z3.SeqRef):
                    new_posargs.append('some_str')
                    arg_types.append(z3.StringSort())
                elif isinstance(v, Value) and isinstance(v.v, z3.z3.ArithRef):
                    new_posargs.append(1)
                    arg_types.append(z3.IntSort())
                else:
                    assert(False) # not supported for now
            else:
                new_posargs.append(v)
        for k,v in kwargs.items():
            if is_symbolic(v):
                if isinstance(v, Value) and isinstance(v.v, z3.z3.SeqRef):
                    new_kwargs[k] = 'some_str'
                elif isinstance(v, Value) and isinstance(v.v, z3.z3.ArithRef):
                    new_kwargs[k] = 1
                else:
                    assert(False) # not supported for now
            else:
                new_kwargs[k] = v
        ret = func(*new_posargs, **new_kwargs)
        if type(ret) is str:
            arg_types.append(z3.StringSort())
            return Value(z3.Function('func_{}_{}'.format(func_name, hash(func)%10000), *arg_types)(*symb_args))
        else:
            arg_types.append(z3.IntSort())
            return Value(z3.Function('func_{}_{}'.format(func_name, hash(func)%10000), *arg_types)(*symb_args))