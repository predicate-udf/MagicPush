import z3
import glbs
from lambda_symbolic_exec.op import *

NUMBER = 300001
LOWERLETTER = 300002
UPPERLETTER = 300003

NUMBER_START=48
NUMBER_END=57
LOWERLETTER_START=97
LOWERLETTER_END=122
UPPERLETTER_START=65
UPPERLETTER_END=90

class SymbolicExpr:
    def __str__(self):
        return ""

def to_be_symbolic(v):
    if type(v) is str:
        if v == "str_0x1234567":
            glbs.SYMBOLIC_MODE = True
            return True
    return False


def is_symbolic(*arg):
    for a in list(arg):
        if is_symbolic_expr(a):
            return True
        if type(a) is list or type(a) is tuple or type(a) is set:
            return any([is_symbolic(x) for x in list(a)])
    return False
    
def getv(a):
    if hasattr(a, 'v'):
        return a.v
    elif type(a) is list:
        return [getv(x) for x in a]
    elif type(a) is tuple:
        return tuple([getv(x) for x in list(a)])
    elif type(a) is set:
        return set([getv(x) for x in list(a)])
    return a

def symbolic_str(a):
    if isinstance(a, Var):
        return a.symbolic_str()
    else:
        return a

def toZ3(a):
    if isinstance(a, Var):
        return a.toZ3()
    else:
        return Var(a).toZ3()

def make_symbolic(v):
    global NUMBER_START, NUMBER_END, NUMBER
    global LOWERLETTER_START, LOWERLETTER_END, LOWERLETTER
    global UPPERLETTER_START, UPPERLETTER_END, UPPERLETTER
    assert(type(getv(v)) is str)
    r = []
    for letter in list(getv(v)):
        if ord(letter) >= NUMBER_START and ord(letter) <= NUMBER_END:
            r.append(Symbol(vrange=NUMBER, actual_v=letter))
        elif ord(letter) >= LOWERLETTER_START and ord(letter) <= LOWERLETTER_END:
            r.append(Symbol(vrange=LOWERLETTER, actual_v=letter))
        elif ord(letter) >= UPPERLETTER_START and ord(letter) <= UPPERLETTER_END:
            r.append(Symbol(vrange=UPPERLETTER, actual_v=letter))
        else:
            r.append(Var(letter))
    glbs.important_symbols = r
    return ComposedVar(r)

class Var(SymbolicExpr):
    def __init__(self, v):
        self.v = v
        self.is_symbolic = False
    def getv(self):
        return self.v
    def make_iter(self):
        return ComposedVar(iter([Var(x) for x in list(self.v)]), isiter=True)
    def __setitem__(self, idx, v):
        #TODO
        if is_symbolic(idx):
            pass
        else:
            self.v[getv(idx)] = v
    def slice(self, start, end):
        if start.is_symbolic and end.is_symbolic:
            pass
        elif start.is_symbolic:
            pass
        elif end.is_symbolic:
            pass
        else:
            return Var(self.getv()[getv(start):getv(end)])
    def toZ3(self):
        if type(self.v) is int or type(self.v) is bool:
            return self.v
        elif type(self.v) is str:
            if len(self.v) == 1:
                return ord(self.v[0])
            else:
                return [ord(x) for x in list(self.v)]
        #TODO: How to handle tuple?
        elif type(self.v) is tuple or type(self.v) is list:
            if type(self.v[0]) is str:
                return [ord(x) for x in list(self.v)]
            else:
                return list(self.v)
        elif isinstance(self.v, Var):
            return self.v.toZ3()
        else:
            assert(False)

    def __str__(self):
        return "v-{}".format(self.getv())
    def symbolic_str(self):
        return self.getv()

class Symbol(Var):
    def __init__(self, vrange=None, actual_v=None):
        self.v = actual_v
        glbs.symbol_counter += 1
        self.symbolv = z3.Int("x{}".format(glbs.symbol_counter))
        self.sid = glbs.symbol_counter
        self.vrange = vrange
        self.is_symbolic = True
    def __str__(self):
        return "s-{} <x{}>".format(self.getv(), self.sid)
    def symbolic_str(self):
        return self.symbolv
    def toZ3(self):
        return self.symbolv
    def valuerange_constraints(self):
        global NUMBER, LOWERLETTER, UPPERLETTER
        assert(self.vrange)
        if self.vrange == NUMBER:
            return z3.And(self.symbolv >= NUMBER_START, self.symbolv <= NUMBER_END)
        elif self.vrange == LOWERLETTER:
            return z3.And(self.symbolv >= LOWERLETTER_START, self.symbolv <= LOWERLETTER_END)
        elif self.vrange == UPPERLETTER:
            return z3.And(self.symbolv >= UPPERLETTER_START, self.symbolv <= UPPERLETTER_END)
        assert(False)

class ComposedVar(Var):
    def __init__(self, v, isiter=False):
        self.v = v
        if type(v) is list or type(v) is tuple or type(v) is set:
            self.is_symbolic = any([isinstance(x, SymbolicExpr) and x.is_symbolic for x in list(self.v)]) 
        elif isiter:
            self.is_symbolic = True
        else:
            self.is_symbolic = False
        self.isiter = isiter
    def getv(self):
        if type(self.v) is list:
            ret = [getv(x) for x in self.v]
            #TODO: This is bad...
            if any([is_symbolic(x) for x in self.v]) and all([type(x) is str for x in ret]):
                return ''.join(ret)
            else:
                return ret
        elif type(self.v) is tuple:
            return tuple([getv(x) for x in list(self.v)])
        elif type(self.v) is set:
            return set([getv(x) for x in list(self.v)])
        else:
            return self.v
    def make_iter(self):
        return ComposedVar(iter(self.v), isiter=True)
    def get_iter(self):
        return next(self.v)
    def next(self):
        return self.get_iter()
    def __getitem__(self, idx):
        if isinstance(idx, Symbol):
            pass
        elif isinstance(idx, Var):
            #TODO
            if is_symbolic(idx):
                pass
            else:
                return self.v[idx.v]
        else:
            if self.is_symbolic:
                pass
            else:
                return self.v[idx]
    def __setitem__(self, idx, v):
        if is_symbolic(idx):
            pass
        else:
            self.v[getv(idx)] = v
    def slice(self, start, end):
        if is_symbolic(start, end):
            pass
        else:
            return ComposedVar(self.v[getv(start):getv(end)])
    def __str__(self):
        if self.isiter:
            return "cv-iter"
        else:
            #return "cv-[{}]".format(','.join(["{}".format(x) for x in self.v]))
            return "cv-{} <{}>".format(self.getv(), "[{}]".format(','.join(["{}".format(x) for x in list(self.v)])))
    def symbolic_str(self):
        if type(self.v) is list:
            return "[{}]".format(','.join(["{}".format(symbolic_str(x) if isinstance(x, SymbolicExpr) else x) for x in self.v]))
        elif type(self.v) is tuple:
            return "({})".format(','.join(["{}".format(symbolic_str(x) if isinstance(x, SymbolicExpr) else x) for x in self.v]))
        elif self.isiter:
            assert(False)
        elif self.v == None:
            return self.v
        else:
            assert(False)
    def toZ3(self):
        if type(self.v) is list or type(self.v) is tuple or type(self.v) is set:
            return [toZ3(x) for x in list(self.v)]
        else:
            assert(False)

class UnaryOp(Var):
    def __init__(self, op, p, compute_actual=True):
        self.op = op
        self.p = p
        self.v = UNARY_OPERATORS[op](getv(p))
        self.is_symbolic = is_symbolic(p)
    def symbolic_str(self):
        if self.op in op.funcops:
            return "({}({}))".format(name_mapping[self.op], symbolic_str(self.p))
        else:
            return "({} {})".format(name_mapping[self.op], symbolic_str(self.p))
    def __str__(self):
        if self.is_symbolic:
            return "v-{} <{} {}>".format(self.getv(), name_mapping[self.op], self.p)
        else:
            return "v-{}".format(self.getv())
    def toZ3(self):
        if self.op == 'INT':
            if isinstance(self.p, Symbol):
                return toZ3(self.p) - ord('0')
            elif isinstance(self.p, ComposedVar):
                d = 1
                r = 0
                for i in reversed(range(0, len(list(self.p.v)))):
                    r = r + self.p.toZ3() * d
                    d = d * 10
                return r
            else:
                assert(False)

class BinOp(Var):
    def __init__(self, lhs, op, rhs, compute_actual=True):
        self.lhs = lhs
        self.rhs = rhs
        self.op = op
        self.v = BINARY_OPERATORS[op](getv(lhs), getv(rhs))
        self.is_symbolic = is_symbolic(self.lhs, self.rhs)
    def symbolic_str(self):
        if self.op == 'SUBSCR':
            return "({}[{}])".format(symbolic_str(self.lhs), symbolic_str(self.rhs))
        else:
            return "({} {} {})".format(symbolic_str(self.lhs), name_mapping[self.op], symbolic_str(self.rhs))
    def __str__(self):
        if self.is_symbolic:
            if self.op == 'SUBSCR':
                return "v-{} <{}[{}]>".format(self.getv(), self.lhs, self.rhs)
            else:
                return "v-{} <{} {} {}>".format(self.getv(), self.lhs, name_mapping[self.op], self.rhs)
        else:
            return "v-{}".format(self.getv())
    def toZ3(self):
        if self.op == 'AND':
            return z3.And(toZ3(self.lhs), toZ3(self.rhs))
        elif self.op == 'ADD':
            return toZ3(self.lhs) + toZ3(self.rhs)
        elif self.op == 'MODULO':
            return toZ3(self.lhs) % toZ3(self.rhs)
        elif self.op == 'DIVIDE':
            return toZ3(self.lhs) / toZ3(self.rhs)
        elif self.op == 'MULTIPLY':
            return toZ3(self.lhs) * toZ3(self.rhs)
        elif self.op == 'SUBTRACT':
            return toZ3(self.lhs) - toZ3(self.rhs)
        elif self.op == 'SUBSCR':
            if isinstance(self.lhs, Var) or isinstance(self.lhs, ComposedVar):
                lst = toZ3(self.lhs)
                assert(type(lst) is list)
                rhs = toZ3(self.rhs)
                r = z3.If(0 == rhs, lst[0], -1)
                if type(rhs) is int and rhs < 0:
                    rhs = len(lst) + rhs
                for i in range(1, len(lst)):
                    r = z3.If(i == rhs, lst[i], r)
                return r
            else:
                assert(False)
            
class CmpOp(Var):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.rhs = rhs
        self.op = op
        self.v = COMPARE_OPERATORS[op](getv(lhs), getv(rhs))
        self.is_symbolic = is_symbolic(self.lhs, self.rhs)
    def __str__(self):
        if self.is_symbolic:
            return "v-{} <{} {} {}>".format(self.getv(), self.lhs, name_mapping[self.op], self.rhs)
        else:
            return "v-{}".format(self.getv())
    def symbolic_str(self):
        return "({} {} {})".format(symbolic_str(self.lhs), name_mapping[self.op], symbolic_str(self.rhs))
    def toZ3(self):
        if self.op == 6:#IN
            v = toZ3(self.rhs)
            assert (type(v) is list)
            cts = [toZ3(self.lhs) == x for x in v]
            return z3.Or(cts)
        elif self.op == 3:#NE
            lhs = toZ3(self.lhs)
            rhs = toZ3(self.rhs)
            if type(lhs) is list or type(rhs) is list:
                if type(lhs) is not list:
                    lhs = [lhs]
                if type(rhs) is not list:
                    rhs = [rhs]
                if len(lhs) != len(rhs):
                    return True
                r = z3.Or([lhs[i] != rhs[i] for i in range(0, len(lhs))])
                return r
            else:
                return lhs != rhs
        elif self.op == 2: #EQ
            lhs = toZ3(self.lhs)
            rhs = toZ3(self.rhs)
            if type(lhs) is list or type(rhs) is list:
                if type(lhs) is not list:
                    lhs = [lhs]
                if type(rhs) is not list:
                    rhs = [rhs]
                if len(lhs) != len(rhs):
                    return False
                r = z3.And([lhs[i] == rhs[i] for i in range(0, len(lhs))])
                return r
            else:
                return lhs == rhs

class TriOp(Var):
    def __init__(self, op, x, y, z):
        self.x = x
        self.y = y
        self.z = z
        self.op = op
        self.v = TRINARY_OPERATORS[op](getv(x), getv(y), getv(z)) 
        self.is_symbolic = is_symbolic(x, y, z)
    def __str__(self):
        if self.is_symbolic:
            if self.op == 'IF-THEN-ELSE':
                return "v-{} <if {} then {} else {}>".format(self.getv(), self.x, self.y, self.z)
        else:
            return "v-{}".format(self.getv())
    def toZ3(self):
        if self.op == 'IF-THEN-ELSE':
            return z3.If(toZ3(self.x), toZ3(self.y), toZ3(self.z))
        else:
            assert(False)


def stringify(v, vm):
    strlst = str(getv(v))
    strl = len(strlst)
    d0 = 1
    #TODO: add constraints here!!
    r = []
    for i in range(0, strl):
        new_sym = Symbol(vrange=NUMBER, actual_v=strlst[i])
        r.append(new_sym)
        newletter = BinOp(BinOp(BinOp(v, 'DIVIDE', Var(d0)), 'MODULO', Var(10)), 'ADD', Var(ord('0')))
        d0 = d0 * 10
        vm.add_assertion(new_sym, newletter) 
    newletter = BinOp(v, 'DIVIDE', Var(d0))
    vm.add_assertion(newletter, Var(0))
    r.reverse()
    return ComposedVar(r)
