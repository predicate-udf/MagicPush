import pprint
import operator
from lambda_symbolic_exec.symbolicv import *
from lambda_symbolic_exec.pyobj import *
from lambda_symbolic_exec.op import *

def handle_inplace_op(op, x, y):
    if op == 'POWER':
        x **= y
    elif op == 'MULTIPLY':
        x *= y
    elif op in ['DIVIDE', 'FLOOR_DIVIDE']:
        x //= y
    elif op == 'TRUE_DIVIDE':
        x /= y
    elif op == 'MODULO':
        x %= y
    elif op == 'ADD':
        x += y
    elif op == 'SUBTRACT':
        x -= y
    elif op == 'LSHIFT':
        x <<= y
    elif op == 'RSHIFT':
        x >>= y
    elif op == 'AND':
        x &= y
    elif op == 'XOR':
        x ^= y
    elif op == 'OR':
        x |= y
    else:           # pragma: no cover
        raise VirtualMachineError("Unknown in-place operator: %r" % op)

def handle_slice_op(op, vm):
    if glbs.SYMBOLIC_MODE:
        start = Var(0)
    else:
        start = 0
    end = None          # we will take this to mean end
    op, count = op[:-2], int(op[-1])
    if count == 1:
        start = vm.pop()
    elif count == 2:
        end = vm.pop()
    elif count == 3:
        end = vm.pop()
        start = vm.pop()
    l = vm.pop()
    if end is None:
        if glbs.SYMBOLIC_MODE:
            end = Var(len(l))
        else:
            end = len(l)
    if op.startswith('STORE_'):
        l[start:end] = vm.pop()
    elif op.startswith('DELETE_'):
        del l[start:end]
    else:
        if glbs.SYMBOLIC_MODE:
            vm.push(l.slice(start, end))
        else:
            vm.push(l[start:end])

def handle_store_subscr(obj, subscr, v):
    if is_symbolic(obj):
        if is_symbolic(subscr):
            assert(False)
        else:
            obj.__setitem__(subscr)
    else:
        if is_symbolic(subscr):
            assert(False)
        else:
            obj[subscr] = v

def handle_delete_subscr(obj, subscr):
    #del obj[subscr]
    pass

def helper_run_iter(arg):
    r = []
    print ("type arg = {}".format(type(arg)))
    if isinstance(arg, Var) or isinstance(arg, ComposedVar):
        arg = arg.make_iter()
    elif isinstance(arg, Generator): 
        pass
    else:
        arg = iter(arg)
    while True:
        try:
            r.append(arg.next())
        except StopIteration:
            break
    return r

def unpack(a):
    r = a
    if isinstance(a, Generator):
        r = helper_run_iter(a)
    return r

def process_builtin_function(vm, func_name, func_body, *posargs, **namedargs):
    #func_body(xxxargs)
    #args = posargs
    args = []
    nargs = {}
    for a in posargs:
        args.append(unpack(a))
    for k,v in namedargs:
        nargs[k] = getv(unpack(v))
    has_symbolic = is_symbolic(args)
    if type(func_name) is tuple:
        has_symbolic = has_symbolic or is_symbolic(func_name[0])
    temp_args = getv(args)
    print ("args = {}".format(args))
    print ("temp_args = {}".format(temp_args))
    print ("func = {}, has_sym = {}".format(func_name, has_symbolic))
    if has_symbolic == False:
        return Var(func_body(*temp_args, **nargs))
    if func_name == 'enumerate':
        arg = args[0]
        r = helper_run_iter(arg)
        print ("r = {}".format(r))
        print ("r = {}".format(",".join(["{}-{}".format(i, v) for i,v in enumerate(r)])))
        ret = ComposedVar([ComposedVar((Var(i), v)) for i,v in enumerate(r)])
        print ("ret = {}".format(ret))
        return ret
    elif func_name == 'all':
        arg = args[0]
        r = helper_run_iter(arg)
        ret = r[0]
        for i in range(1, len(r)):
            ret = BinOp(ret, 'AND', r[i])
        return ret 
    elif func_name == 'len':
        return Var(len(getv(args[0])))
    elif func_name == 'str':
        arg = args[0]
        return stringify(arg, vm)
    elif func_name == 'int':
        arg = args[0]
        if isinstance(arg, ComposedVar):
            d = 1
            r = Var(0)
            for i in reversed(range(0, len(list(arg.v)))):
                r = BinOp(r, 'ADD', BinOp(UnaryOp('INT', arg.v[i]), 'MULTIPLY', Var(d)))
                d = d * 10
            return r
        elif isinstance(arg, Symbol):
            return UnaryOp('INT', arg)
        elif isinstance(arg, Var):
            assert(is_symbolic(arg) == False)
            return Var(int(getv(arg)))
    elif func_name == 'sum':
        arg = args[0]
        r = helper_run_iter(arg)
        ret = r[0]
        for i in range(1, len(r)):
            temp = BinOp(ret, 'ADD', r[i])
            ret = temp
        return ret
    elif type(func_name) is tuple and func_name[1] == 'join':
        arg = args[0]
        r = helper_run_iter(arg)
        jstr = func_name[0]
        #print ("jstr = {} {}, r = {}, is_symbolic = {}".format(jstr, is_symbolic(jstr), r, is_symbolic(ComposedVar(r))))
        if is_symbolic(jstr):
            pass
        else:
            if is_symbolic(ComposedVar(r)):
                if getv(jstr) == '':
                    ret = []
                    for r1 in r:
                        if isinstance(r1, ComposedVar):
                            ret = ret + list(r1.v)
                        else:
                            ret.append(r1)
                    return ComposedVar(ret) 
                else:
                    ret = []
                    for i in range(0, len(r)):
                        if i > 0:
                            ret.append(jstr)
                        if isinstance(r[i], ComposedVar):
                            ret = ret + list(r[i].v)
                        else:
                            ret.append(getv(r[i]))
                    return ComposedVar(ret)
            else:
                return Var(getv(jstr).join([getv(x) for x in r]))
    elif type(func_name) is tuple and func_name[1] == 'index':
        arg = args[0]
        jstr = func_name[0]
        if type(jstr) is not list:
            if isinstance(jstr, ComposedVar):
                jstr = jstr.v
            elif isinstance(jstr, Var):
                jstr = [Var(x) for x in list(jstr.v)]
        ret = TriOp('IF-THEN-ELSE', CmpOp(jstr[0], 2, arg), Var(0), Var(-1))
        for i in range(1, len(jstr)):
            ret = TriOp('IF-THEN-ELSE', CmpOp(jstr[i], 2, arg), Var(i), ret)

        return ret
        #return Var(jstr.getv().index(arg.v))
    elif type(func_name) is tuple and func_name[1] == 'decode':
        return func_name[0]
    elif type(func_name) is tuple and func_name[1] == 'get':
        ddict = func_name[0]
        if is_symbolic(ddict):
            assert(False) #TODO
        else:
            ddict = getv(ddict)
            ret = Var(None)
            for k,v in ddict.items():
                ret = TriOp('IF-THEN-ELSE', CmpOp(k, 2, args[0]), Var(v), ret)
            return ret
    elif func_name == "isinstance":
        v = Var(isinstance(temp_args[0], temp_args[1][1]))
        return v
    else:
        assert(False)

