"""A pure-Python Python bytecode interpreter."""
# Based on:
# pyvm2 by Paul Swartz (z3p), from http://www.twistedmatrix.com/users/z3p/

from __future__ import print_function, division
import dis
import inspect
import linecache
import logging
import operator
import sys

from numpy import var
#from lambda_symbolic_exec.symbolicv import *
#from lambda_symbolic_exec.symbolicop import *
import lambda_symbolic_exec.glbs as glbs
from lambda_symbolic_exec.op import *
#import datagen
import z3

import six
from six.moves import reprlib

PY3, PY2 = True, False #six.PY3, not six.PY3

from lambda_symbolic_exec.pyobj import Frame, Block, Method, Function, Generator

log = logging.getLogger(__name__)

if six.PY3:
    byteint = lambda b: b
else:
    byteint = ord

# Create a repr that won't overflow.
repr_obj = reprlib.Repr()
repr_obj.maxother = 120
repper = repr_obj.repr


class VirtualMachineError(Exception):
    """For raising errors in the operation of the VM."""
    pass

class VMStackElement(object):
    def __init__(self, frame, lasti, assertions):
        self.frame = frame
        self.lasti = lasti
        self.assertions = assertions

class VirtualMachine(object):
    def __init__(self):
        # The call stack of frames.
        self.frames = []
        # The current frame.
        self.frame = None
        self.return_values = []#[(assertion, return_v)] #Var(None)
        self.last_exception = None
        self.ins_counter = 0
        # whenever there's a branch, push one path 
        self.vm_stack = [VMStackElement(self.frame, None, [])] # VMStackElement

    def add_assertion(self, lhs, rhs):
        self.assertions.append((lhs, rhs))
    
    def reset_to_last_stacked_frame(self):
        stack_frame = self.vm_stack[-1]
        self.frame = stack_frame.frame


    def get_return_value(self):
        # TODO: Need to handle NULL
        # each return_value is a Value() object
        #for x in self.return_values:
        #    print("ret: {} || {}".format(x[0], x[1]))
        ret = getv(self.return_values[0][1]) #z3.If(z3.And(*(self.return_values[0][0])), self.return_values[0][1], self.return_values[0][1]) if len(self.return_values[0][0])>0 else self.return_values[0][1]
        for k1,v1 in self.return_values[1:]:
            ret = z3.If(z3.And(*k1), getv(v1), ret) if len(k1) > 0 else getv(v1)
        return Value(ret)

    def top(self):
        """Return the value at the top of the stack, with no changes."""
        return self.frame.stack[-1]

    def pop(self, i=0):
        """Pop a value from the stack.

        Default to the top of the stack, but `i` can be a count from the top
        instead.

        """
        return self.frame.stack.pop(-1-i)

    def push(self, *vals):
        """Push values onto the value stack."""
        self.frame.stack.extend(vals)

    def popn(self, n):
        """Pop a number of values from the value stack.

        A list of `n` values is returned, the deepest value first.

        """
        if n:
            ret = self.frame.stack[-n:]
            self.frame.stack[-n:] = []
            return ret
        else:
            return []

    def peek(self, n=1):
        """Get a value `n` entries down in the stack, without changing the stack."""
        return self.frame.stack[-n]

    def jump(self, jump):
        """Move the bytecode pointer to `jump`, so it will execute next."""
        self.frame.f_lasti = jump

    def push_block(self, type, handler=None, level=None):
        if level is None:
            level = len(self.frame.stack)
        self.frame.block_stack.append(Block(type, handler, level))

    def pop_block(self):
        return self.frame.block_stack.pop()

    def make_frame(self, code, callargs={}, f_globals=None, f_locals=None, var_to_add={}):
        log.info("make_frame: code=%r, callargs=%s" % (code, repper(callargs)))
        if f_globals is not None:
            f_globals = f_globals
            if f_locals is None:
                f_locals = f_globals
        elif self.frame:
            f_globals = self.frame.f_globals
            f_locals = {}
        else:
            f_globals = f_locals = {
                '__builtins__': __builtins__,
                '__name__': '__main__',
                '__doc__': None,
                '__package__': None,
            }
        f_locals.update(callargs)
        f_locals.update(var_to_add)
        f_globals.update(var_to_add)
        frame = Frame(code, f_globals, f_locals, self.frame)
        return frame

    def push_frame(self, frame):
        self.frames.append(frame)
        self.frame = frame
        self.vm_stack[0].frame = self.frame

    def pop_frame(self):
        self.frames.pop()
        if self.frames:
            self.frame = self.frames[-1]
        else:
            self.frame = None

    def print_frames(self):
        """Print the call stack, for debugging."""
        for f in self.frames:
            filename = f.f_code.co_filename
            lineno = f.line_number()
            print('  File "%s", line %d, in %s' % (
                filename, lineno, f.f_code.co_name
            ))
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            if line:
                print('    ' + line.strip())

    def resume_frame(self, frame):
        frame.f_back = self.frame
        val = self.run_frame(frame)
        frame.f_back = None
        return val

    def run_code(self, code, f_globals=None, f_locals=None, var_to_add={}):
        frame = self.make_frame(code, f_globals=f_globals, f_locals=f_locals, var_to_add=var_to_add)
        val = self.run_frame(frame)
        # Check some invariants
        if self.frames:            # pragma: no cover
            raise VirtualMachineError("Frames left over!")
        if self.frame and self.frame.stack:             # pragma: no cover
            raise VirtualMachineError("Data left on stack! %r" % self.frame.stack)

        #print ("code finish!!")
        self.finish()
        return val

    def unwind_block(self, block):
        if block.type == 'except-handler':
            offset = 3
        else:
            offset = 0

        while len(self.frame.stack) > block.level + offset:
            self.pop()

        if block.type == 'except-handler':
            tb, value, exctype = self.popn(3)
            self.last_exception = exctype, value, tb

    def parse_byte_and_args(self):

        f = self.frame
        opoffset = f.f_lasti
        if self.vm_stack[-1].lasti is not None and self.vm_stack[-1].lasti == opoffset: # one branch ends
            while self.vm_stack[-1].lasti == opoffset:
                self.vm_stack.pop(-1)
            #print(len(self.vm_stack))
            #print(self.vm_stack[-1].frame)
            self.reset_to_last_stacked_frame()
            self.frame.f_lasti = opoffset

        byteCode = f.f_code.co_code[opoffset]
        f.f_lasti += 2
        extended_arg = 0
        byte_name = dis.opname[byteCode]
        if byteCode >= dis.HAVE_ARGUMENT:
            # index into the bytecode
            # arg = f.f_code.co_code[f.last_instruction:f.last_instruction+2]  
            # f.last_instruction += 2   # advance the instruction pointer
            # arg_val = arg[0] + (arg[1] * 256)
            arg = f.f_code.co_code[f.f_lasti-1]  # index into the bytecode
            arg_val = arg | extended_arg
            extended_arg = (arg_val << 8) if byteCode == dis.EXTENDED_ARG else 0
            if byteCode in dis.hasconst:   # Look up a constant
                arg = f.f_code.co_consts[arg_val]
            elif byteCode in dis.hasname:  # Look up a name
                arg = f.f_code.co_names[arg_val]
            elif byteCode in dis.haslocal: # Look up a local name
                arg = f.f_code.co_varnames[arg_val]
            elif byteCode in dis.hasjrel:  # Calculate a relative jump
                arg = f.f_lasti + arg_val
            else:
                arg = arg_val
            argument = [arg]
        else:
            argument = []

        #print("{} : arg = {}, stack top = {}".format(byte_name, argument, self.top() if len(self.frame.stack) > 0 else ''))
        return byte_name, argument, opoffset

        """
        f = self.frame
        opoffset = f.f_lasti
        #currentOp = f.opcodes[opoffset]
        #byteCode = currentOp.opcode
        #byteName = currentOp.opname
        byteCode = byteint(f.f_code.co_code[opoffset])
        byteName = dis.opname[byteCode]
        print("byteCode = {}".format(byteCode))
        f.f_lasti += 1
        print("byteName = {}".format(byteName))
        arg = None
        arguments = []
        # if byteCode >= dis.HAVE_ARGUMENT:
        #     arg = f.f_code.co_code[f.f_lasti:f.f_lasti+2]
        #     f.f_lasti += 2
        #     intArg = byteint(arg[0]) + (byteint(arg[1]) << 8)
        if byteCode >= dis.HAVE_ARGUMENT:
# -            arg = f.code_obj.co_code[f.last_instruction:f.last_instruction+2]  # index into the bytecode
# -            f.last_instruction += 2   # advance the instruction pointer
# -            arg_val = arg[0] + (arg[1] << 8)
            arg = f.code_obj.co_code[f.last_instruction-1]  # index into the bytecode
            arg_val = arg | extended_arg
            extended_arg = (arg_val << 8) if byteCode == dis.EXTENDED_ARG else 0

            if byteCode in dis.hasconst:
                arg = f.f_code.co_consts[intArg]
            elif byteCode in dis.hasfree:
                if intArg < len(f.f_code.co_cellvars):
                    arg = f.f_code.co_cellvars[intArg]
                else:
                    var_idx = intArg - len(f.f_code.co_cellvars)
                    arg = f.f_code.co_freevars[var_idx]
            elif byteCode in dis.hasname:
                arg = f.f_code.co_names[intArg]
            elif byteCode in dis.hasjrel:
                arg = f.f_lasti + intArg
            elif byteCode in dis.hasjabs:
                arg = intArg
            elif byteCode in dis.haslocal:
                arg = f.f_code.co_varnames[intArg]
            else:
                arg = intArg
        # if byteCode >= dis.HAVE_ARGUMENT:
        #     if sys.version_info >= (3, 6):
        #         intArg = currentOp.arg
        #     else:
        #         arg = f.f_code.co_code[f.f_lasti:f.f_lasti+2]
        #         f.f_lasti += 2
        #         intArg = byteint(arg[0]) + (byteint(arg[1]) << 8)
        #     if byteCode in dis.hasconst:
        #         arg = f.f_code.co_consts[intArg]
        #     elif byteCode in dis.hasfree:
        #         if intArg < len(f.f_code.co_cellvars):
        #             arg = f.f_code.co_cellvars[intArg]
        #         else:
        #             var_idx = intArg - len(f.f_code.co_cellvars)
        #             arg = f.f_code.co_freevars[var_idx]
        #     elif byteCode in dis.hasname:
        #         arg = f.f_code.co_names[intArg]
        #     elif byteCode in dis.hasjrel:
        #         if sys.version_info >= (3, 6):
        #             arg = f.f_lasti + intArg//2
        #         else:
        #             arg = f.f_lasti + intArg
        #     elif byteCode in dis.hasjabs:
        #         if sys.version_info >= (3, 6):
        #             arg = intArg//2
        #         else:
        #             arg = intArg
        #     elif byteCode in dis.haslocal:
        #         arg = f.f_code.co_varnames[intArg]
        #     else:
        #         arg = intArg
            arguments = [arg]
        self.ins_counter += 1

        sttop = self.peek() if self.frame and len(self.frame.stack) > 0 else '-'
        if hasattr(sttop, '__str__'):
            pass
        else:
            sttop = type(sttop)
         
        print("\tstack top = {}".format(sttop))
        
        print("{}, {}, {}".format(self.ins_counter, byteName, arguments))
        return byteName, arguments, opoffset
        """

    def log(self, byteName, arguments, opoffset):
        """ Log arguments, block stack, and data stack for each opcode."""
        op = "%d: %s" % (opoffset, byteName)
        if arguments:
            op += " %r" % (arguments[0],)
        indent = "    "*(len(self.frames)-1)
        stack_rep = repper(self.frame.stack)
        block_stack_rep = repper(self.frame.block_stack)

        log.info("  %sdata: %s" % (indent, stack_rep))
        log.info("  %sblks: %s" % (indent, block_stack_rep))
        log.info("%s%s" % (indent, op))

    def dispatch(self, byteName, arguments):
        """ Dispatch by bytename to the corresponding methods.
        Exceptions are caught and set on the virtual machine."""
        why = None
        #try:
        if True:
            if byteName.startswith('UNARY_'):
                self.unaryOperator(byteName[6:])
            elif byteName.startswith('BINARY_'):
                self.binaryOperator(byteName[7:])
            elif byteName.startswith('INPLACE_'):
                self.inplaceOperator(byteName[8:])
            elif 'SLICE+' in byteName:
                self.sliceOperator(byteName)
            else:
                # dispatch
                bytecode_fn = getattr(self, 'byte_%s' % byteName, None)
                if not bytecode_fn:            # pragma: no cover
                    raise VirtualMachineError(
                        "unknown bytecode type: %s" % byteName
                    )
                why = bytecode_fn(*arguments)

        try:
            pass
        except:
            # deal with exceptions encountered while executing the op.
            self.last_exception = sys.exc_info()[:2] + (None,)
            log.exception("Caught exception during execution")
            why = 'exception'

        return why

    def manage_block_stack(self, why):
        """ Manage a frame's block stack.
        Manipulate the block stack and data stack for looping,
        exception handling, or returning."""
        assert why != 'yield'

        block = self.frame.block_stack[-1]
        if block.type == 'loop' and why == 'continue':
            self.jump(self.get_return_value())
            why = None
            return why

        self.pop_block()
        self.unwind_block(block)

        if block.type == 'loop' and why == 'break':
            why = None
            self.jump(block.handler)
            return why

        if PY2:
            if (
                block.type == 'finally' or
                (block.type == 'setup-except' and why == 'exception') or
                block.type == 'with'
            ):
                if why == 'exception':
                    exctype, value, tb = self.last_exception
                    self.push(tb, value, exctype)
                else:
                    if why in ('return', 'continue'):
                        self.push(self.get_return_value())
                    self.push(why)

                why = None
                self.jump(block.handler)
                return why

        elif PY3:
            if (
                why == 'exception' and
                block.type in ['setup-except', 'finally']
            ):
                self.push_block('except-handler')
                exctype, value, tb = self.last_exception
                self.push(tb, value, exctype)
                # PyErr_Normalize_Exception goes here
                self.push(tb, value, exctype)
                why = None
                self.jump(block.handler)
                return why

            elif block.type == 'finally':
                if why in ('return', 'continue'):
                    self.push(self.get_return_value())
                self.push(why)

                why = None
                self.jump(block.handler)
                return why

        return why


    def run_frame(self, frame):
        """Run a frame until it returns (somehow).

        Exceptions are raised, the return value is returned.

        """
        self.push_frame(frame)
        while True:
            byteName, arguments, opoffset = self.parse_byte_and_args()
            if log.isEnabledFor(logging.INFO):
                self.log(byteName, arguments, opoffset)

            # When unwinding the block stack, we need to keep track of why we
            # are doing it.
            why = self.dispatch(byteName, arguments)
            if why == 'exception':
                # TODO: ceval calls PyTraceBack_Here, not sure what that does.
                pass

            if why == 'reraise':
                why = 'exception'

            if why != 'yield':
                while why and frame.block_stack:
                    # Deal with any block management we need to do.
                    why = self.manage_block_stack(why)

            if why:
                break
            if self.frame.f_lasti >= len(self.frame.f_code.co_code):
                break
        # TODO: handle generator exception state

        self.pop_frame()

        if why == 'exception':
            six.reraise(*self.last_exception)

        return self.get_return_value()

    ## Stack manipulation

    def byte_LOAD_CONST(self, const):
        if glbs.SYMBOLIC_MODE:
            self.push(Var(const))
        else:
            self.push(const)
    
    def byte_LOAD_METHOD(self, namei):
        TOS = self.pop()
        if hasattr(TOS, namei):
            # FIXME: check that gettr(TO, name) is a method
            self.push(getattr(TOS, namei))
            self.push("LOAD_METHOD lookup success")
        else:
            self.push("fill in attribute method lookup")
            self.push(None)

    def byte_CALL_METHOD(self, count):
        posargs = self.popn(count)
        is_success = self.pop()
        if is_success:
            func = self.pop()
            self.call_function_with_args_resolved(func, posargs, {})
        else:
            # FIXME: do something else
            raise self.vm.PyVMError("CALL_METHOD not implemented yet")

    def byte_POP_TOP(self):
        self.pop()

    def byte_DUP_TOP(self):
        self.push(self.top())

    def byte_DUP_TOPX(self, count):
        items = self.popn(count)
        for i in [1, 2]:
            self.push(*items)

    def byte_DUP_TOP_TWO(self):
        # Py3 only
        a, b = self.popn(2)
        self.push(a, b, a, b)

    def byte_ROT_TWO(self):
        a, b = self.popn(2)
        self.push(b, a)

    def byte_ROT_THREE(self):
        a, b, c = self.popn(3)
        self.push(c, a, b)

    def byte_ROT_FOUR(self):
        a, b, c, d = self.popn(4)
        self.push(d, a, b, c)

    ## Names

    def byte_LOAD_NAME(self, name):
        frame = self.frame
        if name in frame.f_locals:
            val = frame.f_locals[name]
        elif name in frame.f_globals:
            val = frame.f_globals[name]
        elif name in frame.f_builtins:
            if glbs.SYMBOLIC_MODE: 
                val = (name, frame.f_builtins[name])
            else:
                val = frame.f_builtins[name]
        else:
            raise NameError("name '%s' is not defined" % name)
        self.push(val)

    def byte_STORE_NAME(self, name):
        v = self.pop()
        if to_be_symbolic(name):
            self.frame.f_locals[name] = make_symbolic(v)
        else:
            self.frame.f_locals[name] = v

    def byte_DELETE_NAME(self, name):
        del self.frame.f_locals[name]

    def byte_LOAD_FAST(self, name):
        if name in self.frame.f_locals:
            val = self.frame.f_locals[name]
        else:
            raise UnboundLocalError(
                "local variable '%s' referenced before assignment" % name
            )
        self.push(val)

    def byte_STORE_FAST(self, name):
        self.frame.f_locals[name] = self.pop()

    def byte_DELETE_FAST(self, name):
        del self.frame.f_locals[name]

    def byte_LOAD_GLOBAL(self, name):
        f = self.frame
        if name in f.f_globals:
            val = f.f_globals[name]
        elif name in f.f_builtins:
            if glbs.SYMBOLIC_MODE:
                val = (name, f.f_builtins[name])
            else:
                val = f.f_builtins[name]
        else:
            raise NameError("global name '%s' is not defined" % name)

        if str(type(val)) == "<type 'function'>":
            val = Function(val.func_name, val.func_code, val.func_globals, val.func_defaults if val.func_defaults else [], None, self)

        self.push(val)

    def byte_STORE_GLOBAL(self, name):
        f = self.frame
        f.f_globals[name] = self.pop()

    def byte_LOAD_DEREF(self, name):
        self.push(self.frame.cells[name].get())

    def byte_STORE_DEREF(self, name):
        self.frame.cells[name].set(self.pop())

    def byte_LOAD_LOCALS(self):
        self.push(self.frame.f_locals)

    ## Operators

    def unaryOperator(self, op):
        x = self.pop()
        if glbs.SYMBOLIC_MODE:
            self.push(UnaryOp(op, x))
        else:
            self.push(UNARY_OPERATORS[op](x))

    def binaryOperator(self, op):
        x, y = self.popn(2)
        if glbs.SYMBOLIC_MODE:
            self.push(BinOp(x, op, y))
        else:
            self.push(BINARY_OPERATORS[op](x, y))

    def inplaceOperator(self, op):
        x, y = self.popn(2)
        if glbs.SYMBOLIC_MODE:
            assert(False) #TODO
        else:
            handle_inplace_op(op, x, y)
        self.push(x)

    def sliceOperator(self, op):
        handle_slice_op(op, self) 

    def byte_COMPARE_OP(self, opnum):
        x, y = self.popn(2)
        if glbs.SYMBOLIC_MODE:
            self.push(CmpOp(x, opnum, y))
        else:
            self.push(COMPARE_OPERATORS[opnum](x, y))


    ## Attributes and indexing

    def byte_LOAD_ATTR(self, attr):
        obj = self.pop()
        print ("obj = {}".format(obj))
        if glbs.SYMBOLIC_MODE:
            val = getattr(getv(obj), attr)
            if val.__class__.__name__ == 'builtin_function_or_method':
                self.push(((obj, attr), val))
            else:
                self.push(val)
        else:
            val = getattr(obj, attr)
            self.push(val)

    def byte_STORE_ATTR(self, name):
        val, obj = self.popn(2)
        setattr(getv(obj), name, val)

    def byte_DELETE_ATTR(self, name):
        obj = self.pop()
        delattr(getv(obj), name)

    def byte_STORE_SUBSCR(self):
        val, obj, subscr = self.popn(3)
        handle_store_subscr(obj, subscr, val)

    def byte_DELETE_SUBSCR(self):
        obj, subscr = self.popn(2)
        handle_delete_subscr(obj, subscr)

    ## Building

    def byte_BUILD_TUPLE(self, count):
        elts = self.popn(count)
        if glbs.SYMBOLIC_MODE:
            self.push(ComposedVar(tuple(elts)))
        else:
            self.push(tuple(elts))

    def byte_BUILD_LIST(self, count):
        elts = self.popn(count)
        if glbs.SYMBOLIC_MODE:
            self.push(ComposedVar(elts))
        else:
            self.push(list(elts))

    def byte_BUILD_SET(self, count):
        # TODO: Not documented in Py2 docs.
        elts = self.popn(count)
        if glbs.SYMBOLIC_MODE:
            self.push(ComposedVar(set(elts)))
        else:
            self.push(set(elts))

    def byte_BUILD_MAP(self, size):
        # size is ignored.
        values = self.popn(2*size)
        ret = {}
        for i in range(0,len(values),2):
            ret[values[i]]=values[i+1]
        self.push(ret)

    def byte_STORE_MAP(self):
        the_map, val, key = self.popn(3)
        the_map[key] = val
        self.push(the_map)

    def byte_UNPACK_SEQUENCE(self, count):
        seq = self.pop()
        if glbs.SYMBOLIC_MODE:
            seq = seq.v
        for x in reversed(seq):
            self.push(x)

    def byte_BUILD_SLICE(self, count):
        if count == 2:
            x, y = self.popn(2)
            if glbs.SYMBOLIC_MODE:
                self.push(ComposedVar(slice(x, y)))
            else:
                self.push(slice(x, y))
        elif count == 3:
            x, y, z = self.popn(3)
            if glbs.SYMBOLIC_MODE:
                self.push(ComposedVar(slice(x, y, z)))
            else:
                self.push(slice(x, y, z))
        else:           # pragma: no cover
            raise VirtualMachineError("Strange BUILD_SLICE count: %r" % count)

    def byte_LIST_APPEND(self, count):
        val = self.pop()
        the_list = self.peek(count)
        the_list.append(val)

    def byte_SET_ADD(self, count):
        val = self.pop()
        the_set = self.peek(count)
        the_set.add(val)

    def byte_MAP_ADD(self, count):
        val, key = self.popn(2)
        the_map = self.peek(count)
        the_map[key] = val

    ## Printing

    if 0:   # Only used in the interactive interpreter, not in modules.
        def byte_PRINT_EXPR(self):
            print(self.pop())

    def byte_PRINT_ITEM(self):
        item = self.pop()
        self.print_item(item)

    def byte_PRINT_ITEM_TO(self):
        to = self.pop()
        item = self.pop()
        self.print_item(item, to)

    def byte_PRINT_NEWLINE(self):
        self.print_newline()

    def byte_PRINT_NEWLINE_TO(self):
        to = self.pop()
        self.print_newline(to)

    def print_item(self, item, to=None):
        if to is None:
            to = sys.stdout
        if to.softspace:
            print(" ", end="", file=to)
            to.softspace = 0
        print(item, end="", file=to)
        if isinstance(item, str):
            if (not item) or (not item[-1].isspace()) or (item[-1] == " "):
                to.softspace = 1
        else:
            to.softspace = 1

    def print_newline(self, to=None):
        if to is None:
            to = sys.stdout
        print("", file=to)
        to.softspace = 0

    ## Jumps

    def byte_JUMP_FORWARD(self, jump):
        self.jump(jump)

    def byte_JUMP_ABSOLUTE(self, jump):
        self.jump(jump)

    if 0:   # Not in py2.7
        def byte_JUMP_IF_TRUE(self, jump):
            val = self.top()
            if val:
                self.jump(jump)

        def byte_JUMP_IF_FALSE(self, jump):
            val = self.top()
            if not val:
                self.jump(jump)

    def byte_POP_JUMP_IF_TRUE(self, jump):
        val = self.pop()
        if is_symbolic(val):
            cur_assertions = [x for x in self.vm_stack[-1].assertions] if len(self.vm_stack) > 0 else []
            self.vm_stack[-1].assertions.append(getv(val)==True)
            # push the left branch (False, no jump)
            self.vm_stack.append(VMStackElement(self.frame.fork(), jump, cur_assertions+[getv(val)==False]))
            self.frame = self.vm_stack[-1].frame
        else:
            if getv(val):
                self.jump(jump)
            else:
                pass
        # if getv(val):
        #     if is_symbolic(val):
        #         self.add_assertion(val, Var(True))
        #     self.jump(jump)
        # else:
        #     if is_symbolic(val):
        #         self.add_assertion(val, Var(False))

    def byte_POP_JUMP_IF_FALSE(self, jump):
        val = self.pop()
        if is_symbolic(val):
            cur_assertions = [x for x in self.vm_stack[-1].assertions] if len(self.vm_stack) > 0 else []
            self.vm_stack[-1].assertions.append(getv(val)==False)
            # push the left branch (False, no jump)
            self.vm_stack.append(VMStackElement(self.frame.fork(), jump, cur_assertions+[getv(val)==True]))
            self.frame = self.vm_stack[-1].frame
        else:
            if not getv(val):
                self.jump(jump)
        # if not getv(val):
        #     if is_symbolic(val):
        #         self.add_assertion(val, Var(False))
        #     self.jump(jump)
        # else:
        #     if is_symbolic(val):
        #         self.add_assertion(val, Var(True))

    def byte_JUMP_IF_TRUE_OR_POP(self, jump):
        val = self.top()
        if getv(val):
            if is_symbolic(val):
                self.add_assertion(val, Var(True))
            self.jump(jump)
        else:
            if is_symbolic(val):
                self.add_assertion(val, Var(False))
            self.pop()

    def byte_JUMP_IF_FALSE_OR_POP(self, jump):
        val = self.top()
        if not getv(val):
            if is_symbolic(val):
                self.add_assertion(val, Var(False))
            self.jump(jump)
        else:
            if is_symbolic(val):
                self.add_assertion(val, Var(True))
            self.pop()

    ## Blocks

    def byte_SETUP_LOOP(self, dest):
        self.push_block('loop', dest)

    def byte_GET_ITER(self):
        if glbs.SYMBOLIC_MODE:
            self.push(self.pop().make_iter())
        else:
            self.push(iter(self.pop()))

    def byte_FOR_ITER(self, jump):
        iterobj = self.top()
        try:
            if glbs.SYMBOLIC_MODE: 
                v = iterobj.get_iter()
            else:
                v = iterobj.next()
            self.push(v)
        except StopIteration:
            self.pop()
            self.jump(jump)

    def byte_BREAK_LOOP(self):
        return 'break'

    def byte_CONTINUE_LOOP(self, dest):
        # This is a trick with the return value.
        # While unrolling blocks, continue and return both have to preserve
        # state as the finally blocks are executed.  For continue, it's
        # where to jump to, for return, it's the value to return.  It gets
        # pushed on the stack for both, so continue puts the jump destination
        # into return_value.
        assert(False)
        self.return_value = dest
        return 'continue'

    def byte_SETUP_EXCEPT(self, dest):
        self.push_block('setup-except', dest)

    def byte_SETUP_FINALLY(self, dest):
        self.push_block('finally', dest)

    #TODO
    def byte_END_FINALLY(self):
        v = self.pop()
        if isinstance(v, str):
            why = v
            if why in ('return', 'continue'):
                #self.return_value = self.pop()
                self.return_values.append((self.vm_stack[-1].assertions, self.pop()))
            if why == 'silenced':       # PY3
                block = self.pop_block()
                assert block.type == 'except-handler'
                self.unwind_block(block)
                why = None
        elif v is None:
            why = None
        elif issubclass(v, BaseException):
            exctype = v
            val = self.pop()
            tb = self.pop()
            self.last_exception = (exctype, val, tb)
            why = 'reraise'
        else:       # pragma: no cover
            raise VirtualMachineError("Confused END_FINALLY")
        return why

    def byte_POP_BLOCK(self):
        self.pop_block()

    #TODO:
    if PY2:
        def byte_RAISE_VARARGS(self, argc):
            # NOTE: the dis docs are completely wrong about the order of the
            # operands on the stack!
            exctype = val = tb = None
            if argc == 0:
                exctype, val, tb = self.last_exception
            elif argc == 1:
                exctype = self.pop()
            elif argc == 2:
                val = self.pop()
                exctype = self.pop()
            elif argc == 3:
                tb = self.pop()
                val = self.pop()
                exctype = self.pop()

            # There are a number of forms of "raise", normalize them somewhat.
            if isinstance(exctype, BaseException):
                val = exctype
                exctype = type(val)

            self.last_exception = (exctype, val, tb)

            if tb:
                return 'reraise'
            else:
                return 'exception'

    elif PY3:
        def byte_RAISE_VARARGS(self, argc):
            cause = exc = None
            if argc == 2:
                cause = self.pop()
                exc = self.pop()
            elif argc == 1:
                exc = self.pop()
            return self.do_raise(exc, cause)

        def do_raise(self, exc, cause):
            if exc is None:         # reraise
                exc_type, val, tb = self.last_exception
                if exc_type is None:
                    return 'exception'      # error
                else:
                    return 'reraise'

            elif type(exc) == type:
                # As in `raise ValueError`
                exc_type = exc
                val = exc()             # Make an instance.
            elif isinstance(exc, BaseException):
                # As in `raise ValueError('foo')`
                exc_type = type(exc)
                val = exc
            else:
                return 'exception'      # error

            # If you reach this point, you're guaranteed that
            # val is a valid exception instance and exc_type is its class.
            # Now do a similar thing for the cause, if present.
            if cause:
                if type(cause) == type:
                    cause = cause()
                elif not isinstance(cause, BaseException):
                    return 'exception'  # error

                val.__cause__ = cause

            self.last_exception = exc_type, val, val.__traceback__
            return 'exception'

    def byte_POP_EXCEPT(self):
        block = self.pop_block()
        if block.type != 'except-handler':
            raise Exception("popped block is not an except handler")
        self.unwind_block(block)

    def byte_SETUP_WITH(self, dest):
        ctxmgr = self.pop()
        self.push(ctxmgr.__exit__)
        ctxmgr_obj = ctxmgr.__enter__()
        if PY2:
            self.push_block('with', dest)
        elif PY3:
            self.push_block('finally', dest)
        self.push(ctxmgr_obj)

    def byte_WITH_CLEANUP(self):
        # The code here does some weird stack manipulation: the exit function
        # is buried in the stack, and where depends on what's on top of it.
        # Pull out the exit function, and leave the rest in place.
        v = w = None
        u = self.top()
        if u is None:
            exit_func = self.pop(1)
        elif isinstance(u, str):
            if u in ('return', 'continue'):
                exit_func = self.pop(2)
            else:
                exit_func = self.pop(1)
            u = None
        elif issubclass(u, BaseException):
            if PY2:
                w, v, u = self.popn(3)
                exit_func = self.pop()
                self.push(w, v, u)
            elif PY3:
                w, v, u = self.popn(3)
                tp, exc, tb = self.popn(3)
                exit_func = self.pop()
                self.push(tp, exc, tb)
                self.push(None)
                self.push(w, v, u)
                block = self.pop_block()
                assert block.type == 'except-handler'
                self.push_block(block.type, block.handler, block.level-1)
        else:       # pragma: no cover
            raise VirtualMachineError("Confused WITH_CLEANUP")
        exit_ret = exit_func(u, v, w)
        err = (u is not None) and bool(exit_ret)
        if err:
            # An error occurred, and was suppressed
            if PY2:
                self.popn(3)
                self.push(None)
            elif PY3:
                self.push('silenced')

    ## Functions

    def byte_MAKE_FUNCTION(self, argc):
        if PY3:
            name = self.pop()
        else:
            # Pushes a new function object on the stack. TOS is the code
            # associated with the function. The function object is defined to
            # have argc default parameters, which are found below TOS.
            name = None
        code = self.pop()
        globs = self.frame.f_globals
        if PY3 and sys.version_info.minor >= 6:
            closure = self.pop() if (argc & 0x8) else None
            ann = self.pop() if (argc & 0x4) else None
            kwdefaults = self.pop() if (argc & 0x2) else None
            defaults = self.pop() if (argc & 0x1) else None
            fn = Function(name, code, globs, defaults, kwdefaults, closure, self)
        else:
            defaults = self.popn(argc)
            fn = Function(name, code, globs, defaults, None, None, self)
        self.push(fn)

    def byte_LOAD_CLOSURE(self, name):
        self.push(self.frame.cells[name])

    def byte_MAKE_CLOSURE(self, argc):
        if PY3:
            # TODO: the py3 docs don't mention this change.
            name = self.pop()
        else:
            name = None
        closure, code = self.popn(2)
        defaults = self.popn(argc)
        globs = self.frame.f_globals
        if glbs.SYMBOLIC_MODE:
            closure = getv(closure)
            code = getv(code)
        fn = Function(name, code, globs, defaults, closure, self)
        self.push(fn)

    def byte_CALL_FUNCTION(self, arg):
        return self.call_function(arg, [], {})

    def byte_CALL_FUNCTION_VAR(self, arg):
        args = self.pop()
        return self.call_function(arg, args, {})

    def byte_CALL_FUNCTION_KW(self, arg):
        kwargs = self.pop()
        return self.call_function(arg, [], kwargs)

    def byte_CALL_FUNCTION_VAR_KW(self, arg):
        args, kwargs = self.popn(2)
        return self.call_function(arg, args, kwargs)


    def call_function_with_args_resolved(self, func, pos_args, named_args):
        frame = self.frame
        pos_args = [getv(v) for v in pos_args]
        if hasattr(func, "im_func"):
            # Methods get self as an implicit first parameter.
            if func.im_self is not None:
                pos_args.insert(0, func.im_self)
            # The first parameter must be the correct type.
            if not isinstance(pos_args[0], func.im_class):
                raise TypeError(
                    "unbound method %s() must be called with %s instance "
                    "as first argument (got %s instance instead)"
                    % (
                        func.im_func.func_name,
                        func.im_class.__name__,
                        type(pos_args[0]).__name__,
                    )
                )
            func = func.im_func
        retval = func(*pos_args, **named_args)
        self.push(retval)

    def call_function(self, arg, args, kwargs):
        lenKw, lenPos = divmod(arg, 256)
        namedargs = {}
        for i in range(lenKw):
            key, val = self.popn(2)
            namedargs[getv(key)] = val
        namedargs.update(kwargs)
        posargs = self.popn(lenPos)
        posargs.extend(args)

        func = self.pop()
        return self.call_function_with_args_resolved(func, posargs, namedargs)

        # if hasattr(func, 'im_func'):
        #     # Methods get self as an implicit first parameter.
        #     if func.im_self:
        #         posargs.insert(0, func.im_self)
        #     # The first parameter must be the correct type.
        #     if not isinstance(posargs[0], func.im_class):
        #         raise TypeError(
        #             'unbound method %s() must be called with %s instance '
        #             'as first argument (got %s instance instead)' % (
        #                 func.im_func.func_name,
        #                 func.im_class.__name__,
        #                 type(posargs[0]).__name__,
        #             )
        #         )
        #     func = func.im_func
        # #if any([is_symbolic(v) for v in posargs]) or any([is_symbolic(v) for k,v in namedargs.items()]):
        # #    assert(False)  
        # #else:
        # #print(func.__name__)
        # #print(posargs)
        # if True:
        #     retval = func(*posargs, **namedargs)
        #     #print("retval = {}".format(retval))
        #     self.push(retval)

    def byte_RETURN_VALUE(self):
        #self.return_value = self.pop()
        v = self.pop()
        #print("assertions = {}".format(','.join([str(xx) for xx in self.vm_stack[-1].assertions])))
        #print("return v = {}".format(v))
        self.return_values.append((self.vm_stack[-1].assertions, v))
        self.assertions = []
        if self.frame.generator:
            self.frame.generator.finished = True
        #return "return"

    def byte_YIELD_VALUE(self):
        #self.return_value = self.pop()
        self.return_values.append((self.vm_stack[-1].assertions, self.pop()))
        #return "yield"

    #TODO
    def byte_YIELD_FROM(self):
        u = self.pop()
        x = self.top()

        try:
            if not isinstance(x, Generator) or u is None:
                # Call next on iterators.
                retval = next(x)
            else:
                retval = x.send(u)
            #self.return_value = retval
            self.return_values.append((self.vm_stack.assertions, self.pop()))
        except StopIteration as e:
            self.pop()
            self.push(e.value)
        else:
            # YIELD_FROM decrements f_lasti, so that it will be called
            # repeatedly until a StopIteration is raised.
            self.jump(self.frame.f_lasti - 1)
            # Returning "yield" prevents the block stack cleanup code
            # from executing, suspending the frame in its current state.
            #return "yield"

    ## Importing

    def byte_IMPORT_NAME(self, name):
        level, fromlist = self.popn(2)
        frame = self.frame
        print ("name = {}, f_globals = {}, f_locals = {}, from_list = {}, level = {}".format(name, frame.f_globals, frame.f_locals, fromlist, level))
        self.push(
            __import__(name, frame.f_globals, frame.f_locals, fromlist, level)
        )

    def byte_IMPORT_STAR(self):
        # TODO: this doesn't use __all__ properly.
        mod = self.pop()
        for attr in dir(mod):
            if attr[0] != '_':
                self.frame.f_locals[attr] = getattr(mod, attr)

    def byte_IMPORT_FROM(self, name):
        mod = self.top()
        self.push(ComposedVar(getattr(mod, name)))

    ## And the rest...

    def byte_EXEC_STMT(self):
        stmt, globs, locs = self.popn(3)
        six.exec_(stmt, globs, locs)

    if PY2:
        def byte_BUILD_CLASS(self):
            name, bases, methods = self.popn(3)
            self.push(type(name, bases, methods))


    elif PY3:
        def byte_LOAD_BUILD_CLASS(self):
            # New in py3
            self.push(__build_class__)

        def byte_STORE_LOCALS(self):
            self.frame.f_locals = self.pop()

    if 0:   # Not in py2.7
        def byte_SET_LINENO(self, lineno):
            self.frame.f_lineno = lineno

    def finish(self):
        pass
        #print("FINISH!! return = {}".format(self.return_value))
        #print("FINISH!")
        #for assertions,return_v in self.return_values:
        #    print("return {} when {}".format(return_v, ' & '.join([str(assertion) for assertion in assertions])))
        
