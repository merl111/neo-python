import hashlib
import datetime

import neo.VM.OpCode as opc
from neo.VM.RandomAccessStack import RandomAccessStack, InvalidStackSize
from neo.VM.ExecutionContext import ExecutionContext
from neo.VM import VMState
from neo.VM.InteropService import Array, Struct, CollectionMixin, Map, Boolean
from neo.Core.UInt160 import UInt160
from neo.Settings import settings
from neo.VM.VMFault import VMFault
from logging import DEBUG as LOGGING_LEVEL_DEBUG
from neo.logging import log_manager
from typing import TYPE_CHECKING
from neo.VM.Script import Script
import cProfile

if TYPE_CHECKING:
    from neo.VM.InteropService import BigInteger

logger = log_manager.getLogger('vm')

int_MaxValue = 2147483647


class VMException(Exception):
    pass


def execPUSH0(self, context, opcode):
    context._EvaluationStack.PushT(bytearray(0))

    # return self.ExecuteImmediate()


def execPUSHBYTES(self, context, opcode):

    if not self.CheckMaxItemSize(len(context.CurrentInstruction.Operand)):
        return False
    context._EvaluationStack.PushT(context.CurrentInstruction.Operand)


def execPUSHOPS(self, context, opcode):
    topush = int.from_bytes(opcode, 'little') - int.from_bytes(opc.PUSH1, 'little') + 1
    context._EvaluationStack.PushT(topush)

    # return self.ExecuteImmediate()


def execNOP(self, context, opcode):
    # return self.ExecuteImmediate()
    pass


def execJMPALL(self, context, opcode):
    offset = context.InstructionPointer + context.CurrentInstruction.TokenI16

    if offset < 0 or offset > context.Script.Length:
        return self.VM_FAULT_and_report(VMFault.INVALID_JUMP)

    fValue = True
    if opcode > opc.JMP:
        fValue = context._EvaluationStack.Pop().GetBoolean()
        if opcode == opc.JMPIFNOT:
            fValue = not fValue
    if fValue:
        context.InstructionPointer = offset
    else:
        context.InstructionPointer += 3
    return True


def execCALL(self, context, opcode):
    if not self.CheckMaxInvocationStack():
        return self.VM_FAULT_and_report(VMFault.CALL_EXCEED_MAX_INVOCATIONSTACK_SIZE)

    context_call = self._LoadScriptInternal(context.Script)
    context_call.InstructionPointer = context.InstructionPointer + context.CurrentInstruction.TokenI16

    if context_call.InstructionPointer < 0 or context_call.InstructionPointer > context_call.Script.Length:
        return False

    context.EvaluationStack.CopyTo(context_call.EvaluationStack)
    context.EvaluationStack.Clear()

    # return self.ExecuteImmediate()


def execRET(self, context, opcode):
    context_pop: ExecutionContext = self._InvocationStack.Pop()
    rvcount = context_pop._RVCount

    if rvcount == -1:
        rvcount = context_pop.EvaluationStack.Count

    if rvcount > 0:
        if context_pop.EvaluationStack.Count < rvcount:
            return self.VM_FAULT_and_report(VMFault.UNKNOWN1)

        if self._InvocationStack.Count == 0:
            stack_eval = self._ResultStack
        else:
            stack_eval = self.CurrentContext.EvaluationStack
        context_pop.EvaluationStack.CopyTo(stack_eval, rvcount)

    if context_pop._RVCount == -1 and self._InvocationStack.Count > 0:
        context_pop.AltStack.CopyTo(self.CurrentContext.AltStack)

    if self._InvocationStack.Count == 0:
        self._VMState = VMState.HALT
    return True


def execAPPTAILCALL(self, context, opcode):
    if self._Table is None:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN2)

    if opcode == opc.APPCALL and not self.CheckMaxInvocationStack():
        return self.VM_FAULT_and_report(VMFault.APPCALL_EXCEED_MAX_INVOCATIONSTACK_SIZE)

    script_hash = context.CurrentInstruction.Operand

    is_normal_call = False
    for b in script_hash:
        if b > 0:
            is_normal_call = True

    if not is_normal_call:
        script_hash = context._EvaluationStack.Pop().GetByteArray()

    context_new = self._LoadScriptByHash(script_hash)
    if context_new is None:
        return self.VM_FAULT_and_report(VMFault.INVALID_CONTRACT, script_hash)

    context._EvaluationStack.CopyTo(context_new.EvaluationStack)

    if opcode == opc.TAILCALL:
        self._InvocationStack.Remove(1)
    else:
        context._EvaluationStack.Clear()

    # return self.ExecuteImmediate()


def execSYSCALL(self, context, opcode):
    if len(context.CurrentInstruction.Operand) > 252:
        return False

    call = context.CurrentInstruction.Operand.decode('ascii')
    self.write_log(call)
    ret = self._Service.Invoke(call, self)
    if not ret:
        return self.VM_FAULT_and_report(VMFault.SYSCALL_ERROR,
                                        context.CurrentInstruction.Operand)

    # return self.ExecuteImmediate()


def execDUPFROMALTSTACK(self, context, opcode):
    context._EvaluationStack.PushT(context._AltStack.Peek())

    # return self.ExecuteImmediate()


def execTOALTSTACK(self, context, opcode):
    context._AltStack.PushT(context._EvaluationStack.Pop())
    # return self.ExecuteImmediate()


def execFROMALTSTACK(self, context, opcode):
    context._EvaluationStack.PushT(context._AltStack.Pop())
    # return self.ExecuteImmediate()


def execXDROP(self, context, opcode):
    n = context._EvaluationStack.Pop().GetBigInteger()
    if n < 0:
        self._VMState |= VMState.FAULT
        return
    context._EvaluationStack.Remove(n)

    # return self.ExecuteImmediate()


def execXSWAP(self, context, opcode):
    n = context._EvaluationStack.Pop().GetBigInteger()

    if n < 0:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN3)

    # if n == 0 break, same as do x if n > 0
    if n > 0:
        item = context._EvaluationStack.Peek(n)
        context._EvaluationStack.Set(n, context._EvaluationStack.Peek())
        context._EvaluationStack.Set(0, item)

    # return self.ExecuteImmediate()


def execXTUCK(self, context, opcode):
    n = context._EvaluationStack.Pop().GetBigInteger()

    if n <= 0:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN4)

    context._EvaluationStack.Insert(n, context._EvaluationStack.Peek())

    # return self.ExecuteImmediate()


def execDEPTH(self, context, opcode):
    context._EvaluationStack.PushT(context._EvaluationStack.Count)

    # return self.ExecuteImmediate()


def execDROP(self, context, opcode):
    context._EvaluationStack.Pop()

    # return self.ExecuteImmediate()


def execDUP(self, context, opcode):
    context._EvaluationStack.PushT(context._EvaluationStack.Peek())

    # return self.ExecuteImmediate()


def execNIP(self, context, opcode):
    context._EvaluationStack.Remove(1)

    # return self.ExecuteImmediate()


def execOVER(self, context, opcode):
    context._EvaluationStack.PushT(context._EvaluationStack.Peek(1))

    # return self.ExecuteImmediate()


def execPICK(self, context, opcode):
    n = context._EvaluationStack.Pop().GetBigInteger()
    if n < 0:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN5)

    context._EvaluationStack.PushT(context._EvaluationStack.Peek(n))

    # return self.ExecuteImmediate()


def execROLL(self, context, opcode):
    n = context._EvaluationStack.Pop().GetBigInteger()
    if n < 0:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN6)

    if n > 0:
        context._EvaluationStack.PushT(context._EvaluationStack.Remove(n))

    # return self.ExecuteImmediate()


def execROT(self, context, opcode):
    context._EvaluationStack.PushT(context._EvaluationStack.Remove(2))
    # return self.ExecuteImmediate()


def execSWAP(self, context, opcode):
    context._EvaluationStack.PushT(context._EvaluationStack.Remove(1))

    # return self.ExecuteImmediate()


def execTUCK(self, context, opcode):
    context._EvaluationStack.Insert(2, context._EvaluationStack.Peek())

    # return self.ExecuteImmediate()


def execCAT(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetByteArray()
    x1 = context._EvaluationStack.Pop().GetByteArray()

    if not self.CheckMaxItemSize(len(x1) + len(x2)):
        return self.VM_FAULT_and_report(VMFault.CAT_EXCEED_MAXITEMSIZE)

    context._EvaluationStack.PushT(x1 + x2)

    # return self.ExecuteImmediate()


def execSUBSTR(self, context, opcode):
    count = context._EvaluationStack.Pop().GetBigInteger()
    if count < 0:
        return self.VM_FAULT_and_report(VMFault.SUBSTR_INVALID_LENGTH)

    index = context._EvaluationStack.Pop().GetBigInteger()
    if index < 0:
        return self.VM_FAULT_and_report(VMFault.SUBSTR_INVALID_INDEX)

    x = context._EvaluationStack.Pop().GetByteArray()

    context._EvaluationStack.PushT(x[index:count + index])

    # return self.ExecuteImmediate()


def execLEFT(self, context, opcode):
    count = context._EvaluationStack.Pop().GetBigInteger()
    if count < 0:
        return self.VM_FAULT_and_report(VMFault.LEFT_INVALID_COUNT)

    x = context._EvaluationStack.Pop().GetByteArray()

    context._EvaluationStack.PushT(x[:count])

    # return self.ExecuteImmediate()


def execRIGHT(self, context, opcode):
    count = context._EvaluationStack.Pop().GetBigInteger()
    if count < 0:
        return self.VM_FAULT_and_report(VMFault.RIGHT_INVALID_COUNT)

    x = context._EvaluationStack.Pop().GetByteArray()
    if len(x) < count:
        return self.VM_FAULT_and_report(VMFault.RIGHT_UNKNOWN)

    context._EvaluationStack.PushT(x[-count:])

    # return self.ExecuteImmediate()


def execSIZE(self, context, opcode):
    x = context._EvaluationStack.Pop()
    context._EvaluationStack.PushT(x.GetByteLength())

    # return self.ExecuteImmediate()


def execINVERT(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(~x)

    # return self.ExecuteImmediate()


def execAND(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(x1 & x2)

    # return self.ExecuteImmediate()


def execOR(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(x1 | x2)

    # return self.ExecuteImmediate()


def execXOR(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(x1 ^ x2)

    # return self.ExecuteImmediate()


def execEQUAL(self, context, opcode):
    x2 = context._EvaluationStack.Pop()
    x1 = context._EvaluationStack.Pop()
    context._EvaluationStack.PushT(x1.Equals(x2))

    # return self.ExecuteImmediate()


def execINC(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()

    if not self.CheckBigInteger(x) or not self.CheckBigInteger(x + 1):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x + 1)

    # return self.ExecuteImmediate()


def execDEC(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()

    if not self.CheckBigInteger(x) or not self.CheckBigInteger(x + 1):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x - 1)

    # return self.ExecuteImmediate()


def execSIGN(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(x.Sign)

    # return self.ExecuteImmediate()


def execNEGATE(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(-x)

    # return self.ExecuteImmediate()


def execABS(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(abs(x))

    # return self.ExecuteImmediate()


def execNOT(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(not x)

    # return self.ExecuteImmediate()


def execNZ(self, context, opcode):
    x = context._EvaluationStack.Pop().GetBigInteger()
    context._EvaluationStack.PushT(x is not 0)

    # return self.ExecuteImmediate()


def execADD(self, context, opcode):
    x1 = context._EvaluationStack.Pop().GetBigInteger()
    x2 = context._EvaluationStack.Pop().GetBigInteger()

    if not self.CheckBigInteger(x1) or not self.CheckBigInteger(x2) or not self.CheckBigInteger(x1 + x2):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x1 + x2)

    # return self.ExecuteImmediate()


def execSUB(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    if not self.CheckBigInteger(x1) or not self.CheckBigInteger(x2) or not self.CheckBigInteger(x1 - x2):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x1 - x2)

    # return self.ExecuteImmediate()


def execMUL(self, context, opcode):
    x1 = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckBigInteger(x1):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    x2 = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckBigInteger(x2):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    result = x1 * x2
    if not self.CheckBigInteger(result):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(result)

    # return self.ExecuteImmediate()


def execDIV(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckBigInteger(x2):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    x1 = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckBigInteger(x1):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x1 / x2)

    # return self.ExecuteImmediate()


def execMOD(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckBigInteger(x2):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    x1 = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckBigInteger(x1):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x1 % x2)

    # return self.ExecuteImmediate()


def execSHL(self, context, opcode):
    shift = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckShift(shift):
        return self.VM_FAULT_and_report(VMFault.INVALID_SHIFT)

    x = context._EvaluationStack.Pop().GetBigInteger()

    if not self.CheckBigInteger(x):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    x = x << shift

    if not self.CheckBigInteger(x):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x)

    # return self.ExecuteImmediate()


def execSHR(self, context, opcode):
    shift = context._EvaluationStack.Pop().GetBigInteger()
    if not self.CheckShift(shift):
        return self.VM_FAULT_and_report(VMFault.INVALID_SHIFT)

    x = context._EvaluationStack.Pop().GetBigInteger()

    if not self.CheckBigInteger(x):
        return self.VM_FAULT_and_report(VMFault.BIGINTEGER_EXCEED_LIMIT)

    context._EvaluationStack.PushT(x >> shift)

    # return self.ExecuteImmediate()


def execBOOLAND(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBoolean()
    x1 = context._EvaluationStack.Pop().GetBoolean()

    context._EvaluationStack.PushT(x1 and x2)

    # return self.ExecuteImmediate()


def execBOOLOR(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBoolean()
    x1 = context._EvaluationStack.Pop().GetBoolean()

    context._EvaluationStack.PushT(x1 or x2)

    # return self.ExecuteImmediate()


def execNUMEQUAL(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(x2 == x1)

    # return self.ExecuteImmediate()


def execNUMNOTEQUAL(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(x1 != x2)

    # return self.ExecuteImmediate()


def execLT(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(x1 < x2)

    # return self.ExecuteImmediate()


def execGT(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(x1 > x2)

    # return self.ExecuteImmediate()


def execLTE(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(x1 <= x2)

    # return self.ExecuteImmediate()


def execGTE(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(x1 >= x2)

    # return self.ExecuteImmediate()


def execMIN(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(min(x1, x2))

    # return self.ExecuteImmediate()


def execMAX(self, context, opcode):
    x2 = context._EvaluationStack.Pop().GetBigInteger()
    x1 = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(max(x1, x2))

    # return self.ExecuteImmediate()


def execWITHIN(self, context, opcode):
    b = context._EvaluationStack.Pop().GetBigInteger()
    a = context._EvaluationStack.Pop().GetBigInteger()
    x = context._EvaluationStack.Pop().GetBigInteger()

    context._EvaluationStack.PushT(a <= x and x < b)

    # return self.ExecuteImmediate()


def execSHA1(self, context, opcode):
    h = hashlib.sha1(context._EvaluationStack.Pop().GetByteArray())
    context._EvaluationStack.PushT(h.digest())

    # return self.ExecuteImmediate()


def execSHA256(self, context, opcode):
    h = hashlib.sha256(context._EvaluationStack.Pop().GetByteArray())
    context._EvaluationStack.PushT(h.digest())

    # return self.ExecuteImmediate()


def execHASH160(self, context, opcode):
    context._EvaluationStack.PushT(self.Crypto.Hash160(context._EvaluationStack.Pop().GetByteArray()))

    # return self.ExecuteImmediate()


def execHASH256(self, context, opcode):
    context._EvaluationStack.PushT(self.Crypto.Hash256(context._EvaluationStack.Pop().GetByteArray()))

    # return self.ExecuteImmediate()


def execCHECKSIG(self, context, opcode):
    pubkey = context._EvaluationStack.Pop().GetByteArray()
    sig = context._EvaluationStack.Pop().GetByteArray()
    container = self.ScriptContainer
    if not container:
        logger.debug("Cannot check signature without container")
        context._EvaluationStack.PushT(False)
        return
    try:
        res = self.Crypto.VerifySignature(container.GetMessage(), sig, pubkey)
        context._EvaluationStack.PushT(res)
    except Exception as e:
        context._EvaluationStack.PushT(False)
        logger.debug("Could not checksig: %s " % e)

    # return self.ExecuteImmediate()


def execVERIFY(self, context, opcode):
    pubkey = context._EvaluationStack.Pop().GetByteArray()
    sig = context._EvaluationStack.Pop().GetByteArray()
    message = context._EvaluationStack.Pop().GetByteArray()
    try:
        res = self.Crypto.VerifySignature(message, sig, pubkey, unhex=False)
        context._EvaluationStack.PushT(res)
    except Exception as e:
        context._EvaluationStack.PushT(False)
        logger.debug("Could not verify: %s " % e)

    # return self.ExecuteImmediate()


def execCHECKMULTISIG(self, context, opcode):
    item = context._EvaluationStack.Pop()
    pubkeys = []

    if isinstance(item, Array):

        for p in item.GetArray():
            pubkeys.append(p.GetByteArray())
        n = len(pubkeys)
        if n == 0:
            return self.VM_FAULT_and_report(VMFault.CHECKMULTISIG_INVALID_PUBLICKEY_COUNT)

    else:
        n = item.GetBigInteger()

        if n < 1 or n > context._EvaluationStack.Count:
            return self.VM_FAULT_and_report(VMFault.CHECKMULTISIG_INVALID_PUBLICKEY_COUNT)

        for i in range(0, n):
            pubkeys.append(context._EvaluationStack.Pop().GetByteArray())

    item = context._EvaluationStack.Pop()
    sigs = []

    if isinstance(item, Array):
        for s in item.GetArray():
            sigs.append(s.GetByteArray())
        m = len(sigs)

        if m == 0 or m > n:
            return self.VM_FAULT_and_report(VMFault.CHECKMULTISIG_SIGNATURE_ERROR, m, n)
    else:
        m = item.GetBigInteger()

        if m < 1 or m > n or m > context._EvaluationStack.Count:
            return self.VM_FAULT_and_report(VMFault.CHECKMULTISIG_SIGNATURE_ERROR, m, n)

        for i in range(0, m):
            sigs.append(context._EvaluationStack.Pop().GetByteArray())

    message = self.ScriptContainer.GetMessage() if self.ScriptContainer else ''

    fSuccess = True

    try:

        i = 0
        j = 0

        while fSuccess and i < m and j < n:

            if self.Crypto.VerifySignature(message, sigs[i], pubkeys[j]):
                i += 1
            j += 1

            if m - i > n - j:
                fSuccess = False

    except Exception as e:
        fSuccess = False

    context._EvaluationStack.PushT(fSuccess)

    # return self.ExecuteImmediate()


def execARRAYSIZE(self, context, opcode):
    item = context._EvaluationStack.Pop()

    if not item:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN7)

    if isinstance(item, CollectionMixin):
        context._EvaluationStack.PushT(item.Count)

    else:
        context._EvaluationStack.PushT(len(item.GetByteArray()))

    # return self.ExecuteImmediate()


def execPACK(self, context, opcode):
    size = context._EvaluationStack.Pop().GetBigInteger()

    if size < 0 or size > context._EvaluationStack.Count or not self.CheckArraySize(size):
        return self.VM_FAULT_and_report(VMFault.UNKNOWN8)

    items = []

    for i in range(0, size):
        topack = context._EvaluationStack.Pop()
        items.append(topack)

    context._EvaluationStack.PushT(items)

    # return self.ExecuteImmediate()


def execUNPACK(self, context, opcode):
    item = context._EvaluationStack.Pop()

    if not isinstance(item, Array):
        return self.VM_FAULT_and_report(VMFault.UNPACK_INVALID_TYPE, item)

    items = item.GetArray()
    items.reverse()

    [context._EvaluationStack.PushT(i) for i in items]

    context._EvaluationStack.PushT(len(items))

    # return self.ExecuteImmediate()


def execPICKITEM(self, context, opcode):
    key = context._EvaluationStack.Pop()

    if isinstance(key, CollectionMixin):
        # key must be an array index or dictionary key, but not a collection
        return self.VM_FAULT_and_report(VMFault.KEY_IS_COLLECTION, key)

    collection = context._EvaluationStack.Pop()

    if isinstance(collection, Array):
        index = key.GetBigInteger()
        if index < 0 or index >= collection.Count:
            return self.VM_FAULT_and_report(VMFault.PICKITEM_INVALID_INDEX, index, collection.Count)

        items = collection.GetArray()
        to_pick = items[index]
        context._EvaluationStack.PushT(to_pick)

    elif isinstance(collection, Map):
        success, value = collection.TryGetValue(key)

        if success:
            context._EvaluationStack.PushT(value)

        else:
            return self.VM_FAULT_and_report(VMFault.DICT_KEY_NOT_FOUND, key, collection.Keys)
    else:
        return self.VM_FAULT_and_report(VMFault.PICKITEM_INVALID_TYPE, key, collection)

    # return self.ExecuteImmediate()


def execSETITEM(self, context, opcode):
    value = context._EvaluationStack.Pop()

    if isinstance(value, Struct):
        value = value.Clone()

    key = context._EvaluationStack.Pop()

    if isinstance(key, CollectionMixin):
        return self.VM_FAULT_and_report(VMFault.KEY_IS_COLLECTION)

    collection = context._EvaluationStack.Pop()

    if isinstance(collection, Array):

        index = key.GetBigInteger()

        if index < 0 or index >= collection.Count:
            return self.VM_FAULT_and_report(VMFault.SETITEM_INVALID_INDEX)

        items = collection.GetArray()
        items[index] = value

    elif isinstance(collection, Map):
        if not collection.ContainsKey(key) and not self.CheckArraySize(collection.Count + 1):
            return self.VM_FAULT_and_report(VMFault.SETITEM_INVALID_MAP)

        collection.SetItem(key, value)

    else:
        return self.VM_FAULT_and_report(VMFault.SETITEM_INVALID_TYPE, key, collection)

    # return self.ExecuteImmediate()


def execNEWARRAYSTRUCT(self, context, opcode):

    item = context._EvaluationStack.Pop()
    if isinstance(item, Array):
        result = None
        if isinstance(item, Struct):
            if opcode == opc.NEWSTRUCT:
                result = item
        else:
            if opcode == opc.NEWARRAY:
                result = item

        if result is None:
            result = Array(item) if opcode == opc.NEWARRAY else Struct(item)

        context._EvaluationStack.PushT(result)

    else:
        count = item.GetBigInteger()
        if count < 0:
            return self.VM_FAULT_and_report(VMFault.NEWARRAY_NEGATIVE_COUNT)

        if not self.CheckArraySize(count):
            return self.VM_FAULT_and_report(VMFault.NEWARRAY_EXCEED_ARRAYLIMIT)

        items = [Boolean(False) for i in range(0, count)]

        result = Array(items) if opcode == opc.NEWARRAY else Struct(items)

        context._EvaluationStack.PushT(result)

    # return self.ExecuteImmediate()


def execNEWMAP(self, context, opcode):
    context._EvaluationStack.PushT(Map())

    # return self.ExecuteImmediate()


def execAPPEND(self, context, opcode):
    newItem = context._EvaluationStack.Pop()

    if isinstance(newItem, Struct):
        newItem = newItem.Clone()

    arrItem = context._EvaluationStack.Pop()

    if not isinstance(arrItem, Array):
        return self.VM_FAULT_and_report(VMFault.APPEND_INVALID_TYPE, arrItem)

    arr = arrItem.GetArray()
    if not self.CheckArraySize(len(arr) + 1):
        return self.VM_FAULT_and_report(VMFault.APPEND_EXCEED_ARRAYLIMIT)
    arr.append(newItem)

    # return self.ExecuteImmediate()


def execREVERSE(self, context, opcode):
    arrItem = context._EvaluationStack.Pop()

    if not isinstance(arrItem, Array):
        return self.VM_FAULT_and_report(VMFault.REVERSE_INVALID_TYPE, arrItem)

    arrItem.Reverse()

    # return self.ExecuteImmediate()


def execREMOVE(self, context, opcode):
    key = context._EvaluationStack.Pop()

    if isinstance(key, CollectionMixin):
        return self.VM_FAULT_and_report(VMFault.UNKNOWN1)

    collection = context._EvaluationStack.Pop()

    if isinstance(collection, Array):

        index = key.GetBigInteger()

        if index < 0 or index >= collection.Count:
            return self.VM_FAULT_and_report(VMFault.REMOVE_INVALID_INDEX, index, collection.Count)

        collection.RemoveAt(index)

    elif isinstance(collection, Map):

        collection.Remove(key)

    else:

        return self.VM_FAULT_and_report(VMFault.REMOVE_INVALID_TYPE, key, collection)

    # return self.ExecuteImmediate()


def execHASKEY(self, context, opcode):
    key = context._EvaluationStack.Pop()

    if isinstance(key, CollectionMixin):
        return self.VM_FAULT_and_report(VMFault.DICT_KEY_ERROR)

    collection = context._EvaluationStack.Pop()

    if isinstance(collection, Array):

        index = key.GetBigInteger()

        if index < 0:
            return self.VM_FAULT_and_report(VMFault.DICT_KEY_ERROR)

        context._EvaluationStack.PushT(index < collection.Count)

    elif isinstance(collection, Map):

        context._EvaluationStack.PushT(collection.ContainsKey(key))

    else:

        return self.VM_FAULT_and_report(VMFault.DICT_KEY_ERROR)

    # return self.ExecuteImmediate()


def execKEYS(self, context, opcode):
    collection = context._EvaluationStack.Pop()

    if isinstance(collection, Map):

        context._EvaluationStack.PushT(Array(collection.Keys))
    else:
        return self.VM_FAULT_and_report(VMFault.DICT_KEY_ERROR)

    # return self.ExecuteImmediate()


def execVALUES(self, context, opcode):
    collection = context._EvaluationStack.Pop()
    values = []

    if isinstance(collection, Map):
        values = collection.Values

    elif isinstance(collection, Array):
        values = collection

    else:
        return self.VM_FAULT_and_report(VMFault.DICT_KEY_ERROR)

    newArray = Array()
    for item in values:
        if isinstance(item, Struct):
            newArray.Add(item.Clone())
        else:
            newArray.Add(item)

    context._EvaluationStack.PushT(newArray)

    # return self.ExecuteImmediate()


def execCALL_I(self, context, opcode):
    if not self.CheckMaxInvocationStack():
        return self.VM_FAULT_and_report(VMFault.CALL__I_EXCEED_MAX_INVOCATIONSTACK_SIZE)

    rvcount = context.CurrentInstruction.Operand[0]
    pcount = context.CurrentInstruction.Operand[1]

    if context._EvaluationStack.Count < pcount:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN_STACKISOLATION)

    context_call = self._LoadScriptInternal(context.Script, rvcount)
    context_call.InstructionPointer = context.InstructionPointer + context.CurrentInstruction.TokenI16_1 + 2

    if context_call.InstructionPointer < 0 or context_call.InstructionPointer > context_call.Script.Length:
        return False

    context._EvaluationStack.CopyTo(context_call.EvaluationStack, pcount)

    for i in range(0, pcount, 1):
        context._EvaluationStack.Pop()

    # return self.ExecuteImmediate()


def execCALL_E(self, context, opcode):
    if self._Table is None:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN_STACKISOLATION2)

    rvcount = context.CurrentInstruction.Operand[0]
    pcount = context.CurrentInstruction.Operand[1]

    if context._EvaluationStack.Count < pcount:
        return self.VM_FAULT_and_report(VMFault.UNKNOWN_STACKISOLATION)

    if opcode in [opc.CALL_ET, opc.CALL_EDT]:
        if context._RVCount != rvcount:
            return self.VM_FAULT_and_report(VMFault.UNKNOWN_STACKISOLATION3)
    else:
        if not self.CheckMaxInvocationStack():
            return self.VM_FAULT_and_report(VMFault.UNKNOWN_EXCEED_MAX_INVOCATIONSTACK_SIZE)

    if opcode in [opc.CALL_ED, opc.CALL_EDT]:
        script_hash = context._EvaluationStack.Pop().GetByteArray()
    else:
        script_hash = context.CurrentInstruction.ReadBytes(2, 20)

    context_new = self._LoadScriptByHash(script_hash, rvcount)
    if context_new is None:
        return self.VM_FAULT_and_report(VMFault.INVALID_CONTRACT, script_hash)

    context._EvaluationStack.CopyTo(context_new.EvaluationStack, pcount)

    if opcode in [opc.CALL_ET, opc.CALL_EDT]:
        self._InvocationStack.Remove(1)
    else:
        for i in range(0, pcount, 1):
            context._EvaluationStack.Pop()

    # return self.ExecuteImmediate()


def execTHROW(self, context, opcode):
    return self.VM_FAULT_and_report(VMFault.THROW)


def execTHROWIFNOT(self, context, opcode):
    if not context._EvaluationStack.Pop().GetBoolean():
        return self.VM_FAULT_and_report(VMFault.THROWIFNOT)

    # return self.ExecuteImmediate()


class ExecutionEngine:
    log_file_name = 'vm_instructions.log'
    # file descriptor
    log_file = None
    _vm_debugger = None

    MaxSizeForBigInteger = 32
    max_shl_shr = 256
    min_shl_shr = -256
    maxItemSize = 1024 * 1024
    maxArraySize = 1024
    maxStackSize = 2048
    maxInvocationStackSize = 1024
    opDict = {
        opc.PUSH0: execPUSH0,
        opc.PUSHF: execPUSH0,
        opc.PUSHBYTES1: execPUSHBYTES,
        opc.PUSHBYTES2: execPUSHBYTES,
        opc.PUSHBYTES3: execPUSHBYTES,
        opc.PUSHBYTES4: execPUSHBYTES,
        opc.PUSHBYTES5: execPUSHBYTES,
        opc.PUSHBYTES6: execPUSHBYTES,
        opc.PUSHBYTES7: execPUSHBYTES,
        opc.PUSHBYTES8: execPUSHBYTES,
        opc.PUSHBYTES9: execPUSHBYTES,
        opc.PUSHBYTES10: execPUSHBYTES,
        opc.PUSHBYTES11: execPUSHBYTES,
        opc.PUSHBYTES12: execPUSHBYTES,
        opc.PUSHBYTES13: execPUSHBYTES,
        opc.PUSHBYTES14: execPUSHBYTES,
        opc.PUSHBYTES15: execPUSHBYTES,
        opc.PUSHBYTES16: execPUSHBYTES,
        opc.PUSHBYTES17: execPUSHBYTES,
        opc.PUSHBYTES18: execPUSHBYTES,
        opc.PUSHBYTES19: execPUSHBYTES,
        opc.PUSHBYTES20: execPUSHBYTES,
        opc.PUSHBYTES21: execPUSHBYTES,
        opc.PUSHBYTES22: execPUSHBYTES,
        opc.PUSHBYTES23: execPUSHBYTES,
        opc.PUSHBYTES24: execPUSHBYTES,
        opc.PUSHBYTES25: execPUSHBYTES,
        opc.PUSHBYTES26: execPUSHBYTES,
        opc.PUSHBYTES27: execPUSHBYTES,
        opc.PUSHBYTES28: execPUSHBYTES,
        opc.PUSHBYTES29: execPUSHBYTES,
        opc.PUSHBYTES30: execPUSHBYTES,
        opc.PUSHBYTES31: execPUSHBYTES,
        opc.PUSHBYTES32: execPUSHBYTES,
        opc.PUSHBYTES33: execPUSHBYTES,
        opc.PUSHBYTES34: execPUSHBYTES,
        opc.PUSHBYTES35: execPUSHBYTES,
        opc.PUSHBYTES36: execPUSHBYTES,
        opc.PUSHBYTES37: execPUSHBYTES,
        opc.PUSHBYTES38: execPUSHBYTES,
        opc.PUSHBYTES39: execPUSHBYTES,
        opc.PUSHBYTES40: execPUSHBYTES,
        opc.PUSHBYTES41: execPUSHBYTES,
        opc.PUSHBYTES42: execPUSHBYTES,
        opc.PUSHBYTES43: execPUSHBYTES,
        opc.PUSHBYTES44: execPUSHBYTES,
        opc.PUSHBYTES45: execPUSHBYTES,
        opc.PUSHBYTES46: execPUSHBYTES,
        opc.PUSHBYTES47: execPUSHBYTES,
        opc.PUSHBYTES48: execPUSHBYTES,
        opc.PUSHBYTES49: execPUSHBYTES,
        opc.PUSHBYTES50: execPUSHBYTES,
        opc.PUSHBYTES51: execPUSHBYTES,
        opc.PUSHBYTES52: execPUSHBYTES,
        opc.PUSHBYTES53: execPUSHBYTES,
        opc.PUSHBYTES54: execPUSHBYTES,
        opc.PUSHBYTES55: execPUSHBYTES,
        opc.PUSHBYTES56: execPUSHBYTES,
        opc.PUSHBYTES57: execPUSHBYTES,
        opc.PUSHBYTES58: execPUSHBYTES,
        opc.PUSHBYTES59: execPUSHBYTES,
        opc.PUSHBYTES60: execPUSHBYTES,
        opc.PUSHBYTES61: execPUSHBYTES,
        opc.PUSHBYTES62: execPUSHBYTES,
        opc.PUSHBYTES63: execPUSHBYTES,
        opc.PUSHBYTES64: execPUSHBYTES,
        opc.PUSHBYTES65: execPUSHBYTES,
        opc.PUSHBYTES66: execPUSHBYTES,
        opc.PUSHBYTES67: execPUSHBYTES,
        opc.PUSHBYTES68: execPUSHBYTES,
        opc.PUSHBYTES69: execPUSHBYTES,
        opc.PUSHBYTES70: execPUSHBYTES,
        opc.PUSHBYTES71: execPUSHBYTES,
        opc.PUSHBYTES72: execPUSHBYTES,
        opc.PUSHBYTES73: execPUSHBYTES,
        opc.PUSHBYTES74: execPUSHBYTES,
        opc.PUSHBYTES75: execPUSHBYTES,
        opc.PUSHDATA1: execPUSHBYTES,
        opc.PUSHDATA2: execPUSHBYTES,
        opc.PUSHDATA4: execPUSHBYTES,
        opc.PUSHM1: execPUSHOPS,
        opc.PUSH1: execPUSHOPS,
        opc.PUSHT: execPUSHOPS,
        opc.PUSH2: execPUSHOPS,
        opc.PUSH3: execPUSHOPS,
        opc.PUSH4: execPUSHOPS,
        opc.PUSH5: execPUSHOPS,
        opc.PUSH6: execPUSHOPS,
        opc.PUSH7: execPUSHOPS,
        opc.PUSH8: execPUSHOPS,
        opc.PUSH9: execPUSHOPS,
        opc.PUSH10: execPUSHOPS,
        opc.PUSH11: execPUSHOPS,
        opc.PUSH12: execPUSHOPS,
        opc.PUSH13: execPUSHOPS,
        opc.PUSH14: execPUSHOPS,
        opc.PUSH15: execPUSHOPS,
        opc.PUSH16: execPUSHOPS,
        opc.NOP: execNOP,
        opc.JMP: execJMPALL,
        opc.JMPIF: execJMPALL,
        opc.JMPIFNOT: execJMPALL,
        opc.CALL: execCALL,
        opc.RET: execRET,
        opc.APPCALL: execAPPTAILCALL,
        opc.TAILCALL: execAPPTAILCALL,
        opc.SYSCALL: execSYSCALL,
        opc.DUPFROMALTSTACK: execDUPFROMALTSTACK,
        opc.TOALTSTACK: execTOALTSTACK,
        opc.FROMALTSTACK: execFROMALTSTACK,
        opc.XDROP: execXDROP,
        opc.XSWAP: execXSWAP,
        opc.XTUCK: execXTUCK,
        opc.DEPTH: execDEPTH,
        opc.DROP: execDROP,
        opc.DUP: execDUP,
        opc.NIP: execNIP,
        opc.OVER: execOVER,
        opc.PICK: execPICK,
        opc.ROLL: execROLL,
        opc.ROT: execROT,
        opc.SWAP: execSWAP,
        opc.TUCK: execTUCK,
        opc.CAT: execCAT,
        opc.SUBSTR: execSUBSTR,
        opc.LEFT: execLEFT,
        opc.RIGHT: execRIGHT,
        opc.SIZE: execSIZE,
        opc.INVERT: execINVERT,
        opc.AND: execAND,
        opc.OR: execOR,
        opc.XOR: execXOR,
        opc.EQUAL: execEQUAL,
        opc.INC: execINC,
        opc.DEC: execDEC,
        opc.SIGN: execSIGN,
        opc.NEGATE: execNEGATE,
        opc.ABS: execABS,
        opc.NOT: execNOT,
        opc.NZ: execNZ,
        opc.ADD: execADD,
        opc.SUB: execSUB,
        opc.MUL: execMUL,
        opc.DIV: execDIV,
        opc.MOD: execMOD,
        opc.SHL: execSHL,
        opc.SHR: execSHR,
        opc.BOOLAND: execBOOLAND,
        opc.BOOLOR: execBOOLOR,
        opc.NUMEQUAL: execNUMEQUAL,
        opc.NUMNOTEQUAL: execNUMNOTEQUAL,
        opc.LT: execLT,
        opc.GT: execGT,
        opc.LTE: execLTE,
        opc.GTE: execGTE,
        opc.MIN: execMIN,
        opc.MAX: execMAX,
        opc.WITHIN: execWITHIN,
        opc.SHA1: execSHA1,
        opc.SHA256: execSHA256,
        opc.HASH160: execHASH160,
        opc.HASH256: execHASH256,
        opc.CHECKSIG: execCHECKSIG,
        opc.VERIFY: execVERIFY,
        opc.CHECKMULTISIG: execCHECKMULTISIG,
        opc.ARRAYSIZE: execARRAYSIZE,
        opc.PACK: execPACK,
        opc.UNPACK: execUNPACK,
        opc.PICKITEM: execPICKITEM,
        opc.SETITEM: execSETITEM,
        opc.NEWARRAY: execNEWARRAYSTRUCT,
        opc.NEWSTRUCT: execNEWARRAYSTRUCT,
        opc.NEWMAP: execNEWMAP,
        opc.APPEND: execAPPEND,
        opc.REVERSE: execREVERSE,
        opc.REMOVE: execREMOVE,
        opc.HASKEY: execHASKEY,
        opc.KEYS: execKEYS,
        opc.VALUES: execVALUES,
        opc.CALL_I: execCALL_I,
        opc.CALL_E: execCALL_E,
        opc.CALL_ED: execCALL_E,
        opc.CALL_ET: execCALL_E,
        opc.CALL_EDT: execCALL_E,
        opc.THROW: execTHROW,
        opc.THROWIFNOT: execTHROWIFNOT
    }

    def __init__(self, container=None, crypto=None, table=None, service=None, exit_on_error=True):
        self._VMState = VMState.BREAK
        self._ScriptContainer = container
        self._Crypto = crypto
        self._Table = table
        self._Service = service
        self._exit_on_error = exit_on_error
        self._InvocationStack = RandomAccessStack(name='Invocation')
        self._ResultStack = RandomAccessStack(name='Result')
        self._ExecutedScriptHashes = []
        self.ops_processed = 0
        self.op_batch_counter = 0
        self.max_batch_count = 2
        self._debug_map = None
        self._is_write_log = settings.log_vm_instructions
        self._is_stackitem_count_strict = True
        self._stackitem_count = 0
        self._EntryScriptHash = None

    def CheckArraySize(self, length: int) -> bool:
        return length <= self.maxArraySize

    def CheckMaxItemSize(self, length: int) -> bool:
        return length >= 0 and length <= self.maxItemSize

    def CheckMaxInvocationStack(self) -> bool:
        return self.InvocationStack.Count < self.maxInvocationStackSize

    def CheckBigInteger(self, value: 'BigInteger') -> bool:
        return len(value.ToByteArray()) <= self.MaxSizeForBigInteger

    def CheckShift(self, shift: int) -> bool:
        return shift <= self.max_shl_shr and shift >= self.min_shl_shr

    def write_log(self, message):
        """
        Write a line to the VM instruction log file.

        Args:
            message (str): string message to write to file.
        """
        if self._is_write_log and self.log_file and not self.log_file.closed:
            self.log_file.write(message + '\n')

    @property
    def ScriptContainer(self):
        return self._ScriptContainer

    @property
    def Crypto(self):
        return self._Crypto

    @property
    def State(self):
        return self._VMState

    @property
    def InvocationStack(self):
        return self._InvocationStack

    @property
    def ResultStack(self):
        return self._ResultStack

    @property
    def CurrentContext(self) -> ExecutionContext:
        return self._InvocationStack.Peek()

    @property
    def CallingContext(self):
        if self._InvocationStack.Count > 1:
            return self.InvocationStack.Peek(1)
        return None

    @property
    def EntryContext(self):
        return self.InvocationStack.Peek(self.InvocationStack.Count - 1)

    @property
    def ExecutedScriptHashes(self):
        return self._ExecutedScriptHashes

    def LoadDebugInfoForScriptHash(self, debug_map, script_hash):
        if debug_map and script_hash:
            self._debug_map = debug_map
            self._debug_map['script_hash'] = script_hash

    def Dispose(self):
        self.InvocationStack.Clear()

    #@profileit
    def Execute(self):
        self._VMState &= ~VMState.BREAK

        def loop_stepinto():
            while self._VMState & VMState.HALT == 0 and self._VMState & VMState.FAULT == 0:  # and self._VMState & VMState.BREAK == 0:
                self.ExecuteNext()

        if settings.log_vm_instructions:
            with open(self.log_file_name, 'w') as self.log_file:
                self.write_log(str(datetime.datetime.now()))
                loop_stepinto()
        else:
            try:
                loop_stepinto()
            except:
                pass
        return not self._VMState & VMState.FAULT > 0

    def ExecuteInstruction(self):
        context = self.CurrentContext
        opcode = context.CurrentInstruction.OpCode

        try:
            opRet = self.opDict[opcode](self, context, opcode)
            if self._VMState & VMState.FAULT:
                return False
            if opRet or opRet is None:
                if self._VMState & VMState.HALT:
                    return True
                if opRet:
                    return True
                context.MoveNext()
                return True
        except KeyError:
            return self.VM_FAULT_and_report(VMFault.UNKNOWN_OPCODE, opcode)

        return True

    def LoadScript(self, script: bytearray, rvcount: int = -1) -> ExecutionContext:
        # "raw" bytes
        new_script = Script(self.Crypto, script)

        return self._LoadScriptInternal(new_script, rvcount)

    def _LoadScriptInternal(self, script: Script, rvcount=-1):
        context = ExecutionContext(script, rvcount)
        self._InvocationStack.PushT(context)
        self._ExecutedScriptHashes.append(context.ScriptHash())

        # add break points for current script if available
        script_hash = context.ScriptHash()
        if self._debug_map and script_hash == self._debug_map['script_hash']:
            if self.debugger:
                self.debugger._breakpoints[script_hash] = set(self._debug_map['breakpoints'])

        return context

    def _LoadScriptByHash(self, script_hash: bytearray, rvcount=-1):

        if self._Table is None:
            return None
        script = self._Table.GetScript(UInt160(data=script_hash).ToBytes())
        if script is None:
            return None
        return self._LoadScriptInternal(Script.FromHash(script_hash, script), rvcount)

    def PreExecuteInstruction(self):
        # allow overriding
        return True

    def PostExecuteInstruction(self):
        # allow overriding
        return True

    def ExecuteNext(self):
        if self._InvocationStack.Count == 0:
            self._VMState = VMState.HALT
        else:
            self.ops_processed += 1

            try:
                if self._is_write_log:
                    instruction = self.CurrentContext.CurrentInstruction
                    self.write_log("{} {} {}".format(self.ops_processed, instruction.InstructionName, self.CurrentContext.InstructionPointer))

                if not self.PreExecuteInstruction():
                    self._VMState = VMState.FAULT
                    #self._exit_on_error = False
                    raise VMException('FAULT')
                    #return False

                if not self.ExecuteInstruction():
                    self._VMState = VMState.FAULT
                    #self._exit_on_error = False
                    raise VMException('FAULT')
                    #return False

                if not self.PostExecuteInstruction():
                    self._VMState = VMState.FAULT
                    #self._exit_on_error = False
                    raise VMException('FAULT')
                    #return False

                #if self.op_batch_counter >= self.max_batch_count:
                #    self.op_batch_counter = 0
            except InvalidStackSize:
                self.VM_FAULT_and_report(VMFault.INVALID_STACKSIZE)
            #except VMException:
            #    #return False
            #    raise VMException()
            except Exception as e:
                error_msg = f"COULD NOT EXECUTE OP ({self.ops_processed}): {e}"
                # traceback.print_exc()
                self.write_log(error_msg)

                if self._exit_on_error:
                    self._VMState = VMState.FAULT
                    #raise VMException('VM FAULT check error log')

    def VM_FAULT_and_report(self, id, *args):
        self._VMState = VMState.FAULT

        if not logger.hasHandlers() or logger.handlers[0].level != LOGGING_LEVEL_DEBUG:
            return False

        # if settings.log_level != LOGGING_LEVEL_DEBUG:
        #     return

        if id == VMFault.INVALID_JUMP:
            error_msg = "Attemping to JMP/JMPIF/JMPIFNOT to an invalid location."

        elif id == VMFault.INVALID_CONTRACT:
            script_hash = args[0]
            error_msg = "Trying to call an unknown contract with script_hash {}\nMake sure the contract exists on the blockchain".format(script_hash)

        elif id == VMFault.CHECKMULTISIG_INVALID_PUBLICKEY_COUNT:
            error_msg = "CHECKMULTISIG - provided public key count is less than 1."

        elif id == VMFault.CHECKMULTISIG_SIGNATURE_ERROR:
            if args[0] < 1:
                error_msg = "CHECKMULTISIG - Minimum required signature count cannot be less than 1."
            else:  # m > n
                m = args[0]
                n = args[1]
                error_msg = "CHECKMULTISIG - Insufficient signatures provided ({}). Minimum required is {}".format(m, n)

        elif id == VMFault.UNPACK_INVALID_TYPE:
            item = args[0]
            error_msg = "Failed to UNPACK item. Item is not an array but of type: {}".format(type(item))

        elif id == VMFault.PICKITEM_INVALID_TYPE:
            index = args[0]
            item = args[1]
            error_msg = "Cannot access item at index {}. Item is not an Array or Map but of type: {}".format(index, type(item))

        elif id == VMFault.PICKITEM_NEGATIVE_INDEX:
            error_msg = "Attempting to access an array using a negative index"

        elif id == VMFault.PICKITEM_INVALID_INDEX:
            index = args[0]
            length = args[1]
            error_msg = "Array index is less than zero or {} exceeds list length {}".format(index, length)

        elif id == VMFault.APPEND_INVALID_TYPE:
            item = args[0]
            error_msg = "Cannot append to item. Item is not an array but of type: {}".format(type(item))

        elif id == VMFault.REVERSE_INVALID_TYPE:
            item = args[0]
            error_msg = "Cannot REVERSE item. Item is not an array but of type: {}".format(type(item))

        elif id == VMFault.REMOVE_INVALID_TYPE:
            item = args[0]
            index = args[1]
            error_msg = "Cannot REMOVE item at index {}. Item is not an array but of type: {}".format(index, type(item))

        elif id == VMFault.REMOVE_INVALID_INDEX:
            index = args[0]
            length = args[1]

            if index < 0:
                error_msg = "Cannot REMOVE item at index {}. Index < 0".format(index)

            else:  # index >= len(items):
                error_msg = "Cannot REMOVE item at index {}. Index exceeds array length {}".format(index, length)

        elif id == VMFault.POP_ITEM_NOT_ARRAY:
            error_msg = "Items(s) not array: %s" % [item for item in args]

        elif id == VMFault.UNKNOWN_OPCODE:
            opcode = args[0]
            error_msg = "Unknown opcode found: {}".format(opcode)

        else:
            error_msg = id

        if id in [VMFault.THROW, VMFault.THROWIFNOT]:
            logger.debug("({}) {}".format(self.ops_processed, id))
        else:
            logger.debug("({}) {}".format(self.ops_processed, error_msg))

        return False
