//go:build !purego

#include "textflag.h"

#define m1 0xa0761d6478bd642f
#define m2 0xe7037ed1a0b428db
#define m3 0x8ebc6af09c88c6e3
#define m4 0x589965cc75374cc3
#define m5 0x1d8e4e27c47d124f

// func MultiHashUint32Array(hashes []uintptr, values sparse.Uint32Array, seed uintptr)
TEXT ·MultiHashUint32Array(SB), NOSPLIT, $0-56
    MOVD hashes_base+0(FP), R12
    MOVD values_array_ptr+24(FP), R13
    MOVD values_array_len+32(FP), R14
    MOVD values_array_off+40(FP), R15
    MOVD seed+48(FP), R11

    // Load constants
    MOVD $m1, R8
    MOVD $m2, R9
    MOVD $m5, R10
    EOR $4, R10, R10  // R10 = m5 ^ 4
    EOR R11, R8, R8   // R8 = seed ^ m1

    MOVD $0, R16  // index counter
    B test

loop:
    // Load value from sparse array
    MOVWU (R13), R0  // value = values.Index(i)
    
    // First mix: mix(uint64(value) ^ m2, uint64(value) ^ uint64(seed) ^ m1)
    EOR R9, R0, R1   // R1 = value ^ m2
    EOR R8, R0, R2   // R2 = value ^ (seed ^ m1) = value ^ seed ^ m1
    
    // Multiply R1 * R2 and get high ^ low
    // ARM64: MUL Rd, Rn, Rm computes Rd = Rn * Rm (low 64 bits)
    // ARM64: UMULH Rd, Rn, Rm computes Rd = (Rn * Rm) >> 64 (high 64 bits)
    MUL R1, R2, R3   // R3 = R1 * R2 (low 64 bits)
    UMULH R1, R2, R4 // R4 = (R1 * R2) >> 64 (high 64 bits)  
    EOR R4, R3, R5   // R5 = hi ^ lo (result of first mix)
    
    // Second mix: mix(m5 ^ 4, result_of_first_mix)  
    MUL R10, R5, R6  // R6 = R10 * R5 (low 64 bits)
    UMULH R10, R5, R7 // R7 = (R10 * R5) >> 64 (high 64 bits)
    EOR R7, R6, R0   // R0 = hi ^ lo (final hash result)
    
    // Store result
    MOVD R0, (R12)(R16<<3)
    
    // Advance to next element
    ADD $1, R16
    ADD R15, R13     // Advance sparse array pointer
    
test:
    CMP R16, R14
    BNE loop
    RET
