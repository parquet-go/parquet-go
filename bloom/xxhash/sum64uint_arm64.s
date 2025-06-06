//go:build !purego

#include "textflag.h"

#define PRIME1 0x9E3779B185EBCA87
#define PRIME2 0xC2B2AE3D27D4EB4F  
#define PRIME3 0x165667B19E3779F9
#define PRIME4 0x85EBCA77C2B2AE63
#define PRIME5 0x27D4EB2F165667C5

// func MultiSum64Uint32(h []uint64, v []uint32) int
TEXT ·MultiSum64Uint32(SB), NOSPLIT, $0-54
    MOVD h_base+0(FP), R0
    MOVD h_len+8(FP), R1
    MOVD v_base+24(FP), R2
    MOVD v_len+32(FP), R3

    // n = min(len(h), len(v)) - FIXED: correct min logic
    CMP R1, R3           // Compare len(h) with len(v)
    CSEL LT, R3, R1, R4  // If len(h) > len(v), select len(v), else select len(h)
    MOVD R4, ret+48(FP)

    // Early exit if n == 0
    CBZ R4, done32

    // Load constants
    MOVD $PRIME1, R5
    MOVD $PRIME2, R6
    MOVD $PRIME3, R7
    MOVD $(PRIME5+4), R8

    MOVD $0, R9  // index counter

scalar_loop32:
    CMP R9, R4
    BEQ done32
    
    // Standard scalar implementation
    MOVD R8, R11  // h = PRIME5 + 4
    MOVWU (R2)(R9<<2), R12  // load uint32, zero-extend to uint64
    
    // h ^= v * PRIME1
    MUL R5, R12
    EOR R12, R11
    
    // rol23(h) = (h << 23) | (h >> 41)
    LSL $23, R11, R13
    LSR $41, R11, R14
    ORR R13, R14, R11
    
    // h = h * PRIME2 + PRIME3
    MUL R6, R11
    ADD R7, R11
    
    // avalanche(h)
    LSR $33, R11, R13
    EOR R13, R11
    MUL R6, R11
    LSR $29, R11, R13
    EOR R13, R11
    MUL R7, R11
    LSR $32, R11, R13
    EOR R13, R11
    
    // Store result
    MOVD R11, (R0)(R9<<3)
    ADD $1, R9
    B scalar_loop32

done32:
    RET

// func MultiSum64Uint64(h []uint64, v []uint64) int  
TEXT ·MultiSum64Uint64(SB), NOSPLIT, $0-54
    MOVD h_base+0(FP), R0
    MOVD h_len+8(FP), R1
    MOVD v_base+24(FP), R2
    MOVD v_len+32(FP), R3

    // n = min(len(h), len(v)) - FIXED: correct min logic
    CMP R1, R3           // Compare len(h) with len(v)
    CSEL LT, R3, R1, R4  // If len(h) > len(v), select len(v), else select len(h)
    MOVD R4, ret+48(FP)

    // Early exit if n == 0
    CBZ R4, done64

    // Load constants
    MOVD $PRIME1, R5
    MOVD $PRIME2, R6
    MOVD $PRIME3, R7
    MOVD $PRIME4, R8
    MOVD $(PRIME5+8), R9

    MOVD $0, R10  // index counter

scalar_loop64:
    CMP R10, R4
    BEQ done64
    
    // Standard scalar implementation
    MOVD R9, R12  // h = PRIME5 + 8
    MOVD (R2)(R10<<3), R13  // load uint64 value
    
    // round(0, v) = v * PRIME2, rol31, * PRIME1
    MUL R6, R13   // v *= PRIME2
    
    // rol31(v) = (v << 31) | (v >> 33)
    LSL $31, R13, R14
    LSR $33, R13, R15
    ORR R14, R15, R13
    
    MUL R5, R13   // v *= PRIME1
    
    // h ^= round(0, v)
    EOR R13, R12
    
    // rol27(h) = (h << 27) | (h >> 37)
    LSL $27, R12, R14
    LSR $37, R12, R15
    ORR R14, R15, R12
    
    // h = h * PRIME1 + PRIME4
    MUL R5, R12
    ADD R8, R12
    
    // avalanche(h)
    LSR $33, R12, R14
    EOR R14, R12
    MUL R6, R12
    LSR $29, R12, R14
    EOR R14, R12
    MUL R7, R12
    LSR $32, R12, R14
    EOR R14, R12
    
    // Store result
    MOVD R12, (R0)(R10<<3)
    ADD $1, R10
    B scalar_loop64

done64:
    RET
