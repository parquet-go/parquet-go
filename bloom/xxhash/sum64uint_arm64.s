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

    // Check if we have at least 4 elements for unrolling
    CMP $4, R4
    BLT scalar_loop32

    // Calculate unrolled loop limit (process 4 at a time)
    SUB $3, R4, R10
    
unroll_loop32:
    CMP R9, R10
    BGE scalar_loop32
    
    // Process 4 values in parallel
    // Load 4 uint32 values
    MOVWU (R2)(R9<<2), R12      // v[0]
    ADD $1, R9, R16
    MOVWU (R2)(R16<<2), R15     // v[1] - using R15 instead of R17
    ADD $2, R9, R16
    MOVWU (R2)(R16<<2), R25     // v[2] - using R25 instead of R18
    ADD $3, R9, R16
    MOVWU (R2)(R16<<2), R26     // v[3] - using R26 instead of R19
    
    // Initialize 4 hash values
    MOVD R8, R11   // h[0] = PRIME5 + 4
    MOVD R8, R20   // h[1] = PRIME5 + 4
    MOVD R8, R21   // h[2] = PRIME5 + 4
    MOVD R8, R22   // h[3] = PRIME5 + 4
    
    // h[i] ^= v[i] * PRIME1 (parallel)
    MUL R5, R12
    MUL R5, R15
    MUL R5, R25
    MUL R5, R26
    EOR R12, R11
    EOR R15, R20
    EOR R25, R21
    EOR R26, R22
    
    // rol23(h[i]) = (h[i] << 23) | (h[i] >> 41) (parallel)
    LSL $23, R11, R13
    LSR $41, R11, R14
    LSL $23, R20, R23
    LSR $41, R20, R24
    ORR R13, R14, R11
    ORR R23, R24, R20
    
    LSL $23, R21, R13
    LSR $41, R21, R14
    LSL $23, R22, R23
    LSR $41, R22, R24
    ORR R13, R14, R21
    ORR R23, R24, R22
    
    // h[i] = h[i] * PRIME2 + PRIME3 (parallel)
    MUL R6, R11
    MUL R6, R20
    MUL R6, R21
    MUL R6, R22
    ADD R7, R11
    ADD R7, R20
    ADD R7, R21
    ADD R7, R22
    
    // avalanche(h[0])
    LSR $33, R11, R13
    EOR R13, R11
    MUL R6, R11
    LSR $29, R11, R13
    EOR R13, R11
    MUL R7, R11
    LSR $32, R11, R13
    EOR R13, R11
    
    // avalanche(h[1])
    LSR $33, R20, R23
    EOR R23, R20
    MUL R6, R20
    LSR $29, R20, R23
    EOR R23, R20
    MUL R7, R20
    LSR $32, R20, R23
    EOR R23, R20
    
    // avalanche(h[2])
    LSR $33, R21, R13
    EOR R13, R21
    MUL R6, R21
    LSR $29, R21, R13
    EOR R13, R21
    MUL R7, R21
    LSR $32, R21, R13
    EOR R13, R21
    
    // avalanche(h[3])
    LSR $33, R22, R23
    EOR R23, R22
    MUL R6, R22
    LSR $29, R22, R23
    EOR R23, R22
    MUL R7, R22
    LSR $32, R22, R23
    EOR R23, R22
    
    // Store 4 results
    MOVD R11, (R0)(R9<<3)
    ADD $1, R9, R16
    MOVD R20, (R0)(R16<<3)
    ADD $2, R9, R16
    MOVD R21, (R0)(R16<<3)
    ADD $3, R9, R16
    MOVD R22, (R0)(R16<<3)
    
    ADD $4, R9  // Advance by 4
    B unroll_loop32

scalar_loop32:
    CMP R9, R4
    BEQ done32
    
    // Standard scalar implementation for remaining elements
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

    // Check if we have at least 2 elements for unrolling (uint64 needs more registers)
    CMP $2, R4
    BLT scalar_loop64

    // Calculate unrolled loop limit (process 2 at a time)
    SUB $1, R4, R11
    
unroll_loop64:
    CMP R10, R11
    BGE scalar_loop64
    
    // Process 2 values in parallel
    // Load 2 uint64 values
    MOVD (R2)(R10<<3), R13      // v[0]
    ADD $1, R10, R16
    MOVD (R2)(R16<<3), R15      // v[1] - using R15 instead of R17
    
    // Initialize 2 hash values
    MOVD R9, R12   // h[0] = PRIME5 + 8
    MOVD R9, R20   // h[1] = PRIME5 + 8
    
    // round(0, v[i]) = v[i] * PRIME2, rol31, * PRIME1 (parallel)
    MUL R6, R13   // v[0] *= PRIME2
    MUL R6, R15   // v[1] *= PRIME2
    
    // rol31(v[i]) = (v[i] << 31) | (v[i] >> 33) (parallel)
    LSL $31, R13, R14
    LSR $33, R13, R21
    LSL $31, R15, R22
    LSR $33, R15, R23
    ORR R14, R21, R13
    ORR R22, R23, R15
    
    MUL R5, R13   // v[0] *= PRIME1
    MUL R5, R15   // v[1] *= PRIME1
    
    // h[i] ^= round(0, v[i]) (parallel)
    EOR R13, R12
    EOR R15, R20
    
    // rol27(h[i]) = (h[i] << 27) | (h[i] >> 37) (parallel)
    LSL $27, R12, R14
    LSR $37, R12, R21
    LSL $27, R20, R22
    LSR $37, R20, R23
    ORR R14, R21, R12
    ORR R22, R23, R20
    
    // h[i] = h[i] * PRIME1 + PRIME4 (parallel)
    MUL R5, R12
    MUL R5, R20
    ADD R8, R12
    ADD R8, R20
    
    // avalanche(h[0])
    LSR $33, R12, R14
    EOR R14, R12
    MUL R6, R12
    LSR $29, R12, R14
    EOR R14, R12
    MUL R7, R12
    LSR $32, R12, R14
    EOR R14, R12
    
    // avalanche(h[1])
    LSR $33, R20, R22
    EOR R22, R20
    MUL R6, R20
    LSR $29, R20, R22
    EOR R22, R20
    MUL R7, R20
    LSR $32, R20, R22
    EOR R22, R20
    
    // Store 2 results
    MOVD R12, (R0)(R10<<3)
    ADD $1, R10, R16
    MOVD R20, (R0)(R16<<3)
    
    ADD $2, R10  // Advance by 2
    B unroll_loop64

scalar_loop64:
    CMP R10, R4
    BEQ done64
    
    // Standard scalar implementation for remaining elements
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
