//go:build !purego

#include "textflag.h"

// func broadcastRangeInt32ARM64(dst []int32, base int32)
TEXT ·broadcastRangeInt32ARM64(SB), NOSPLIT, $0-28
    MOVD dst_base+0(FP), R0
    MOVD dst_len+8(FP), R1
    MOVW base+24(FP), R2
    MOVD $0, R3  // index

    // Check if we have enough elements for NEON vectorization (4 elements = 16 bytes)
    CMP $4, R1
    BLT scalar_loop

    // Create increment vector [0, 1, 2, 3] manually
    MOVW $0, R4
    MOVW $1, R5
    MOVW $2, R6
    MOVW $3, R7
    
    // Insert values into vector lanes
    VMOV R4, V1.S[0]
    VMOV R5, V1.S[1] 
    VMOV R6, V1.S[2]
    VMOV R7, V1.S[3]
    
    // Create base vector [base, base, base, base]
    VDUP R2, V0.S4
    
    // Add base to increments to get [base, base+1, base+2, base+3]
    VADD V1.S4, V0.S4, V0.S4
    
    // Create step vector [4, 4, 4, 4] for incrementing between iterations
    MOVD $4, R5
    VDUP R5, V2.S4

    // Calculate how many 4-element chunks we can process
    AND $~3, R1, R4  // Round down to nearest 4-element boundary

neon_loop:
    // Calculate address for current position
    LSL $2, R3, R6  // R6 = R3 * 4 (byte offset)
    ADD R0, R6, R6  // R6 = base + offset
    
    // Store current 4 values
    VST1 [V0.S4], (R6)
    
    // Add 4 to each element for next iteration
    VADD V2.S4, V0.S4, V0.S4
    
    ADD $4, R3
    CMP R3, R4
    BNE neon_loop

    // Process remaining elements
    CMP R3, R1
    BEQ done

scalar_loop:
    // Calculate current value: base + index
    ADDW R2, R3, R5
    // Calculate address and store
    LSL $2, R3, R6
    ADD R0, R6, R6
    MOVW R5, (R6)
    ADD $1, R3
    CMP R3, R1
    BNE scalar_loop

done:
    RET

// func writePointersBE128ARM64(values [][16]byte, rows sparse.Array)
TEXT ·writePointersBE128ARM64(SB), NOSPLIT, $0-48
    MOVD values_base+0(FP), R0
    MOVD rows_array_ptr+24(FP), R1
    MOVD rows_array_len+32(FP), R2
    MOVD rows_array_off+40(FP), R3

    MOVD $0, R4  // index counter
    B test

loop:
    // Clear V0 (16 bytes of zeros)
    VEOR V0.B16, V0.B16, V0.B16

    // Load pointer from sparse array
    MOVD (R1), R5
    CMP $0, R5
    BEQ store_zero

    // Load 16 bytes from pointer
    VLD1 (R5), [V0.B16]

store_zero:
    // Store 16 bytes to destination
    VST1 [V0.B16], (R0)

    // Advance pointers
    ADD $16, R0
    ADD R3, R1
    ADD $1, R4

test:
    CMP R4, R2
    BNE loop
    RET

// ARM64 constants for range operations  
GLOBL ·range0n4(SB), RODATA|NOPTR, $16
DATA ·range0n4+0(SB)/4, $0
DATA ·range0n4+4(SB)/4, $1
DATA ·range0n4+8(SB)/4, $2
DATA ·range0n4+12(SB)/4, $3
