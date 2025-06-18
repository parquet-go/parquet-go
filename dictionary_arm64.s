//go:build !purego

#include "textflag.h"

#define errnoIndexOutOfBounds 1

// func dictionaryBoundsInt32(dict []int32, indexes []int32) (min, max int32, err errno)
TEXT ·dictionaryBoundsInt32(SB), NOSPLIT, $0-64
    MOVD dict_base+0(FP), R0
    MOVD dict_len+8(FP), R1

    MOVD indexes_base+24(FP), R2
    MOVD indexes_len+32(FP), R3

    MOVD $0, R4    // min (initialize to 0)
    MOVD $0, R5    // max (initialize to 0)  
    MOVD $0, R6    // err
    MOVD $1, R7    // loop counter (start at 1, element 0 already processed)

    // Early exit if no indexes
    CBZ R3, return_int32

    // Initialize min/max with first valid element
    MOVW (R2), R8     // Load first index
    // Check bounds: if (uint32)index >= (uint32)dict_len then error
    CMP R8, R1        // Compare index with dict_len (Go assembly: R1 - R8)
    BLS indexOutOfBounds_int32  // Branch if R1 <= R8 (i.e., dict_len <= index)
    MOVW (R0)(R8<<2), R4  // Load dict[index]
    MOVW R4, R5       // max = min initially

    // Simple scalar loop for now (no unrolling to avoid bugs)
    B scalar_loop_int32

scalar_loop_int32:
    CMP R7, R3
    BEQ return_int32
    
    // Process remaining elements one by one
    MOVW (R2)(R7<<2), R8
    CMP R8, R1
    BLS indexOutOfBounds_int32
    MOVW (R0)(R8<<2), R8
    
    // Update min/max (signed comparison)  
    CMP R4, R8   // Compare current_min with new_value
    CSEL LT, R8, R4, R4       // if new_value < current_min, select new_value
    CMP R5, R8   // Compare current_max with new_value
    CSEL GT, R8, R5, R5       // if new_value > current_max, select new_value
    
    ADD $1, R7
    B scalar_loop_int32

return_int32:
    MOVW R4, min+48(FP)
    MOVW R5, max+52(FP)
    MOVD R6, err+56(FP)
    RET

indexOutOfBounds_int32:
    MOVD $errnoIndexOutOfBounds, R6
    B return_int32

// func dictionaryBoundsUint32(dict []uint32, indexes []int32) (min, max uint32, err errno)
TEXT ·dictionaryBoundsUint32(SB), NOSPLIT, $0-64
    MOVD dict_base+0(FP), R0
    MOVD dict_len+8(FP), R1

    MOVD indexes_base+24(FP), R2
    MOVD indexes_len+32(FP), R3

    MOVD $0, R4    // min (initialize to 0)
    MOVD $0, R5    // max (initialize to 0)  
    MOVD $0, R6    // err
    MOVD $1, R7    // loop counter (start at 1, element 0 already processed)

    // Early exit if no indexes
    CBZ R3, return_uint32

    // Initialize min/max with first valid element
    MOVW (R2), R8     // Load first index
    // Check bounds: if (uint32)index >= (uint32)dict_len then error
    CMP R8, R1        // Compare index with dict_len (Go assembly: R1 - R8)
    BLS indexOutOfBounds_uint32  // Branch if R1 <= R8 (i.e., dict_len <= index)
    MOVW (R0)(R8<<2), R4  // Load dict[index]
    MOVW R4, R5       // max = min initially

    // Simple scalar loop for now (no unrolling to avoid bugs)
    B scalar_loop_uint32

scalar_loop_uint32:
    CMP R7, R3
    BEQ return_uint32
    
    // Process remaining elements one by one
    MOVW (R2)(R7<<2), R8
    CMP R8, R1
    BLS indexOutOfBounds_uint32
    MOVW (R0)(R8<<2), R8
    
    // Update min/max (unsigned comparison)
    CMP R4, R8   // Compare current_min with new_value  
    CSEL LO, R8, R4, R4       // if new_value < current_min (unsigned), select new_value
    CMP R5, R8   // Compare current_max with new_value
    CSEL HI, R8, R5, R5       // if new_value > current_max (unsigned), select new_value
    
    ADD $1, R7
    B scalar_loop_uint32

return_uint32:
    MOVW R4, min+48(FP)
    MOVW R5, max+52(FP)
    MOVD R6, err+56(FP)
    RET

indexOutOfBounds_uint32:
    MOVD $errnoIndexOutOfBounds, R6
    B return_uint32
