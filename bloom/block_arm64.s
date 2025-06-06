//go:build !purego

#include "textflag.h"

#define salt0 0x47b6137b
#define salt1 0x44974d91
#define salt2 0x8824ad5b
#define salt3 0xa2b7289d
#define salt4 0x705495c7
#define salt5 0x2df1424b
#define salt6 0x9efc4947
#define salt7 0x5c6bfb31

// func blockInsert(b *Block, x uint32)
TEXT ·blockInsert(SB), NOSPLIT, $0-16
    MOVD b+0(FP), R0
    MOVWU x+8(FP), R1
    
    // Load salt constants
    MOVD $salt0, R2
    MOVD $salt1, R3
    MOVD $salt2, R4
    MOVD $salt3, R5
    MOVD $salt4, R6
    MOVD $salt5, R7
    MOVD $salt6, R8
    MOVD $salt7, R9
    
    // Process first 4 salt values with instruction overlap
    // First multiplication
    MUL R2, R1, R10   // R10 = x * salt0
    MUL R3, R1, R11   // R11 = x * salt1
    LSR $27, R10      // R10 = (x * salt0) >> 27
    LSR $27, R11      // R11 = (x * salt1) >> 27
    
    // Second multiplication while first completes
    MUL R4, R1, R12   // R12 = x * salt2
    MUL R5, R1, R13   // R13 = x * salt3
    
    // Create masks for first two values
    MOVW $1, R14
    LSLW R10, R14, R15  // R15 = 1 << ((x * salt0) >> 27)
    MOVW $1, R16
    LSLW R11, R16, R17  // R17 = 1 << ((x * salt1) >> 27)
    
    // Continue with second pair
    LSR $27, R12      // R12 = (x * salt2) >> 27
    LSR $27, R13      // R13 = (x * salt3) >> 27
    
    // Load current block values and apply first two masks
    MOVWU (R0), R10        // Load block[0]
    MOVWU 4(R0), R11       // Load block[1]
    ORR R15, R10           // block[0] |= mask0
    ORR R17, R11           // block[1] |= mask1
    MOVW R10, (R0)         // Store block[0]
    MOVW R11, 4(R0)        // Store block[1]
    
    // Create masks for second pair
    MOVW $1, R14
    LSLW R12, R14, R15  // R15 = 1 << ((x * salt2) >> 27)
    MOVW $1, R16
    LSLW R13, R16, R17  // R17 = 1 << ((x * salt3) >> 27)
    
    // Process last 4 salt values
    MUL R6, R1, R10   // R10 = x * salt4
    MUL R7, R1, R11   // R11 = x * salt5
    
    // Apply second pair masks
    MOVWU 8(R0), R12       // Load block[2]
    MOVWU 12(R0), R13      // Load block[3]
    ORR R15, R12           // block[2] |= mask2
    ORR R17, R13           // block[3] |= mask3
    MOVW R12, 8(R0)        // Store block[2]
    MOVW R13, 12(R0)       // Store block[3]
    
    // Continue with last multiplication
    LSR $27, R10      // R10 = (x * salt4) >> 27
    LSR $27, R11      // R11 = (x * salt5) >> 27
    MUL R8, R1, R12   // R12 = x * salt6
    MUL R9, R1, R13   // R13 = x * salt7
    
    // Create masks for third pair
    MOVW $1, R14
    LSLW R10, R14, R15  // R15 = 1 << ((x * salt4) >> 27)
    MOVW $1, R16
    LSLW R11, R16, R17  // R17 = 1 << ((x * salt5) >> 27)
    
    // Finish last multiplication
    LSR $27, R12      // R12 = (x * salt6) >> 27
    LSR $27, R13      // R13 = (x * salt7) >> 27
    
    // Apply third pair masks
    MOVWU 16(R0), R10      // Load block[4]
    MOVWU 20(R0), R11      // Load block[5]
    ORR R15, R10           // block[4] |= mask4
    ORR R17, R11           // block[5] |= mask5
    MOVW R10, 16(R0)       // Store block[4]
    MOVW R11, 20(R0)       // Store block[5]
    
    // Create masks for last pair
    MOVW $1, R14
    LSLW R12, R14, R15  // R15 = 1 << ((x * salt6) >> 27)
    MOVW $1, R16
    LSLW R13, R16, R17  // R17 = 1 << ((x * salt7) >> 27)
    
    // Apply last pair masks
    MOVWU 24(R0), R12      // Load block[6]
    MOVWU 28(R0), R13      // Load block[7]
    ORR R15, R12           // block[6] |= mask6
    ORR R17, R13           // block[7] |= mask7
    MOVW R12, 24(R0)       // Store block[6]
    MOVW R13, 28(R0)       // Store block[7]
    
    RET

// func blockCheck(b *Block, x uint32) bool
TEXT ·blockCheck(SB), NOSPLIT, $0-17
    MOVD b+0(FP), R0
    MOVWU x+8(FP), R1
    
    // Load salt constants
    MOVD $salt0, R2
    MOVD $salt1, R3
    MOVD $salt2, R4
    MOVD $salt3, R5
    MOVD $salt4, R6
    MOVD $salt5, R7
    MOVD $salt6, R8
    MOVD $salt7, R9
    
    // Process all 8 checks with instruction overlap
    // First 4 multiplications
    MUL R2, R1, R10   // R10 = x * salt0
    MUL R3, R1, R11   // R11 = x * salt1
    MUL R4, R1, R12   // R12 = x * salt2
    MUL R5, R1, R13   // R13 = x * salt3
    
    // Right shifts
    LSR $27, R10      // R10 = (x * salt0) >> 27
    LSR $27, R11      // R11 = (x * salt1) >> 27
    LSR $27, R12      // R12 = (x * salt2) >> 27
    LSR $27, R13      // R13 = (x * salt3) >> 27
    
    // Load block values for first 4 checks
    MOVWU (R0), R14        // R14 = block[0]
    MOVWU 4(R0), R15       // R15 = block[1]
    MOVWU 8(R0), R16       // R16 = block[2]
    MOVWU 12(R0), R17      // R17 = block[3]
    
    // Create masks and test first 4 bits
    MOVW $1, R2
    LSLW R10, R2, R2   // R2 = 1 << ((x * salt0) >> 27)
    AND R2, R14        // Test bit in block[0]
    CBZ R14, not_found
    
    MOVW $1, R3
    LSLW R11, R3, R3   // R3 = 1 << ((x * salt1) >> 27)
    AND R3, R15        // Test bit in block[1]
    CBZ R15, not_found
    
    MOVW $1, R4
    LSLW R12, R4, R4   // R4 = 1 << ((x * salt2) >> 27)
    AND R4, R16        // Test bit in block[2]
    CBZ R16, not_found
    
    MOVW $1, R5
    LSLW R13, R5, R5   // R5 = 1 << ((x * salt3) >> 27)
    AND R5, R17        // Test bit in block[3]
    CBZ R17, not_found
    
    // Process last 4 values
    MUL R6, R1, R10   // R10 = x * salt4
    MUL R7, R1, R11   // R11 = x * salt5
    MUL R8, R1, R12   // R12 = x * salt6
    MUL R9, R1, R13   // R13 = x * salt7
    
    // Right shifts
    LSR $27, R10      // R10 = (x * salt4) >> 27
    LSR $27, R11      // R11 = (x * salt5) >> 27
    LSR $27, R12      // R12 = (x * salt6) >> 27
    LSR $27, R13      // R13 = (x * salt7) >> 27
    
    // Load block values for last 4 checks
    MOVWU 16(R0), R14      // R14 = block[4]
    MOVWU 20(R0), R15      // R15 = block[5]
    MOVWU 24(R0), R16      // R16 = block[6]
    MOVWU 28(R0), R17      // R17 = block[7]
    
    // Create masks and test last 4 bits
    MOVW $1, R2
    LSLW R10, R2, R2   // R2 = 1 << ((x * salt4) >> 27)
    AND R2, R14        // Test bit in block[4]
    CBZ R14, not_found
    
    MOVW $1, R3
    LSLW R11, R3, R3   // R3 = 1 << ((x * salt5) >> 27)
    AND R3, R15        // Test bit in block[5]
    CBZ R15, not_found
    
    MOVW $1, R4
    LSLW R12, R4, R4   // R4 = 1 << ((x * salt6) >> 27)
    AND R4, R16        // Test bit in block[6]
    CBZ R16, not_found
    
    MOVW $1, R5
    LSLW R13, R5, R5   // R5 = 1 << ((x * salt7) >> 27)
    AND R5, R17        // Test bit in block[7]
    CBZ R17, not_found
    
    // All bits were set - return true
    MOVD $1, R6
    MOVB R6, ret+16(FP)
    RET

not_found:
    // At least one bit was not set - return false
    MOVD $0, R6
    MOVB R6, ret+16(FP)
    RET

