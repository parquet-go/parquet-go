//go:build !purego

#include "textflag.h"

#define sizeOfValue 24

// ARM64 NEON implementation for Value memset
// Since NEON vectors are 16 bytes, we'll use a different strategy than AMD64
// We'll process 2 Values at a time (48 bytes = 3 x 16-byte vectors)
//
// func memsetValuesNEON(values []Value, model Value, _ uint64)
TEXT ·memsetValuesNEON(SB), NOSPLIT, $0-56
	MOVD values_base+0(FP), R0
	MOVD values_len+8(FP), R1

	// Load model Value (24 bytes) into registers  
	MOVD model_ptr+24(FP), R2
	MOVD model_u64+32(FP), R3
	MOVD model+40(FP), R4

	MOVD $0, R5 // index counter
	MOVD R1, R6 // total count
	MOVD R1, R7 // byte count
	MOVD $sizeOfValue, R8
	MUL R8, R7  // total bytes = len * 24

	// Check if we have enough elements for NEON optimization (2 elements = 48 bytes)
	CMP $2, R1
	BLT scalar_loop

	// Load model into NEON vectors (24 bytes = 1.5 vectors)
	// We'll replicate the model to fill multiple vectors
	VMOV R2, V0.D[0]  // ptr field (8 bytes)
	VMOV R3, V0.D[1]  // u64 field (8 bytes)
	VMOV R4, V1.D[0]  // remaining 8 bytes (kind, levels, columnIndex)
	VMOV R4, V1.D[1]  // duplicate for second Value

	VMOV R2, V2.D[0]  // ptr field for second value
	VMOV R3, V2.D[1]  // u64 field for second value

	// Calculate how many 2-Value chunks we can process
	LSR $1, R1, R9   // R9 = R1 / 2 (number of 2-element chunks)
	LSL $1, R9, R10  // R10 = R9 * 2 (round down to even count)

neon_loop:
	CMP R5, R10
	BEQ remainder

	// Calculate byte offset for first value
	MOVD $sizeOfValue, R11
	MUL R5, R11      // R11 = index * 24
	ADD R0, R11, R12 // R12 = base + offset

	// Store first Value (24 bytes = 16 + 8)
	VST1 [V0.B16], (R12)          // Store first 16 bytes
	MOVD R4, 16(R12)              // Store last 8 bytes

	// Store second Value (24 bytes = 16 + 8)
	ADD $sizeOfValue, R12, R13    // Advance to second Value
	VST1 [V0.B16], (R13)          // Store first 16 bytes  
	MOVD R4, 16(R13)              // Store last 8 bytes

	ADD $2, R5
	B neon_loop

remainder:
	// Handle remaining elements (if count was odd)
	CMP R5, R1
	BEQ done

scalar_loop:
	// Calculate byte offset for current element
	MOVD $sizeOfValue, R11
	MUL R5, R11
	ADD R0, R11, R12        // R12 = base + offset

	// Store 3 x 8-byte words  
	MOVD R2, (R12)          // ptr field
	MOVD R3, 8(R12)         // u64 field
	MOVD R4, 16(R12)        // remaining fields

	ADD $1, R5
	CMP R5, R1
	BNE scalar_loop

done:
	RET
