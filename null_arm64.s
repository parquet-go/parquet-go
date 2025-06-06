//go:build !purego

#include "textflag.h"

// func nullIndex8ARM64(bits *uint64, rows sparse.Array)
TEXT ·nullIndex8ARM64(SB), NOSPLIT, $0-32
	MOVD bits+0(FP), R0
	MOVD rows_array_ptr+8(FP), R1
	MOVD rows_array_len+16(FP), R2
	MOVD rows_array_off+24(FP), R3

	MOVD $0, R4  // index counter
	CMP $0, R2
	BEQ done

loop8:
	// Load byte value using sparse array indexing
	MOVBU (R1), R5
	CMP $0, R5
	BEQ next8

	// Set bit in bitmap
	// bit_index = R4, word_index = R4 >> 6, bit_offset = R4 & 63
	AND $63, R4, R6     // bit offset within word
	LSR $6, R4, R7      // word index
	MOVD $1, R8
	LSL R6, R8, R8      // create bit mask
	LSL $3, R7, R9      // word offset in bytes
	MOVD (R0)(R9), R10  // load current word
	ORR R8, R10, R10    // set bit
	MOVD R10, (R0)(R9)  // store back

next8:
	ADD R3, R1          // advance pointer by offset
	ADD $1, R4          // increment index
	CMP R4, R2
	BNE loop8

done:
	RET

// func nullIndex32ARM64(bits *uint64, rows sparse.Array)
TEXT ·nullIndex32ARM64(SB), NOSPLIT, $0-32
	MOVD bits+0(FP), R0
	MOVD rows_array_ptr+8(FP), R1
	MOVD rows_array_len+16(FP), R2
	MOVD rows_array_off+24(FP), R3

	MOVD $0, R4  // index counter
	CMP $0, R2
	BEQ done32

	// Check if we can do NEON 4-way processing
	CMP $4, R2
	BLT scalar32

	// NEON optimization for 4x int32 values at a time
	SUB $3, R2, R5  // loop limit (process 4 at a time)

neon_loop32:
	CMP R4, R5
	BGT scalar32

	// Manual gather: load 4 int32 values from sparse array
	MOVW (R1), R6              // first value
	ADD R3, R1, R7
	MOVW (R7), R8              // second value
	ADD R3, R7, R7
	MOVW (R7), R9              // third value
	ADD R3, R7, R7
	MOVW (R7), R10             // fourth value

	// Move values to NEON vector
	VMOV R6, V0.S[0]
	VMOV R8, V0.S[1]
	VMOV R9, V0.S[2]
	VMOV R10, V0.S[3]

	// Compare with zero vector
	VEOR V1.B16, V1.B16, V1.B16  // create zero vector
	VCMEQ V1.S4, V0.S4, V2.S4    // compare equal to zero

	// Extract comparison results to scalar registers
	VMOV V2.S[0], R6
	VMOV V2.S[1], R8
	VMOV V2.S[2], R9
	VMOV V2.S[3], R10

	// Process each bit (1 means non-null, 0 means null)
	// For each index i, if value != 0, set bit i in bitmap

	// Process first value (index R4)
	CMP $0, R6
	BEQ skip1
	AND $63, R4, R11
	LSR $6, R4, R12
	MOVD $1, R13
	LSL R11, R13, R13
	LSL $3, R12, R14
	MOVD (R0)(R14), R15
	ORR R13, R15, R15
	MOVD R15, (R0)(R14)
skip1:

	// Process second value (index R4+1)
	ADD $1, R4, R16
	CMP $0, R8
	BEQ skip2
	AND $63, R16, R11
	LSR $6, R16, R12
	MOVD $1, R13
	LSL R11, R13, R13
	LSL $3, R12, R14
	MOVD (R0)(R14), R15
	ORR R13, R15, R15
	MOVD R15, (R0)(R14)
skip2:

	// Process third value (index R4+2)
	ADD $2, R4, R16
	CMP $0, R9
	BEQ skip3
	AND $63, R16, R11
	LSR $6, R16, R12
	MOVD $1, R13
	LSL R11, R13, R13
	LSL $3, R12, R14
	MOVD (R0)(R14), R15
	ORR R13, R15, R15
	MOVD R15, (R0)(R14)
skip3:

	// Process fourth value (index R4+3)
	ADD $3, R4, R16
	CMP $0, R10
	BEQ skip4
	AND $63, R16, R11
	LSR $6, R16, R12
	MOVD $1, R13
	LSL R11, R13, R13
	LSL $3, R12, R14
	MOVD (R0)(R14), R15
	ORR R13, R15, R15
	MOVD R15, (R0)(R14)
skip4:

	// Advance pointers and counters
	LSL $2, R3, R17     // offset * 4
	ADD R17, R1         // advance pointer by 4 * offset
	ADD $4, R4          // advance index by 4
	B neon_loop32

scalar32:
	CMP R4, R2
	BEQ done32

	// Load int32 value
	MOVW (R1), R5
	CMP $0, R5
	BEQ next32

	// Set bit in bitmap
	AND $63, R4, R6
	LSR $6, R4, R7
	MOVD $1, R8
	LSL R6, R8, R8
	LSL $3, R7, R9
	MOVD (R0)(R9), R10
	ORR R8, R10, R10
	MOVD R10, (R0)(R9)

next32:
	ADD R3, R1
	ADD $1, R4
	CMP R4, R2
	BNE scalar32

done32:
	RET

// func nullIndex64ARM64(bits *uint64, rows sparse.Array)
TEXT ·nullIndex64ARM64(SB), NOSPLIT, $0-32
	MOVD bits+0(FP), R0
	MOVD rows_array_ptr+8(FP), R1
	MOVD rows_array_len+16(FP), R2
	MOVD rows_array_off+24(FP), R3

	MOVD $0, R4  // index counter
	CMP $0, R2
	BEQ done64

loop64:
	// Load int64 value
	MOVD (R1), R5
	CMP $0, R5
	BEQ next64

	// Set bit in bitmap
	AND $63, R4, R6
	LSR $6, R4, R7
	MOVD $1, R8
	LSL R6, R8, R8
	LSL $3, R7, R9
	MOVD (R0)(R9), R10
	ORR R8, R10, R10
	MOVD R10, (R0)(R9)

next64:
	ADD R3, R1
	ADD $1, R4
	CMP R4, R2
	BNE loop64

done64:
	RET

// func nullIndex128ARM64(bits *uint64, rows sparse.Array)
TEXT ·nullIndex128ARM64(SB), NOSPLIT, $0-32
	MOVD bits+0(FP), R0
	MOVD rows_array_ptr+8(FP), R1
	MOVD rows_array_len+16(FP), R2
	MOVD rows_array_off+24(FP), R3

	MOVD $0, R4  // index counter
	CMP $0, R2
	BEQ done128

loop128:
	// Load 16-byte value as two 64-bit words
	MOVD (R1), R5
	MOVD 8(R1), R6
	
	// Check if either word is non-zero
	ORR R5, R6, R7
	CMP $0, R7
	BEQ next128

	// Set bit in bitmap
	AND $63, R4, R8
	LSR $6, R4, R9
	MOVD $1, R10
	LSL R8, R10, R10
	LSL $3, R9, R11
	MOVD (R0)(R11), R12
	ORR R10, R12, R12
	MOVD R12, (R0)(R11)

next128:
	ADD R3, R1
	ADD $1, R4
	CMP R4, R2
	BNE loop128

done128:
	RET
