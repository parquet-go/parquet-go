//go:build !purego

#include "textflag.h"

// func gather128(dst [][16]byte, src Uint128Array) int
TEXT ·gather128(SB), NOSPLIT, $0-56
	MOVD dst_base+0(FP), R0
	MOVD dst_len+8(FP), R1
	MOVD src_array_ptr+24(FP), R2
	MOVD src_array_len+32(FP), R3
	MOVD src_array_off+40(FP), R4
	MOVD $0, R5  // counter

	// n = min(len(dst), src.Len())
	MOVD R1, R6  // R6 = n (result count)
	CMP R3, R6   // Compare n with src_len
	BGT use_src_len
	B min_done
use_src_len:
	MOVD R3, R6  // R6 = src_len
min_done:

	// if n == 0, return 0
	CMP $0, R6
	BEQ done

	// if n == 1, handle single item
	CMP $1, R6
	BEQ tail

	// Main loop: process 2 items at a time
	// R7 = loop_end = (n / 2) * 2
	MOVD R6, R7
	LSR $1, R7
	LSL $1, R7

loop:
	// Load two 16-byte values
	VLD1 (R2), [V0.B16]
	ADD R4, R2, R8
	VLD1 (R8), [V1.B16]

	// Store to dst
	VST1 [V0.B16], (R0)
	ADD $16, R0
	VST1 [V1.B16], (R0)
	ADD $16, R0

	// Advance src pointer by 2 * offset
	LSL $1, R4, R8  // R8 = 2 * offset
	ADD R8, R2
	ADD $2, R5
	CMP R7, R5
	BLT loop

	// Check if we need to handle the last item
	CMP R6, R5
	BEQ done

tail:
	// Handle last item
	VLD1 (R2), [V0.B16]
	VST1 [V0.B16], (R0)

done:
	MOVD R6, ret+48(FP)
	RET
