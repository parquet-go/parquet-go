//go:build !purego

#include "textflag.h"

// func broadcastNEON(dst []byte, src byte)
TEXT ·broadcastNEON(SB), NOSPLIT, $0-25
	MOVD dst_base+0(FP), R0
	MOVD dst_len+8(FP), R1
	MOVBU src+24(FP), R2

	CMP $8, R1
	BLE test

	CMP $64, R1
	BLT init8

	// Prepare for 64-byte NEON loop
	MOVD R1, R3
	ANDW $~63, R3
	VMOV R2, V0.B[0]
	VDUP V0.B[0], V0.B16
	MOVD $0, R4

loop64:
	VST1 [V0.B16], (R0)
	ADD $16, R0
	VST1 [V0.B16], (R0)
	ADD $16, R0
	VST1 [V0.B16], (R0)
	ADD $16, R0
	VST1 [V0.B16], (R0)
	ADD $16, R0
	ADD $64, R4
	CMP R3, R4
	BLT loop64

	// Reset R0 and handle remainder with overlapping stores
	SUB R4, R0
	ADD R1, R0, R5
	SUB $64, R5
	VST1 [V0.B16], (R5)
	ADD $16, R5
	VST1 [V0.B16], (R5)
	ADD $16, R5
	VST1 [V0.B16], (R5)
	ADD $16, R5
	VST1 [V0.B16], (R5)
	RET

init8:
	// For 8-63 bytes, use 8-byte stores
	MOVD $0x0101010101010101, R3
	MUL R3, R2

loop8:
	SUB $8, R1
	MOVD R2, (R0)(R1)
	CMP $8, R1
	BHS loop8
	MOVD R2, (R0)
	RET

loop:
	SUB $1, R1
	MOVBU R2, (R0)(R1)
test:
	CMP $0, R1
	BNE loop
	RET
