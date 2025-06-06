//go:build !purego

#include "textflag.h"

// ARM64 implementation for multiProbe32 using optimized scalar operations
// 
// table32Group structure (64 bytes):
// - keys: [7]uint32 (28 bytes)  
// - values: [7]uint32 (28 bytes)
// - bits: uint32 (4 bytes)
// - padding: uint32 (4 bytes)
//
// func multiProbe32NEON(table []table32Group, numKeys int, hashes []uintptr, keys sparse.Uint32Array, values []int32) int
TEXT ·multiProbe32NEON(SB), NOSPLIT, $0-112
	MOVD table_base+0(FP), R0
	MOVD table_len+8(FP), R1
	MOVD numKeys+24(FP), R2
	MOVD hashes_base+32(FP), R3
	MOVD hashes_len+40(FP), R4
	MOVD keys_array_ptr+56(FP), R5
	MOVD keys_array_off+72(FP), R14
	MOVD values_base+80(FP), R6
	SUB $1, R1   // modulo = len(table) - 1

	MOVD $0, R7  // index counter
	B test

loop:
	// Load hash and key
	MOVD (R3)(R7<<3), R8  // hash
	MOVW (R5), R9         // key

probe:
	// Calculate group index: hash & modulo
	AND R1, R8, R10
	LSL $6, R10  // multiply by 64 (sizeof(table32Group))
	ADD R0, R10, R11  // group pointer

	// Load bits mask from group
	MOVW 56(R11), R13  // group.bits

	// Check each key position manually
	// Check position 0
	AND $1, R13, R24
	CBZ R24, check_pos1
	MOVW (R11), R15      // Load key[0]
	CMP R9, R15
	BNE check_pos1
	MOVW 28(R11), R16    // group.values[0]
	B store_result

check_pos1:
	AND $2, R13, R24
	CBZ R24, check_pos2
	MOVW 4(R11), R15     // Load key[1]
	CMP R9, R15
	BNE check_pos2
	MOVW 32(R11), R16    // group.values[1]
	B store_result

check_pos2:
	AND $4, R13, R24
	CBZ R24, check_pos3
	MOVW 8(R11), R15     // Load key[2]
	CMP R9, R15
	BNE check_pos3
	MOVW 36(R11), R16    // group.values[2]
	B store_result

check_pos3:
	AND $8, R13, R24
	CBZ R24, check_pos4
	MOVW 12(R11), R15    // Load key[3]
	CMP R9, R15
	BNE check_pos4
	MOVW 40(R11), R16    // group.values[3]
	B store_result

check_pos4:
	AND $16, R13, R24
	CBZ R24, check_pos5
	MOVW 16(R11), R15    // Load key[4]
	CMP R9, R15
	BNE check_pos5
	MOVW 44(R11), R16    // group.values[4]
	B store_result

check_pos5:
	AND $32, R13, R24
	CBZ R24, check_pos6
	MOVW 20(R11), R15    // Load key[5]
	CMP R9, R15
	BNE check_pos6
	MOVW 48(R11), R16    // group.values[5]
	B store_result

check_pos6:
	AND $64, R13, R24
	CBZ R24, insert
	MOVW 24(R11), R15    // Load key[6]
	CMP R9, R15
	BNE insert
	MOVW 52(R11), R16    // group.values[6]
	B store_result

insert:
	// Check if group is full (bits == 127)
	CMP $127, R13
	BEQ probe_next_group

	// Find insertion position by counting set bits (population count)
	// Simple loop-based population count for up to 7 bits
	MOVW R13, R15
	MOVD $0, R16         // population count
popcount_loop:
	CBZ R15, popcount_done
	AND $1, R15, R24
	ADD R24, R16, R16    // Add 1 if bit is set
	LSR $1, R15
	B popcount_loop
popcount_done:
	
	// Insert new key and value
	LSL $1, R13          // Shift bits left
	ORR $1, R13          // Set lowest bit
	MOVW R13, 56(R11)    // Update group.bits

	// Store key at position R16
	LSL $2, R16, R24
	ADD R11, R24, R17 // Calculate key address
	MOVW R9, (R17)       // Store key

	// Store value at position R16  
	ADD $28, R17         // Move to values array
	MOVW R2, (R17)       // Store current numKeys as value
	MOVW R2, R16          // Return this value
	ADD $1, R2           // Increment numKeys
	B store_result

probe_next_group:
	ADD $1, R8           // hash++
	B probe

store_result:
	MOVW R16, (R6)(R7<<2)  // Store result in values array
	ADD $1, R7             // Increment index
	ADD R14, R5            // Advance key pointer by offset

test:
	CMP R7, R4             // Compare index with hashes_len
	BNE loop

	MOVD R2, ret+104(FP)   // Return numKeys
	RET

// ARM64 implementation for multiProbe64 using optimized scalar operations
//
// table64Group structure (64 bytes):
// - keys: [4]uint64 (32 bytes)
// - values: [4]uint32 (16 bytes)  
// - bits: uint32 (4 bytes)
// - padding: 12 bytes
//
// func multiProbe64NEON(table []table64Group, numKeys int, hashes []uintptr, keys sparse.Uint64Array, values []int32) int
TEXT ·multiProbe64NEON(SB), NOSPLIT, $0-112
	MOVD table_base+0(FP), R0
	MOVD table_len+8(FP), R1
	MOVD numKeys+24(FP), R2
	MOVD hashes_base+32(FP), R3
	MOVD hashes_len+40(FP), R4
	MOVD keys_array_ptr+56(FP), R5
	MOVD keys_array_off+72(FP), R14
	MOVD values_base+80(FP), R6
	SUB $1, R1   // modulo = len(table) - 1

	MOVD $0, R7  // index counter
	B test64

loop64:
	// Load hash and key
	MOVD (R3)(R7<<3), R8  // hash
	MOVD (R5), R9         // key

probe64:
	// Calculate group index: hash & modulo
	AND R1, R8, R10
	LSL $6, R10  // multiply by 64 (sizeof(table64Group))
	ADD R0, R10, R11  // group pointer

	// Load bits mask from group
	MOVW 48(R11), R13  // group.bits

	// Check each key position manually
	// Check position 0
	AND $1, R13, R24
	CBZ R24, check64_pos1
	MOVD (R11), R15      // Load key[0]
	CMP R9, R15
	BNE check64_pos1
	MOVW 32(R11), R16    // group.values[0]
	B store64_result

check64_pos1:
	AND $2, R13, R24
	CBZ R24, check64_pos2
	MOVD 8(R11), R15     // Load key[1]
	CMP R9, R15
	BNE check64_pos2
	MOVW 36(R11), R16    // group.values[1]
	B store64_result

check64_pos2:
	AND $4, R13, R24
	CBZ R24, check64_pos3
	MOVD 16(R11), R15    // Load key[2]
	CMP R9, R15
	BNE check64_pos3
	MOVW 40(R11), R16    // group.values[2]
	B store64_result

check64_pos3:
	AND $8, R13, R24
	CBZ R24, insert64
	MOVD 24(R11), R15    // Load key[3]
	CMP R9, R15
	BNE insert64
	MOVW 44(R11), R16    // group.values[3]
	B store64_result

insert64:
	// Check if group is full (bits == 15)
	CMP $15, R13
	BEQ probe64_next_group

	// Find insertion position by counting set bits (population count)
	// Simple loop-based population count for up to 7 bits
	MOVW R13, R15
	MOVD $0, R16         // population count
popcount_loop:
	CBZ R15, popcount_done
	AND $1, R15, R24
	ADD R24, R16, R16    // Add 1 if bit is set
	LSR $1, R15
	B popcount_loop
popcount_done:
	
	// Insert new key and value
	LSL $1, R13          // Shift bits left
	ORR $1, R13          // Set lowest bit
	MOVW R13, 48(R11)    // Update group.bits

	// Store key at position R16
	LSL $3, R16, R24
	ADD R11, R24, R17 // Calculate key address (8 bytes per key)
	MOVD R9, (R17)       // Store key

	// Store value at position R16  
	ADD $32, R11, R17    // Move to values array
	LSL $2, R16, R24
	ADD R17, R24, R17 // Calculate value address (4 bytes per value)
	MOVW R2, (R17)       // Store current numKeys as value
	MOVW R2, R16          // Return this value
	ADD $1, R2           // Increment numKeys
	B store64_result

probe64_next_group:
	ADD $1, R8           // hash++
	B probe64

store64_result:
	MOVW R16, (R6)(R7<<2)  // Store result in values array
	ADD $1, R7             // Increment index
	ADD R14, R5            // Advance key pointer by offset

test64:
	CMP R7, R4             // Compare index with hashes_len
	BNE loop64

	MOVD R2, ret+104(FP)   // Return numKeys
	RET

// ARM64 implementation for multiProbe128 using scalar operations
//
// table structure for 128-bit keys:
// - Linear probing with 16-byte keys stored directly
// - Each entry: 16 bytes key + metadata in separate table
//
// func multiProbe128NEON(table []byte, tableCap, tableLen int, hashes []uintptr, keys sparse.Uint128Array, values []int32) int
TEXT ·multiProbe128NEON(SB), NOSPLIT, $0-120
	MOVD table_base+0(FP), R0
	MOVD tableCap+24(FP), R1
	MOVD tableLen+32(FP), R2
	MOVD hashes_base+40(FP), R3
	MOVD hashes_len+48(FP), R4
	MOVD keys_array_ptr+64(FP), R5
	MOVD keys_array_off+80(FP), R14
	MOVD values_base+88(FP), R6

	// Calculate metadata table pointer
	MOVD R1, R10
	LSL $4, R10          // tableCap * 16 (16 bytes per key)
	ADD R0, R10, R10     // metadata table pointer
	SUB $1, R1           // modulo = tableCap - 1

	MOVD $0, R7          // index counter
	B test128

loop128:
	// Load hash and 128-bit key
	MOVD (R3)(R7<<3), R8 // hash
	// Load 128-bit key as two 64-bit halves
	MOVD (R5), R20       // key low 64 bits
	MOVD 8(R5), R21      // key high 64 bits

probe128:
	// Calculate slot index: hash & modulo
	AND R1, R8, R11

	// Check if slot is empty
	MOVW (R10)(R11<<2), R12  // Load metadata
	CBZ R12, insert128       // If 0, slot is empty

	// Compare 128-bit keys manually
	LSL $4, R11, R13         // Calculate key offset (16 bytes per key)
	ADD R0, R13, R15         // Address of stored key
	MOVD (R15), R16          // Load stored key low 64 bits
	MOVD 8(R15), R17         // Load stored key high 64 bits
	
	// Compare both halves
	CMP R20, R16
	BNE probe128_next
	CMP R21, R17
	BNE probe128_next

	// Found match
	SUB $1, R12          // Convert to 0-based index
	MOVW R12, (R6)(R7<<2)  // Store result
	B next128

probe128_next:
	ADD $1, R8           // hash++
	B probe128

insert128:
	// Insert new key
	ADD $1, R2           // Increment tableLen
	MOVW R2, (R10)(R11<<2)  // Store 1-based index in metadata
	LSL $4, R11, R13     // Calculate key offset
	ADD R0, R13, R15     // Address to store key
	MOVD R20, (R15)      // Store key low 64 bits
	MOVD R21, 8(R15)     // Store key high 64 bits
	SUB $1, R2, R12      // Set return value (0-based index)
	MOVW R12, (R6)(R7<<2)  // Store result

next128:
	ADD $1, R7           // Increment index
	ADD R14, R5          // Advance key pointer by offset

test128:
	CMP R7, R4           // Compare index with hashes_len
	BNE loop128

	MOVD R2, ret+112(FP) // Return tableLen
	RET
