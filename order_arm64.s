//go:build !purego

#include "textflag.h"

#define UNDEFINED 0
#define ASCENDING 1
#define DESCENDING -1

// func orderOfInt32(data []int32) int
TEXT ·orderOfInt32(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return UNDEFINED if length < 2
	CMP $2, R1
	BLT undefined
	
	// Initialize counters for ascending and descending checks
	MOVD $1, R2     // ascending index
	MOVD $1, R3     // descending index
	SUB $1, R1, R4  // last_index = len - 1
	
	// Check for 8-way unrolled ascending order first
	CMP $8, R1
	BLT check_4way_asc32
	
	SUB $7, R4, R5  // limit for 8-way processing
	
unroll8_asc32:
	CMP R2, R5
	BGT check_4way_asc32
	
	// Load 8 consecutive pairs and check ascending order
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	B unroll8_asc32

check_4way_asc32:
	// Check if we can do 4-way processing
	CMP $4, R4
	BLT scalar_asc32
	
	SUB $3, R4, R5
	
unroll4_asc32:
	CMP R2, R5
	BGT scalar_asc32
	
	// Process 4 elements
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	B unroll4_asc32

scalar_asc32:
	// Process remaining elements for ascending check
	CMP R2, R4
	BGT ascending32
	
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending32
	
	ADD $1, R2
	B scalar_asc32

ascending32:
	MOVD $ASCENDING, R8
	MOVD R8, ret+24(FP)
	RET

not_ascending32:
	// Reset for descending check with 8-way unrolling
	MOVD $1, R3
	CMP $8, R1
	BLT check_4way_desc32
	
	SUB $7, R4, R5
	
unroll8_desc32:
	CMP R3, R5
	BGT check_4way_desc32
	
	// Load 8 consecutive pairs and check descending order
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	B unroll8_desc32

check_4way_desc32:
	CMP $4, R4
	BLT scalar_desc32
	
	SUB $3, R4, R5
	
unroll4_desc32:
	CMP R3, R5
	BGT scalar_desc32
	
	// Process 4 elements for descending
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	B unroll4_desc32

scalar_desc32:
	// Process remaining elements for descending check
	CMP R3, R4
	BGT descending32
	
	MOVW (R0)(R3<<2), R6        // data[i]
	MOVW -4(R0)(R3<<2), R7      // data[i-1]
	CMP R6, R7
	BLT undefined
	
	ADD $1, R3
	B scalar_desc32

descending32:
	MOVD $DESCENDING, R8
	MOVD R8, ret+24(FP)
	RET

undefined32:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET

// Global undefined label that can be used by all functions
undefined:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET

// func orderOfInt64(data []int64) int
TEXT ·orderOfInt64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return UNDEFINED if length < 2
	CMP $2, R1
	BLT undefined64
	
	// Initialize counters
	MOVD $1, R2     // ascending index
	MOVD $1, R3     // descending index
	SUB $1, R1, R4  // last_index = len - 1
	
	// Check for 4-way unrolled ascending order first
	CMP $4, R1
	BLT scalar_asc64
	
	SUB $3, R4, R5  // limit for 4-way processing
	
unroll4_asc64:
	CMP R2, R5
	BGT scalar_asc64
	
	// Load 4 consecutive pairs and check ascending order
	MOVD (R0)(R2<<3), R6        // data[i]
	MOVD -8(R0)(R2<<3), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending64
	
	ADD $1, R2
	MOVD (R0)(R2<<3), R6        // data[i]
	MOVD -8(R0)(R2<<3), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending64
	
	ADD $1, R2
	MOVD (R0)(R2<<3), R6        // data[i]
	MOVD -8(R0)(R2<<3), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending64
	
	ADD $1, R2
	MOVD (R0)(R2<<3), R6        // data[i]
	MOVD -8(R0)(R2<<3), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending64
	
	ADD $1, R2
	B unroll4_asc64

scalar_asc64:
	// Process remaining elements for ascending check
	CMP R2, R4
	BGT ascending64
	
	MOVD (R0)(R2<<3), R6        // data[i]
	MOVD -8(R0)(R2<<3), R7      // data[i-1]
	CMP R7, R6
	BLT not_ascending64
	
	ADD $1, R2
	B scalar_asc64

ascending64:
	MOVD $ASCENDING, R8
	MOVD R8, ret+24(FP)
	RET

not_ascending64:
	// Reset for descending check with 4-way unrolling
	MOVD $1, R3
	CMP $4, R1
	BLT scalar_desc64
	
	SUB $3, R4, R5
	
unroll4_desc64:
	CMP R3, R5
	BGT scalar_desc64
	
	// Load 4 consecutive pairs and check descending order
	MOVD (R0)(R3<<3), R6        // data[i]
	MOVD -8(R0)(R3<<3), R7      // data[i-1]
	CMP R6, R7
	BLT undefined64
	
	ADD $1, R3
	MOVD (R0)(R3<<3), R6        // data[i]
	MOVD -8(R0)(R3<<3), R7      // data[i-1]
	CMP R6, R7
	BLT undefined64
	
	ADD $1, R3
	MOVD (R0)(R3<<3), R6        // data[i]
	MOVD -8(R0)(R3<<3), R7      // data[i-1]
	CMP R6, R7
	BLT undefined64
	
	ADD $1, R3
	MOVD (R0)(R3<<3), R6        // data[i]
	MOVD -8(R0)(R3<<3), R7      // data[i-1]
	CMP R6, R7
	BLT undefined64
	
	ADD $1, R3
	B unroll4_desc64

scalar_desc64:
	// Process remaining elements for descending check
	CMP R3, R4
	BGT descending64
	
	MOVD (R0)(R3<<3), R6        // data[i]
	MOVD -8(R0)(R3<<3), R7      // data[i-1]
	CMP R6, R7
	BLT undefined64
	
	ADD $1, R3
	B scalar_desc64

descending64:
	MOVD $DESCENDING, R8
	MOVD R8, ret+24(FP)
	RET

undefined64:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET

// func orderOfUint32(data []uint32) int
TEXT ·orderOfUint32(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return UNDEFINED if length < 2
	CMP $2, R1
	BLT undefined_uint32
	
	// Initialize counters
	MOVD $1, R2     // ascending index
	MOVD $1, R3     // descending index
	SUB $1, R1, R4  // last_index = len - 1
	
	// Check for 8-way unrolled ascending order first
	CMP $8, R1
	BLT check_4way_asc_uint32
	
	SUB $7, R4, R5  // limit for 8-way processing
	
unroll8_asc_uint32:
	CMP R2, R5
	BGT check_4way_asc_uint32
	
	// Load 8 consecutive pairs and check ascending order (unsigned)
	MOVW (R0)(R2<<2), R6        // data[i]
	MOVW -4(R0)(R2<<2), R7      // data[i-1]
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	B unroll8_asc_uint32

check_4way_asc_uint32:
	CMP $4, R4
	BLT scalar_asc_uint32
	
	SUB $3, R4, R5
	
unroll4_asc_uint32:
	CMP R2, R5
	BGT scalar_asc_uint32
	
	// Process 4 elements
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	B unroll4_asc_uint32

scalar_asc_uint32:
	// Process remaining elements for ascending check
	CMP R2, R4
	BGT ascending_uint32
	
	MOVW (R0)(R2<<2), R6
	MOVW -4(R0)(R2<<2), R7
	CMPW R7, R6
	BLO not_ascending_uint32
	
	ADD $1, R2
	B scalar_asc_uint32

ascending_uint32:
	MOVD $ASCENDING, R8
	MOVD R8, ret+24(FP)
	RET

not_ascending_uint32:
	// Reset for descending check
	MOVD $1, R3
	CMP $8, R1
	BLT check_4way_desc_uint32
	
	SUB $7, R4, R5
	
unroll8_desc_uint32:
	CMP R3, R5
	BGT check_4way_desc_uint32
	
	// Load 8 consecutive pairs and check descending order (unsigned)
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	B unroll8_desc_uint32

check_4way_desc_uint32:
	CMP $4, R4
	BLT scalar_desc_uint32
	
	SUB $3, R4, R5
	
unroll4_desc_uint32:
	CMP R3, R5
	BGT scalar_desc_uint32
	
	// Process 4 elements for descending
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	B unroll4_desc_uint32

scalar_desc_uint32:
	// Process remaining elements for descending check
	CMP R3, R4
	BGT descending_uint32
	
	MOVW (R0)(R3<<2), R6
	MOVW -4(R0)(R3<<2), R7
	CMPW R6, R7
	BLO undefined_uint32
	
	ADD $1, R3
	B scalar_desc_uint32

descending_uint32:
	MOVD $DESCENDING, R8
	MOVD R8, ret+24(FP)
	RET

undefined_uint32:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET

// func orderOfUint64(data []uint64) int
TEXT ·orderOfUint64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return UNDEFINED if length < 2
	CMP $2, R1
	BLT undefined_uint64
	
	// Initialize counters
	MOVD $1, R2     // ascending index
	MOVD $1, R3     // descending index
	SUB $1, R1, R4  // last_index = len - 1
	
	// Check for 4-way unrolled ascending order first
	CMP $4, R1
	BLT scalar_asc_uint64
	
	SUB $3, R4, R5  // limit for 4-way processing
	
unroll4_asc_uint64:
	CMP R2, R5
	BGT scalar_asc_uint64
	
	// Load 4 consecutive pairs and check ascending order (unsigned)
	MOVD (R0)(R2<<3), R6
	MOVD -8(R0)(R2<<3), R7
	CMP R7, R6
	BLO not_ascending_uint64
	
	ADD $1, R2
	MOVD (R0)(R2<<3), R6
	MOVD -8(R0)(R2<<3), R7
	CMP R7, R6
	BLO not_ascending_uint64
	
	ADD $1, R2
	MOVD (R0)(R2<<3), R6
	MOVD -8(R0)(R2<<3), R7
	CMP R7, R6
	BLO not_ascending_uint64
	
	ADD $1, R2
	MOVD (R0)(R2<<3), R6
	MOVD -8(R0)(R2<<3), R7
	CMP R7, R6
	BLO not_ascending_uint64
	
	ADD $1, R2
	B unroll4_asc_uint64

scalar_asc_uint64:
	// Process remaining elements for ascending check
	CMP R2, R4
	BGT ascending_uint64
	
	MOVD (R0)(R2<<3), R6
	MOVD -8(R0)(R2<<3), R7
	CMP R7, R6
	BLO not_ascending_uint64
	
	ADD $1, R2
	B scalar_asc_uint64

ascending_uint64:
	MOVD $ASCENDING, R8
	MOVD R8, ret+24(FP)
	RET

not_ascending_uint64:
	// Reset for descending check
	MOVD $1, R3
	CMP $4, R1
	BLT scalar_desc_uint64
	
	SUB $3, R4, R5
	
unroll4_desc_uint64:
	CMP R3, R5
	BGT scalar_desc_uint64
	
	// Load 4 consecutive pairs and check descending order (unsigned)
	MOVD (R0)(R3<<3), R6
	MOVD -8(R0)(R3<<3), R7
	CMP R6, R7
	BLO undefined_uint64
	
	ADD $1, R3
	MOVD (R0)(R3<<3), R6
	MOVD -8(R0)(R3<<3), R7
	CMP R6, R7
	BLO undefined_uint64
	
	ADD $1, R3
	MOVD (R0)(R3<<3), R6
	MOVD -8(R0)(R3<<3), R7
	CMP R6, R7
	BLO undefined_uint64
	
	ADD $1, R3
	MOVD (R0)(R3<<3), R6
	MOVD -8(R0)(R3<<3), R7
	CMP R6, R7
	BLO undefined_uint64
	
	ADD $1, R3
	B unroll4_desc_uint64

scalar_desc_uint64:
	// Process remaining elements for descending check
	CMP R3, R4
	BGT descending_uint64
	
	MOVD (R0)(R3<<3), R6
	MOVD -8(R0)(R3<<3), R7
	CMP R6, R7
	BLO undefined_uint64
	
	ADD $1, R3
	B scalar_desc_uint64

descending_uint64:
	MOVD $DESCENDING, R8
	MOVD R8, ret+24(FP)
	RET

undefined_uint64:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET

// func orderOfFloat32(data []float32) int
TEXT ·orderOfFloat32(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return UNDEFINED if length < 2
	CMP $2, R1
	BLT undefined_float32
	
	// Initialize counters
	MOVD $1, R2     // ascending index
	MOVD $1, R3     // descending index
	SUB $1, R1, R4  // last_index = len - 1
	
	// Check for 4-way unrolled ascending order first
	CMP $4, R1
	BLT scalar_asc_float32
	
	SUB $3, R4, R5  // limit for 4-way processing
	
unroll4_asc_float32:
	CMP R2, R5
	BGT scalar_asc_float32
	
	// Load 4 consecutive pairs and check ascending order
	FMOVS (R0)(R2<<2), F0      // data[i]
	FMOVS -4(R0)(R2<<2), F1    // data[i-1]
	FCMPS F1, F0
	BLT not_ascending_float32
	
	ADD $1, R2
	FMOVS (R0)(R2<<2), F0
	FMOVS -4(R0)(R2<<2), F1
	FCMPS F1, F0
	BLT not_ascending_float32
	
	ADD $1, R2
	FMOVS (R0)(R2<<2), F0
	FMOVS -4(R0)(R2<<2), F1
	FCMPS F1, F0
	BLT not_ascending_float32
	
	ADD $1, R2
	FMOVS (R0)(R2<<2), F0
	FMOVS -4(R0)(R2<<2), F1
	FCMPS F1, F0
	BLT not_ascending_float32
	
	ADD $1, R2
	B unroll4_asc_float32

scalar_asc_float32:
	// Process remaining elements for ascending check
	CMP R2, R4
	BGT ascending_float32
	
	FMOVS (R0)(R2<<2), F0
	FMOVS -4(R0)(R2<<2), F1
	FCMPS F1, F0
	BLT not_ascending_float32
	
	ADD $1, R2
	B scalar_asc_float32

ascending_float32:
	MOVD $ASCENDING, R8
	MOVD R8, ret+24(FP)
	RET

not_ascending_float32:
	// Reset for descending check
	MOVD $1, R3
	CMP $4, R1
	BLT scalar_desc_float32
	
	SUB $3, R4, R5
	
unroll4_desc_float32:
	CMP R3, R5
	BGT scalar_desc_float32
	
	// Load 4 consecutive pairs and check descending order
	FMOVS (R0)(R3<<2), F0
	FMOVS -4(R0)(R3<<2), F1
	FCMPS F0, F1
	BLT undefined_float32
	
	ADD $1, R3
	FMOVS (R0)(R3<<2), F0
	FMOVS -4(R0)(R3<<2), F1
	FCMPS F0, F1
	BLT undefined_float32
	
	ADD $1, R3
	FMOVS (R0)(R3<<2), F0
	FMOVS -4(R0)(R3<<2), F1
	FCMPS F0, F1
	BLT undefined_float32
	
	ADD $1, R3
	FMOVS (R0)(R3<<2), F0
	FMOVS -4(R0)(R3<<2), F1
	FCMPS F0, F1
	BLT undefined_float32
	
	ADD $1, R3
	B unroll4_desc_float32

scalar_desc_float32:
	// Process remaining elements for descending check
	CMP R3, R4
	BGT descending_float32
	
	FMOVS (R0)(R3<<2), F0
	FMOVS -4(R0)(R3<<2), F1
	FCMPS F0, F1
	BLT undefined_float32
	
	ADD $1, R3
	B scalar_desc_float32

descending_float32:
	MOVD $DESCENDING, R8
	MOVD R8, ret+24(FP)
	RET

undefined_float32:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET

// func orderOfFloat64(data []float64) int
TEXT ·orderOfFloat64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return UNDEFINED if length < 2
	CMP $2, R1
	BLT undefined_float64
	
	// Initialize counters
	MOVD $1, R2     // ascending index
	MOVD $1, R3     // descending index
	SUB $1, R1, R4  // last_index = len - 1
	
	// Check for 4-way unrolled ascending order first
	CMP $4, R1
	BLT scalar_asc_float64
	
	SUB $3, R4, R5  // limit for 4-way processing
	
unroll4_asc_float64:
	CMP R2, R5
	BGT scalar_asc_float64
	
	// Load 4 consecutive pairs and check ascending order
	FMOVD (R0)(R2<<3), F0      // data[i]
	FMOVD -8(R0)(R2<<3), F1    // data[i-1]
	FCMPD F1, F0
	BLT not_ascending_float64
	
	ADD $1, R2
	FMOVD (R0)(R2<<3), F0
	FMOVD -8(R0)(R2<<3), F1
	FCMPD F1, F0
	BLT not_ascending_float64
	
	ADD $1, R2
	FMOVD (R0)(R2<<3), F0
	FMOVD -8(R0)(R2<<3), F1
	FCMPD F1, F0
	BLT not_ascending_float64
	
	ADD $1, R2
	FMOVD (R0)(R2<<3), F0
	FMOVD -8(R0)(R2<<3), F1
	FCMPD F1, F0
	BLT not_ascending_float64
	
	ADD $1, R2
	B unroll4_asc_float64

scalar_asc_float64:
	// Process remaining elements for ascending check
	CMP R2, R4
	BGT ascending_float64
	
	FMOVD (R0)(R2<<3), F0
	FMOVD -8(R0)(R2<<3), F1
	FCMPD F1, F0
	BLT not_ascending_float64
	
	ADD $1, R2
	B scalar_asc_float64

ascending_float64:
	MOVD $ASCENDING, R8
	MOVD R8, ret+24(FP)
	RET

not_ascending_float64:
	// Reset for descending check
	MOVD $1, R3
	CMP $4, R1
	BLT scalar_desc_float64
	
	SUB $3, R4, R5
	
unroll4_desc_float64:
	CMP R3, R5
	BGT scalar_desc_float64
	
	// Load 4 consecutive pairs and check descending order
	FMOVD (R0)(R3<<3), F0
	FMOVD -8(R0)(R3<<3), F1
	FCMPD F0, F1
	BLT undefined_float64
	
	ADD $1, R3
	FMOVD (R0)(R3<<3), F0
	FMOVD -8(R0)(R3<<3), F1
	FCMPD F0, F1
	BLT undefined_float64
	
	ADD $1, R3
	FMOVD (R0)(R3<<3), F0
	FMOVD -8(R0)(R3<<3), F1
	FCMPD F0, F1
	BLT undefined_float64
	
	ADD $1, R3
	FMOVD (R0)(R3<<3), F0
	FMOVD -8(R0)(R3<<3), F1
	FCMPD F0, F1
	BLT undefined_float64
	
	ADD $1, R3
	B unroll4_desc_float64

scalar_desc_float64:
	// Process remaining elements for descending check
	CMP R3, R4
	BGT descending_float64
	
	FMOVD (R0)(R3<<3), F0
	FMOVD -8(R0)(R3<<3), F1
	FCMPD F0, F1
	BLT undefined_float64
	
	ADD $1, R3
	B scalar_desc_float64

descending_float64:
	MOVD $DESCENDING, R8
	MOVD R8, ret+24(FP)
	RET

undefined_float64:
	MOVD $UNDEFINED, R8
	MOVD R8, ret+24(FP)
	RET
