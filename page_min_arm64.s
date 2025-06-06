//go:build !purego

#include "textflag.h"

// func minInt32(data []int32) int32
TEXT ·minInt32(SB), NOSPLIT, $0-28
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return 0 if empty array
	CMP $0, R1
	BEQ empty32
	
	// Initialize min with first element
	MOVW (R0), R2  // min value
	MOVD $1, R3    // index counter
	
	// Check for 8-way unrolled processing
	CMP $8, R1
	BLT check_4way32
	
	// Calculate limit for 8-way unrolled loop
	SUB $7, R1, R4
	
unroll8_loop32:
	CMP R3, R4
	BGT check_4way32
	
	// Process 8 elements at once for maximum ILP
	MOVW (R0)(R3<<2), R5      // val1
	ADD $1, R3
	MOVW (R0)(R3<<2), R6      // val2
	ADD $1, R3
	MOVW (R0)(R3<<2), R7      // val3
	ADD $1, R3
	MOVW (R0)(R3<<2), R8      // val4
	ADD $1, R3
	MOVW (R0)(R3<<2), R9      // val5
	ADD $1, R3
	MOVW (R0)(R3<<2), R10     // val6
	ADD $1, R3
	MOVW (R0)(R3<<2), R11     // val7
	ADD $1, R3
	MOVW (R0)(R3<<2), R12     // val8
	ADD $1, R3
	
	// Update min with all 8 values
	CMP R5, R2
	CSEL LT, R5, R2, R2
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R7, R2
	CSEL LT, R7, R2, R2
	CMP R8, R2
	CSEL LT, R8, R2, R2
	CMP R9, R2
	CSEL LT, R9, R2, R2
	CMP R10, R2
	CSEL LT, R10, R2, R2
	CMP R11, R2
	CSEL LT, R11, R2, R2
	CMP R12, R2
	CSEL LT, R12, R2, R2
	
	B unroll8_loop32

check_4way32:
	// Check if we can do 4-way processing
	CMP $4, R1
	BLT scalar32
	
	SUB $3, R1, R4
	
unroll4_loop32:
	CMP R3, R4
	BGT scalar32
	
	// Process 4 elements at once
	MOVW (R0)(R3<<2), R5      // val1
	ADD $1, R3
	MOVW (R0)(R3<<2), R6      // val2
	ADD $1, R3
	MOVW (R0)(R3<<2), R7      // val3
	ADD $1, R3
	MOVW (R0)(R3<<2), R8      // val4
	ADD $1, R3
	
	// Update min with all 4 values
	CMP R5, R2
	CSEL LT, R5, R2, R2
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R7, R2
	CSEL LT, R7, R2, R2
	CMP R8, R2
	CSEL LT, R8, R2, R2
	
	B unroll4_loop32

scalar32:
	// Process remaining elements
	CMP R3, R1
	BEQ done32
	
	MOVW (R0)(R3<<2), R5
	CMP R5, R2
	CSEL LT, R5, R2, R2
	
	ADD $1, R3
	B scalar32

empty32:
	MOVD $0, R2

done32:
	MOVW R2, ret+24(FP)
	RET

// func minInt64(data []int64) int64
TEXT ·minInt64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return 0 if empty array
	CMP $0, R1
	BEQ empty64
	
	// Initialize min with first element
	MOVD (R0), R2  // min value
	MOVD $1, R3    // index counter
	
	// Check for 4-way unrolled processing
	CMP $4, R1
	BLT scalar64
	
	// Calculate limit for 4-way unrolled loop
	SUB $3, R1, R4
	
unroll4_loop64:
	CMP R3, R4
	BGT scalar64
	
	// Process 4 elements at once
	MOVD (R0)(R3<<3), R5      // val1
	ADD $1, R3
	MOVD (R0)(R3<<3), R6      // val2
	ADD $1, R3
	MOVD (R0)(R3<<3), R7      // val3
	ADD $1, R3
	MOVD (R0)(R3<<3), R8      // val4
	ADD $1, R3
	
	// Update min with all 4 values
	CMP R5, R2
	CSEL LT, R5, R2, R2
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R7, R2
	CSEL LT, R7, R2, R2
	CMP R8, R2
	CSEL LT, R8, R2, R2
	
	B unroll4_loop64

scalar64:
	// Process remaining elements
	CMP R3, R1
	BEQ done64
	
	MOVD (R0)(R3<<3), R5
	CMP R5, R2
	CSEL LT, R5, R2, R2
	
	ADD $1, R3
	B scalar64

empty64:
	MOVD $0, R2

done64:
	MOVD R2, ret+24(FP)
	RET

// func minUint32(data []uint32) uint32
TEXT ·minUint32(SB), NOSPLIT, $0-28
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return 0 if empty array
	CMP $0, R1
	BEQ empty_uint32
	
	// Initialize min with first element
	MOVW (R0), R2  // min value
	MOVD $1, R3    // index counter
	
	// Check for 8-way unrolled processing
	CMP $8, R1
	BLT check_4way_uint32
	
	// Calculate limit for 8-way unrolled loop
	SUB $7, R1, R4
	
unroll8_loop_uint32:
	CMP R3, R4
	BGT check_4way_uint32
	
	// Process 8 elements at once
	MOVW (R0)(R3<<2), R5
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	ADD $1, R3
	MOVW (R0)(R3<<2), R7
	ADD $1, R3
	MOVW (R0)(R3<<2), R8
	ADD $1, R3
	MOVW (R0)(R3<<2), R9
	ADD $1, R3
	MOVW (R0)(R3<<2), R10
	ADD $1, R3
	MOVW (R0)(R3<<2), R11
	ADD $1, R3
	MOVW (R0)(R3<<2), R12
	ADD $1, R3
	
	// Update min with all 8 values (unsigned comparison)
	CMPW R5, R2
	CSEL LO, R5, R2, R2
	CMPW R6, R2
	CSEL LO, R6, R2, R2
	CMPW R7, R2
	CSEL LO, R7, R2, R2
	CMPW R8, R2
	CSEL LO, R8, R2, R2
	CMPW R9, R2
	CSEL LO, R9, R2, R2
	CMPW R10, R2
	CSEL LO, R10, R2, R2
	CMPW R11, R2
	CSEL LO, R11, R2, R2
	CMPW R12, R2
	CSEL LO, R12, R2, R2
	
	B unroll8_loop_uint32

check_4way_uint32:
	CMP $4, R1
	BLT scalar_uint32
	
	SUB $3, R1, R4
	
unroll4_loop_uint32:
	CMP R3, R4
	BGT scalar_uint32
	
	// Process 4 elements at once
	MOVW (R0)(R3<<2), R5
	ADD $1, R3
	MOVW (R0)(R3<<2), R6
	ADD $1, R3
	MOVW (R0)(R3<<2), R7
	ADD $1, R3
	MOVW (R0)(R3<<2), R8
	ADD $1, R3
	
	// Update min with all 4 values (unsigned comparison)
	CMPW R5, R2
	CSEL LO, R5, R2, R2
	CMPW R6, R2
	CSEL LO, R6, R2, R2
	CMPW R7, R2
	CSEL LO, R7, R2, R2
	CMPW R8, R2
	CSEL LO, R8, R2, R2
	
	B unroll4_loop_uint32

scalar_uint32:
	// Process remaining elements
	CMP R3, R1
	BEQ done_uint32
	
	MOVW (R0)(R3<<2), R5
	CMPW R5, R2
	CSEL LO, R5, R2, R2
	
	ADD $1, R3
	B scalar_uint32

empty_uint32:
	MOVD $0, R2

done_uint32:
	MOVW R2, ret+24(FP)
	RET

// func minUint64(data []uint64) uint64
TEXT ·minUint64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return 0 if empty array
	CMP $0, R1
	BEQ empty_uint64
	
	// Initialize min with first element
	MOVD (R0), R2  // min value
	MOVD $1, R3    // index counter
	
	// Check for 4-way unrolled processing
	CMP $4, R1
	BLT scalar_uint64
	
	// Calculate limit for 4-way unrolled loop
	SUB $3, R1, R4
	
unroll4_loop_uint64:
	CMP R3, R4
	BGT scalar_uint64
	
	// Process 4 elements at once
	MOVD (R0)(R3<<3), R5
	ADD $1, R3
	MOVD (R0)(R3<<3), R6
	ADD $1, R3
	MOVD (R0)(R3<<3), R7
	ADD $1, R3
	MOVD (R0)(R3<<3), R8
	ADD $1, R3
	
	// Update min with all 4 values (unsigned comparison)
	CMP R5, R2
	CSEL LO, R5, R2, R2
	CMP R6, R2
	CSEL LO, R6, R2, R2
	CMP R7, R2
	CSEL LO, R7, R2, R2
	CMP R8, R2
	CSEL LO, R8, R2, R2
	
	B unroll4_loop_uint64

scalar_uint64:
	// Process remaining elements
	CMP R3, R1
	BEQ done_uint64
	
	MOVD (R0)(R3<<3), R5
	CMP R5, R2
	CSEL LO, R5, R2, R2
	
	ADD $1, R3
	B scalar_uint64

empty_uint64:
	MOVD $0, R2

done_uint64:
	MOVD R2, ret+24(FP)
	RET

// func minFloat32(data []float32) float32
TEXT ·minFloat32(SB), NOSPLIT, $0-28
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return 0 if empty array
	CMP $0, R1
	BEQ empty_float32
	
	// Initialize min with first element
	FMOVS (R0), F0  // min value
	MOVD $1, R2     // index counter
	
	// Check for 4-way unrolled processing
	CMP $4, R1
	BLT scalar_float32
	
	// Calculate limit for 4-way unrolled loop
	SUB $3, R1, R3
	
unroll4_loop_float32:
	CMP R2, R3
	BGT scalar_float32
	
	// Process 4 elements at once
	FMOVS (R0)(R2<<2), F1      // val1
	ADD $1, R2
	FMOVS (R0)(R2<<2), F2      // val2
	ADD $1, R2
	FMOVS (R0)(R2<<2), F3      // val3
	ADD $1, R2
	FMOVS (R0)(R2<<2), F4      // val4
	ADD $1, R2
	
	// Update min with all 4 values
	FCMPS F1, F0
	BPL no_min1_f32
	FMOVS F1, F0
no_min1_f32:
	FCMPS F2, F0
	BPL no_min2_f32
	FMOVS F2, F0
no_min2_f32:
	FCMPS F3, F0
	BPL no_min3_f32
	FMOVS F3, F0
no_min3_f32:
	FCMPS F4, F0
	BPL no_min4_f32
	FMOVS F4, F0
no_min4_f32:
	
	B unroll4_loop_float32

scalar_float32:
	// Process remaining elements
	CMP R2, R1
	BEQ done_float32
	
	FMOVS (R0)(R2<<2), F1
	FCMPS F1, F0
	BPL no_scalar_min_f32
	FMOVS F1, F0
no_scalar_min_f32:
	
	ADD $1, R2
	B scalar_float32

empty_float32:
	FMOVS $0.0, F0

done_float32:
	FMOVS F0, ret+24(FP)
	RET

// func minFloat64(data []float64) float64
TEXT ·minFloat64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return 0 if empty array
	CMP $0, R1
	BEQ empty_float64
	
	// Initialize min with first element
	FMOVD (R0), F0  // min value
	MOVD $1, R2     // index counter
	
	// Check for 4-way unrolled processing
	CMP $4, R1
	BLT scalar_float64
	
	// Calculate limit for 4-way unrolled loop
	SUB $3, R1, R3
	
unroll4_loop_float64:
	CMP R2, R3
	BGT scalar_float64
	
	// Process 4 elements at once
	FMOVD (R0)(R2<<3), F1      // val1
	ADD $1, R2
	FMOVD (R0)(R2<<3), F2      // val2
	ADD $1, R2
	FMOVD (R0)(R2<<3), F3      // val3
	ADD $1, R2
	FMOVD (R0)(R2<<3), F4      // val4
	ADD $1, R2
	
	// Update min with all 4 values
	FCMPD F1, F0
	BPL no_min1_f64
	FMOVD F1, F0
no_min1_f64:
	FCMPD F2, F0
	BPL no_min2_f64
	FMOVD F2, F0
no_min2_f64:
	FCMPD F3, F0
	BPL no_min3_f64
	FMOVD F3, F0
no_min3_f64:
	FCMPD F4, F0
	BPL no_min4_f64
	FMOVD F4, F0
no_min4_f64:
	
	B unroll4_loop_float64

scalar_float64:
	// Process remaining elements
	CMP R2, R1
	BEQ done_float64
	
	FMOVD (R0)(R2<<3), F1
	FCMPD F1, F0
	BPL no_scalar_min_f64
	FMOVD F1, F0
no_scalar_min_f64:
	
	ADD $1, R2
	B scalar_float64

empty_float64:
	FMOVD $0.0, F0

done_float64:
	FMOVD F0, ret+24(FP)
	RET

// func minBE128(data [][16]byte) []byte
TEXT ·minBE128(SB), NOSPLIT, $0-48
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1
	
	// Return null slice if empty
	CMP $0, R1
	BEQ null_slice
	
	// Initialize min pointer to first element
	MOVD R0, R2    // min_ptr = &data[0]
	MOVD $1, R3    // index counter
	
	// Process elements with 128-bit big-endian comparison
loop_be128:
	CMP R3, R1
	BEQ done_be128
	
	// Calculate current element pointer
	LSL $4, R3, R4      // offset = index * 16
	ADD R0, R4, R5      // current_ptr = base + offset
	
	// Compare high 8 bytes (big-endian comparison)
	MOVD (R5), R6       // current high
	MOVD (R2), R7       // min high
	REV R6, R6          // byte swap to compare as big-endian
	REV R7, R7          // byte swap to compare as big-endian
	CMP R6, R7
	BLT update_min      // if current < min, update
	BGT next_element    // if current > min, skip
	
	// High bytes are equal, compare low 8 bytes
	MOVD 8(R5), R6      // current low
	MOVD 8(R2), R7      // min low
	REV R6, R6          // byte swap to compare as big-endian
	REV R7, R7          // byte swap to compare as big-endian
	CMP R6, R7
	BHS next_element    // if current >= min, skip
	
update_min:
	MOVD R5, R2         // min_ptr = current_ptr

next_element:
	ADD $1, R3
	B loop_be128

done_be128:
	MOVD R2, ret_base+24(FP)
	MOVD $16, R4
	MOVD R4, ret_len+32(FP)
	MOVD R4, ret_cap+40(FP)
	RET

null_slice:
	MOVD $0, R2
	MOVD R2, ret_base+24(FP)
	MOVD R2, ret_len+32(FP)
	MOVD R2, ret_cap+40(FP)
	RET
