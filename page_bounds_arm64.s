//go:build !purego

#include "textflag.h"

// func combinedBoundsInt32ARM64(data []int32) (min, max int32)
TEXT ·combinedBoundsInt32ARM64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1

	// Return zeros if empty array
	CMP $0, R1
	BEQ empty32

	// Initialize min/max with first element
	MOVW (R0), R2  // min
	MOVW (R0), R3  // max
	MOVD $1, R4    // index counter

	// Optimize with 8-way unrolled loop for maximum performance
	CMP $8, R1
	BLT check_4way32

	// Calculate limit for 8-way unrolled loop
	SUB $7, R1, R5

unroll8_loop32:
	CMP R4, R5
	BGT check_4way32

	// Process 8 elements at once for maximum ILP
	MOVW (R0)(R4<<2), R6      // val1
	ADD $1, R4
	MOVW (R0)(R4<<2), R7      // val2
	ADD $1, R4
	MOVW (R0)(R4<<2), R8      // val3
	ADD $1, R4
	MOVW (R0)(R4<<2), R9      // val4
	ADD $1, R4
	MOVW (R0)(R4<<2), R10     // val5
	ADD $1, R4
	MOVW (R0)(R4<<2), R11     // val6
	ADD $1, R4
	MOVW (R0)(R4<<2), R12     // val7
	ADD $1, R4
	MOVW (R0)(R4<<2), R13     // val8
	ADD $1, R4

	// Update min with all 8 values (optimized sequence)
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
	CMP R13, R2
	CSEL LT, R13, R2, R2

	// Update max with all 8 values
	CMP R6, R3
	CSEL GT, R6, R3, R3
	CMP R7, R3
	CSEL GT, R7, R3, R3
	CMP R8, R3
	CSEL GT, R8, R3, R3
	CMP R9, R3
	CSEL GT, R9, R3, R3
	CMP R10, R3
	CSEL GT, R10, R3, R3
	CMP R11, R3
	CSEL GT, R11, R3, R3
	CMP R12, R3
	CSEL GT, R12, R3, R3
	CMP R13, R3
	CSEL GT, R13, R3, R3

	B unroll8_loop32

check_4way32:
	// Check if we can do 4-way processing for remaining elements
	CMP $4, R1
	BLT scalar32

	// Calculate limit for 4-way unrolled loop
	SUB $3, R1, R5

unroll4_loop32:
	CMP R4, R5
	BGT scalar32

	// Process 4 elements at once
	MOVW (R0)(R4<<2), R6      // val1
	ADD $1, R4
	MOVW (R0)(R4<<2), R7      // val2
	ADD $1, R4
	MOVW (R0)(R4<<2), R8      // val3
	ADD $1, R4
	MOVW (R0)(R4<<2), R9      // val4
	ADD $1, R4

	// Update min with all 4 values
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R7, R2
	CSEL LT, R7, R2, R2
	CMP R8, R2
	CSEL LT, R8, R2, R2
	CMP R9, R2
	CSEL LT, R9, R2, R2

	// Update max with all 4 values
	CMP R6, R3
	CSEL GT, R6, R3, R3
	CMP R7, R3
	CSEL GT, R7, R3, R3
	CMP R8, R3
	CSEL GT, R8, R3, R3
	CMP R9, R3
	CSEL GT, R9, R3, R3

	B unroll4_loop32

scalar32:
	// Process remaining elements
	CMP R4, R1
	BEQ done32

	MOVW (R0)(R4<<2), R6
	CMP R6, R2
	CSEL LT, R6, R2, R2  // min = min(min, val)
	CMP R6, R3
	CSEL GT, R6, R3, R3  // max = max(max, val)

	ADD $1, R4
	B scalar32

empty32:
	MOVD $0, R2
	MOVD $0, R3

done32:
	MOVW R2, min+24(FP)
	MOVW R3, max+28(FP)
	RET

// func combinedBoundsInt64ARM64(data []int64) (min, max int64)
TEXT ·combinedBoundsInt64ARM64(SB), NOSPLIT, $0-40
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1

	// Return zeros if empty array
	CMP $0, R1
	BEQ empty64

	// Initialize min/max with first element
	MOVD (R0), R2  // min
	MOVD (R0), R3  // max
	MOVD $1, R4    // index counter

	// Optimize with 4-way unrolled loop for 64-bit values
	CMP $4, R1
	BLT check_2way64

	// Calculate limit for 4-way unrolled loop
	SUB $3, R1, R5

unroll4_loop64:
	CMP R4, R5
	BGT check_2way64

	// Process 4 elements at once
	MOVD (R0)(R4<<3), R6      // val1
	ADD $1, R4
	MOVD (R0)(R4<<3), R7      // val2
	ADD $1, R4
	MOVD (R0)(R4<<3), R8      // val3
	ADD $1, R4
	MOVD (R0)(R4<<3), R9      // val4
	ADD $1, R4

	// Update min and max
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R7, R2
	CSEL LT, R7, R2, R2
	CMP R8, R2
	CSEL LT, R8, R2, R2
	CMP R9, R2
	CSEL LT, R9, R2, R2

	CMP R6, R3
	CSEL GT, R6, R3, R3
	CMP R7, R3
	CSEL GT, R7, R3, R3
	CMP R8, R3
	CSEL GT, R8, R3, R3
	CMP R9, R3
	CSEL GT, R9, R3, R3

	B unroll4_loop64

check_2way64:
	// Check if we can do 2-way processing for remaining elements
	CMP $2, R1
	BLT scalar64

	SUB $1, R1, R5

unroll2_loop64:
	CMP R4, R5
	BGT scalar64

	// Process 2 elements at once
	MOVD (R0)(R4<<3), R6      // val1
	ADD $1, R4
	MOVD (R0)(R4<<3), R7      // val2
	ADD $1, R4

	// Update min and max
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R7, R2
	CSEL LT, R7, R2, R2

	CMP R6, R3
	CSEL GT, R6, R3, R3
	CMP R7, R3
	CSEL GT, R7, R3, R3

	B unroll2_loop64

scalar64:
	CMP R4, R1
	BEQ done64

	MOVD (R0)(R4<<3), R6
	CMP R6, R2
	CSEL LT, R6, R2, R2
	CMP R6, R3
	CSEL GT, R6, R3, R3

	ADD $1, R4
	B scalar64

empty64:
	MOVD $0, R2
	MOVD $0, R3

done64:
	MOVD R2, min+24(FP)
	MOVD R3, max+32(FP)
	RET

// func combinedBoundsUint32ARM64(data []uint32) (min, max uint32)
TEXT ·combinedBoundsUint32ARM64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1

	CMP $0, R1
	BEQ empty_uint32

	MOVW (R0), R2  // min
	MOVW (R0), R3  // max
	MOVD $1, R4    // index counter

	// 8-way loop unrolling for uint32
	CMP $8, R1
	BLT check_4way_uint32

	SUB $7, R1, R5

unroll8_loop_uint32:
	CMP R4, R5
	BGT check_4way_uint32

	MOVW (R0)(R4<<2), R6
	ADD $1, R4
	MOVW (R0)(R4<<2), R7
	ADD $1, R4
	MOVW (R0)(R4<<2), R8
	ADD $1, R4
	MOVW (R0)(R4<<2), R9
	ADD $1, R4
	MOVW (R0)(R4<<2), R10
	ADD $1, R4
	MOVW (R0)(R4<<2), R11
	ADD $1, R4
	MOVW (R0)(R4<<2), R12
	ADD $1, R4
	MOVW (R0)(R4<<2), R13
	ADD $1, R4

	// Unsigned comparisons for min
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
	CMPW R13, R2
	CSEL LO, R13, R2, R2

	// Unsigned comparisons for max
	CMPW R6, R3
	CSEL HI, R6, R3, R3
	CMPW R7, R3
	CSEL HI, R7, R3, R3
	CMPW R8, R3
	CSEL HI, R8, R3, R3
	CMPW R9, R3
	CSEL HI, R9, R3, R3
	CMPW R10, R3
	CSEL HI, R10, R3, R3
	CMPW R11, R3
	CSEL HI, R11, R3, R3
	CMPW R12, R3
	CSEL HI, R12, R3, R3
	CMPW R13, R3
	CSEL HI, R13, R3, R3

	B unroll8_loop_uint32

check_4way_uint32:
	CMP $4, R1
	BLT scalar_uint32

	SUB $3, R1, R5

unroll4_loop_uint32:
	CMP R4, R5
	BGT scalar_uint32

	MOVW (R0)(R4<<2), R6
	ADD $1, R4
	MOVW (R0)(R4<<2), R7
	ADD $1, R4
	MOVW (R0)(R4<<2), R8
	ADD $1, R4
	MOVW (R0)(R4<<2), R9
	ADD $1, R4

	// Unsigned comparisons
	CMPW R6, R2
	CSEL LO, R6, R2, R2
	CMPW R7, R2
	CSEL LO, R7, R2, R2
	CMPW R8, R2
	CSEL LO, R8, R2, R2
	CMPW R9, R2
	CSEL LO, R9, R2, R2

	CMPW R6, R3
	CSEL HI, R6, R3, R3
	CMPW R7, R3
	CSEL HI, R7, R3, R3
	CMPW R8, R3
	CSEL HI, R8, R3, R3
	CMPW R9, R3
	CSEL HI, R9, R3, R3

	B unroll4_loop_uint32

scalar_uint32:
	CMP R4, R1
	BEQ done_uint32

	MOVW (R0)(R4<<2), R6
	CMPW R6, R2
	CSEL LO, R6, R2, R2
	CMPW R6, R3
	CSEL HI, R6, R3, R3

	ADD $1, R4
	B scalar_uint32

empty_uint32:
	MOVD $0, R2
	MOVD $0, R3

done_uint32:
	MOVW R2, min+24(FP)
	MOVW R3, max+28(FP)
	RET

// func combinedBoundsUint64ARM64(data []uint64) (min, max uint64)
TEXT ·combinedBoundsUint64ARM64(SB), NOSPLIT, $0-40
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1

	CMP $0, R1
	BEQ empty_uint64

	MOVD (R0), R2  // min
	MOVD (R0), R3  // max
	MOVD $1, R4    // index counter

	// 4-way loop unrolling for uint64
	CMP $4, R1
	BLT check_2way_uint64

	SUB $3, R1, R5

unroll4_loop_uint64:
	CMP R4, R5
	BGT check_2way_uint64

	MOVD (R0)(R4<<3), R6
	ADD $1, R4
	MOVD (R0)(R4<<3), R7
	ADD $1, R4
	MOVD (R0)(R4<<3), R8
	ADD $1, R4
	MOVD (R0)(R4<<3), R9
	ADD $1, R4

	// Unsigned comparisons
	CMP R6, R2
	CSEL LO, R6, R2, R2
	CMP R7, R2
	CSEL LO, R7, R2, R2
	CMP R8, R2
	CSEL LO, R8, R2, R2
	CMP R9, R2
	CSEL LO, R9, R2, R2

	CMP R6, R3
	CSEL HI, R6, R3, R3
	CMP R7, R3
	CSEL HI, R7, R3, R3
	CMP R8, R3
	CSEL HI, R8, R3, R3
	CMP R9, R3
	CSEL HI, R9, R3, R3

	B unroll4_loop_uint64

check_2way_uint64:
	CMP $2, R1
	BLT scalar_uint64

	SUB $1, R1, R5

unroll2_loop_uint64:
	CMP R4, R5
	BGT scalar_uint64

	MOVD (R0)(R4<<3), R6
	ADD $1, R4
	MOVD (R0)(R4<<3), R7
	ADD $1, R4

	// Unsigned comparisons
	CMP R6, R2
	CSEL LO, R6, R2, R2
	CMP R7, R2
	CSEL LO, R7, R2, R2

	CMP R6, R3
	CSEL HI, R6, R3, R3
	CMP R7, R3
	CSEL HI, R7, R3, R3

	B unroll2_loop_uint64

scalar_uint64:
	CMP R4, R1
	BEQ done_uint64

	MOVD (R0)(R4<<3), R6
	CMP R6, R2
	CSEL LO, R6, R2, R2
	CMP R6, R3
	CSEL HI, R6, R3, R3

	ADD $1, R4
	B scalar_uint64

empty_uint64:
	MOVD $0, R2
	MOVD $0, R3

done_uint64:
	MOVD R2, min+24(FP)
	MOVD R3, max+32(FP)
	RET

// func combinedBoundsFloat32ARM64(data []float32) (min, max float32)
TEXT ·combinedBoundsFloat32ARM64(SB), NOSPLIT, $0-32
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1

	CMP $0, R1
	BEQ empty_float32

	FMOVS (R0), F2  // min
	FMOVS (R0), F3  // max
	MOVD $1, R4     // index counter

	// Optimize with 4-way unrolled loop for float32
	CMP $4, R1
	BLT scalar_float32

	SUB $3, R1, R5

unroll4_loop_float32:
	CMP R4, R5
	BGT scalar_float32

	FMOVS (R0)(R4<<2), F5
	ADD $1, R4
	FMOVS (R0)(R4<<2), F6
	ADD $1, R4
	FMOVS (R0)(R4<<2), F7
	ADD $1, R4
	FMOVS (R0)(R4<<2), F8
	ADD $1, R4

	// Update min
	FCMPS F5, F2
	BPL no_min1_f32
	FMOVS F5, F2
no_min1_f32:
	FCMPS F6, F2
	BPL no_min2_f32
	FMOVS F6, F2
no_min2_f32:
	FCMPS F7, F2
	BPL no_min3_f32
	FMOVS F7, F2
no_min3_f32:
	FCMPS F8, F2
	BPL no_min4_f32
	FMOVS F8, F2
no_min4_f32:

	// Update max
	FCMPS F5, F3
	BMI no_max1_f32
	FMOVS F5, F3
no_max1_f32:
	FCMPS F6, F3
	BMI no_max2_f32
	FMOVS F6, F3
no_max2_f32:
	FCMPS F7, F3
	BMI no_max3_f32
	FMOVS F7, F3
no_max3_f32:
	FCMPS F8, F3
	BMI no_max4_f32
	FMOVS F8, F3
no_max4_f32:

	B unroll4_loop_float32

scalar_float32:
	CMP R4, R1
	BEQ done_float32

	FMOVS (R0)(R4<<2), F5
	FCMPS F5, F2
	BPL no_scalar_min_f32
	FMOVS F5, F2
no_scalar_min_f32:
	FCMPS F5, F3
	BMI no_scalar_max_f32
	FMOVS F5, F3
no_scalar_max_f32:

	ADD $1, R4
	B scalar_float32

empty_float32:
	FMOVS $0.0, F2
	FMOVS $0.0, F3

done_float32:
	FMOVS F2, min+24(FP)
	FMOVS F3, max+28(FP)
	RET

// func combinedBoundsFloat64ARM64(data []float64) (min, max float64)
TEXT ·combinedBoundsFloat64ARM64(SB), NOSPLIT, $0-40
	MOVD data_base+0(FP), R0
	MOVD data_len+8(FP), R1

	CMP $0, R1
	BEQ empty_float64

	FMOVD (R0), F2  // min
	FMOVD (R0), F3  // max
	MOVD $1, R4     // index counter

	// Optimize with 4-way unrolled loop for float64
	CMP $4, R1
	BLT scalar_float64

	SUB $3, R1, R5

unroll4_loop_float64:
	CMP R4, R5
	BGT scalar_float64

	FMOVD (R0)(R4<<3), F5
	ADD $1, R4
	FMOVD (R0)(R4<<3), F6
	ADD $1, R4
	FMOVD (R0)(R4<<3), F7
	ADD $1, R4
	FMOVD (R0)(R4<<3), F8
	ADD $1, R4

	// Update min
	FCMPD F5, F2
	BPL no_min1_f64
	FMOVD F5, F2
no_min1_f64:
	FCMPD F6, F2
	BPL no_min2_f64
	FMOVD F6, F2
no_min2_f64:
	FCMPD F7, F2
	BPL no_min3_f64
	FMOVD F7, F2
no_min3_f64:
	FCMPD F8, F2
	BPL no_min4_f64
	FMOVD F8, F2
no_min4_f64:

	// Update max
	FCMPD F5, F3
	BMI no_max1_f64
	FMOVD F5, F3
no_max1_f64:
	FCMPD F6, F3
	BMI no_max2_f64
	FMOVD F6, F3
no_max2_f64:
	FCMPD F7, F3
	BMI no_max3_f64
	FMOVD F7, F3
no_max3_f64:
	FCMPD F8, F3
	BMI no_max4_f64
	FMOVD F8, F3
no_max4_f64:

	B unroll4_loop_float64

scalar_float64:
	CMP R4, R1
	BEQ done_float64

	FMOVD (R0)(R4<<3), F5
	FCMPD F5, F2
	BPL no_scalar_min_f64
	FMOVD F5, F2
no_scalar_min_f64:
	FCMPD F5, F3
	BMI no_scalar_max_f64
	FMOVD F5, F3
no_scalar_max_f64:

	ADD $1, R4
	B scalar_float64

empty_float64:
	FMOVD $0.0, F2
	FMOVD $0.0, F3

done_float64:
	FMOVD F2, min+24(FP)
	FMOVD F3, max+32(FP)
	RET
