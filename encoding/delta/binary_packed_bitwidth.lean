/-
Lean 4.36.0, using only Std. Check from the repository root:
  lean -R encoding/delta -o encoding/delta/binary_packed_bitwidth.olean encoding/delta/binary_packed_bitwidth.lean
  LEAN_PATH=encoding/delta leanchecker binary_packed_bitwidth
The #print axioms commands below audit every theorem.
-/
import Std

/- This proof concerns unsigned bit lengths. The correspondence between the
   native instruction sequence and this OR fold, including the ABI and the
   LZCNTQ interpretation, is an unmodeled premise. -/
def bitWidth (n : Nat) : Nat := if n = 0 then 0 else n.log2 + 1

theorem bitWidth_le (x k : Nat) : bitWidth x ≤ k ↔ x < 2 ^ k := by
  by_cases h : x = 0
  · simp [bitWidth, h, Nat.two_pow_pos]
  · simp only [bitWidth, ite_eq_right h, Nat.add_one_le_iff]
    exact Nat.log2_lt h

theorem bitWidth_or (x y : Nat) :
    bitWidth (x ||| y) = max (bitWidth x) (bitWidth y) := by
  have bound (k : Nat) : bitWidth (x ||| y) ≤ k ↔
      max (bitWidth x) (bitWidth y) ≤ k := by
    simp only [bitWidth_le, Nat.max_le]
    constructor
    · intro h
      exact ⟨Nat.lt_of_le_of_lt Nat.left_le_or h,
             Nat.lt_of_le_of_lt Nat.right_le_or h⟩
    · intro ⟨hx, hy⟩
      exact Nat.or_lt_two_pow hx hy
  exact Nat.le_antisymm ((bound _).mpr (Nat.le_refl _))
    ((bound _).mp (Nat.le_refl _))

theorem miniBlock_width (xs : List (BitVec 64)) :
    bitWidth ((xs.foldr (· ||| ·) 0).toNat) =
      xs.foldr (fun x w => max (bitWidth x.toNat) w) 0 := by
  induction xs with
  | nil => decide
  | cons x xs ih =>
    simp only [List.foldr_cons, BitVec.toNat_or, bitWidth_or, ih]

-- A reduction that loses its second operand is not width-preserving.
theorem omitted_lane_counterexample :
    bitWidth (0 : Nat) ≠ bitWidth (0 ||| 9223372036854775808) := by decide

#print axioms bitWidth_le
#print axioms bitWidth_or
#print axioms miniBlock_width
#print axioms omitted_lane_counterexample
