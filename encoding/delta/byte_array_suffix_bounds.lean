/- Lean 4.36.0. Check from the repository root:
   lean -o encoding/delta/byte_array_suffix_bounds.olean encoding/delta/byte_array_suffix_bounds.lean
   LEAN_PATH=. leanchecker encoding.delta.byte_array_suffix_bounds
   The #print axioms commands below audit every theorem. -/

import Lean.Elab.Tactic.Omega

/- The encoder validates offset slices before copying. Source and destination
   are disjoint, all lengths and cursor sums fit in a signed 64-bit integer,
   and p/suffix lengths fit in nonnegative int32. Assembly instruction
   semantics and the correspondence to the Go/Plan 9 loop are not modeled.
   `remaining` counts suffix bytes, not source bytes including future prefixes.
   The assembly tests remaining >= 32 before any vector access, consumes the
   current p, copies the initial 32 bytes, then full chunks at offsets
   32,64,... strictly below n-32, and a final exact block at n-32 for n > 32.
   It returns its unconsumed cursors to the Go copy loop on either bail-out. -/

def suffixBytes : List (Nat × Nat) → Nat
  | [] => 0
  | (_, n) :: rest => n + suffixBytes rest

def inputBytes : List (Nat × Nat) → Nat
  | [] => 0
  | (p, n) :: rest => p + n + inputBytes rest

theorem input_covers_suffix (values : List (Nat × Nat)) :
    suffixBytes values ≤ inputBytes values := by
  induction values with
  | nil => simp [suffixBytes, inputBytes]
  | cons v rest ih =>
    cases v with
    | mk p n => simp only [suffixBytes, inputBytes]; omega

theorem current_covers (p n : Nat) (rest : List (Nat × Nat)) :
    p + (n + suffixBytes rest) ≤ p + n + inputBytes rest := by
  have h := input_covers_suffix rest
  omega

theorem source_after_prefix (src p remaining : Nat)
    (h : p + remaining ≤ src) : remaining ≤ src - p := by omega

theorem first_block (src p remaining : Nat)
    (hsrc : p + remaining ≤ src) (hguard : 32 ≤ remaining) :
    p + 32 ≤ src ∧ 32 ≤ remaining := by omega

theorem full_block (src p remaining n offset : Nat)
    (hsrc : p + remaining ≤ src) (hlen : n ≤ remaining)
    (hlong : 32 < n) (h : offset < n - 32) :
    p + offset + 32 ≤ src ∧ offset + 32 ≤ remaining := by omega

theorem last_block (src p remaining n : Nat)
    (hsrc : p + remaining ≤ src) (hlen : n ≤ remaining)
    (hlong : 32 < n) :
    p + (n - 32) + 32 ≤ src ∧ (n - 32) + 32 ≤ remaining := by omega

theorem next_cursors (src p remaining n : Nat)
    (hsrc : p + remaining ≤ src) (hlen : n ≤ remaining) :
    remaining - n ≤ src - (p + n) := by omega

#print axioms source_after_prefix
#print axioms first_block
#print axioms full_block
#print axioms last_block
#print axioms next_cursors

-- Removing the guard permits a one-byte remaining suffix, but the first
-- access is still 32 bytes wide: this is a concrete refutation, not a failed
-- search for a proof of a safety condition.
theorem missing_guard_counterexample : ¬ (32 ≤ (1 : Nat)) := by decide

#print axioms input_covers_suffix
#print axioms current_covers
#print axioms missing_guard_counterexample
