-- Lean 4.36.0. Check from the repository root:
-- lean encoding/delta/binary_packed_decode_int32.lean

import Std

-- Premises: VEX packed adds implement BitVec 32 addition. Byte shifts stay
-- inside each 128-bit half. The two stores, bounds and scalar tail are unchanged.
def localScan (a b c d e f g h : BitVec 32) : List (BitVec 32) :=
  let lo := d + c + (b + a)
  [a, b+a, c+b+a, lo, e+lo, f+e+lo, g+f+e+lo, h+g+(f+e)+lo]
def sequential (carry a b c d e f g h : BitVec 32) : List (BitVec 32) :=
  [carry+a, carry+a+b, carry+a+b+c, carry+a+b+c+d,
   carry+a+b+c+d+e, carry+a+b+c+d+e+f,
   carry+a+b+c+d+e+f+g, carry+a+b+c+d+e+f+g+h]

theorem add_left_comm (a b c : BitVec 32) : a + (b + c) = b + (a + c) := by
  rw [← BitVec.add_assoc, ← BitVec.add_assoc, BitVec.add_comm a b]

-- Every lane, including the final lane broadcast as the next carry, agrees.
theorem carry_scan (carry a b c d e f g h : BitVec 32) :
    (localScan a b c d e f g h).map (· + carry) =
      sequential carry a b c d e f g h := by
  simp [localScan, sequential, BitVec.add_assoc, BitVec.add_comm, add_left_comm]
#print axioms carry_scan

def scalarScan (carry : BitVec 32) : List (BitVec 32) → List (BitVec 32)
  | [] => []
  | x :: xs => (carry+x) :: scalarScan (carry+x) xs

def vectorScan (carry : BitVec 32) : List (BitVec 32) → List (BitVec 32)
  | a::b::c::d::e::f::g::h::xs =>
      (localScan a b c d e f g h).map (· + carry) ++
        vectorScan (h+g+(f+e)+(d+c+(b+a))+carry) xs
  | xs => scalarScan carry xs

theorem vector_scan_eq (carry : BitVec 32) (xs : List (BitVec 32)) :
    vectorScan carry xs = scalarScan carry xs := by
  fun_induction vectorScan carry xs <;>
    simp_all [scalarScan, localScan,
      BitVec.add_assoc, BitVec.add_comm, add_left_comm]
#print axioms vector_scan_eq
