pub use brad_qe::radixspline::build;
pub use brad_qe::radixspline::lookup;
pub use brad_qe::radixspline::clear;


fn main() -> () {
    let mut input: [u64; 15] = [1,2,3,4,6,7,8,9,11,12,13,15,17,20,21];
    unsafe {
        let rs_ptr = build(input.as_mut_ptr(), 15);
        println!("7 is in the array: {}", lookup(rs_ptr, 7));

        println!("13 is in the array: {}", lookup(rs_ptr, 13));

        println!("5 is in the array: {}", lookup(rs_ptr, 5));
    }
}
