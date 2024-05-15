use brad_qe::radixspline::RadixSpline;

fn main() -> () {
    let input: [u64; 15] = [1,2,3,4,6,7,8,9,11,12,13,15,17,20,21];
    let rspline = RadixSpline::build_simple(input);
    println!("7 is in the array: {}", rspline.lookup(7));

    println!("13 is in the array: {}", rspline.lookup(13));

    println!("5 is in the array: {}", rspline.lookup(5));

}
