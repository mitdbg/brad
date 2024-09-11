use std::env;
use std::path::PathBuf;

// NOTE: This file should be named `build.rs` once it is working.

fn main() {
    // This is the directory where the `c` library is located.
    let libdir_path = PathBuf::from("RadixSplineLib")
        // Canonicalize the path as `rustc-link-search` requires an absolute
        // path.
        .canonicalize()
        .expect("cannot canonicalize path");

    // This is the path to the `c` headers file.
    let headers_path = libdir_path.join("radixspline.h");
    let headers_path_str = headers_path.to_str().expect("Path is not a valid string");

    // This is the path to the intermediate object file for our library.
    let obj_path = libdir_path.join("radixspline.o");
    // This is the path to the static library file.
    let lib_path = libdir_path.join("libradixspline.a");

    // Run `clang` to compile the `radixspline.cpp` file into a `radixspline.o` object file.
    // Unwrap if it is not possible to spawn the process.
    if !std::process::Command::new("clang++")
        .arg("-c")
        .arg("-o")
        .arg(&obj_path)
        .arg(libdir_path.join("radixspline.cpp"))
        .output()
        .expect("could not spawn `clang`")
        .status
        .success()
    {
        // Panic if the command was not successful.
        panic!("could not compile object file");
    }

    // Run `ar` to generate the `libradixspline.a` file from the `radixspline.o` file.
    // Unwrap if it is not possible to spawn the process.
    if !std::process::Command::new("ar")
        .arg("rcus")
        .arg(lib_path)
        .arg(obj_path)
        .output()
        .expect("could not spawn `ar`")
        .status
        .success()
    {
        // Panic if the command was not successful.
        panic!("could not emit library file");
    }

    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search=native={}", libdir_path.to_str().unwrap());

    // Tell cargo to tell rustc to link our `radixspline` library. Cargo will
    // automatically know it must look for a `libradixspline.a` file.
    println!("cargo:rustc-link-lib=radixspline");
    println!("cargo:rustc-link-lib=stdc++");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        .opaque_type("^(std::.*)$")
        .allowlist_function("build")
        .allowlist_function("lookup")
        .allowlist_function("clear")
        // The input header we would like to generate
        // bindings for.
        .header(headers_path_str)
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindings
        .write_to_file(out_path)
        .expect("Couldn't write bindings!");
}
