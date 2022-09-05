extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    let redisdb_source_dir= "../../src";
    let redisdb_build_dir = "../../cmake-build-debug";

    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search={}", redisdb_build_dir);

    // Tell cargo to tell rustc to link the system bzip2
    // shared library.
    println!("cargo:rustc-link-lib=redisdb");

    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=wrapper.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .clang_arg("-I".to_owned() + redisdb_source_dir)
        .clang_arg("-I".to_owned() + redisdb_build_dir + "/_deps/rocksdb-src/include/rocksdb")
        .header("wrapper.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
