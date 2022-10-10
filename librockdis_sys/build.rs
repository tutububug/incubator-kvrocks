use cc::Build;
use std::path::{Path, PathBuf};
use std::str;
use std::env;

fn main() {
    let rocksdb_include_dir = env::current_dir().unwrap()
        .join("rocksdb").join("include");

    let mut build= build_rockdis(vec![&rocksdb_include_dir]);
    link_cpp(&mut build);
    build.warnings(false).compile("librockdis.a");

    let file_path = env::current_dir().unwrap()
        .join("src").join("lib.rs");
    bindgen_rockdis(file_path.as_path(), &rocksdb_include_dir);
}

fn build_rockdis(include_dirs: Vec<&PathBuf>) -> Build {
    let mut build = Build::new();
    for dir in include_dirs {
        build.include(dir);
    }
    build.cpp(true)
        .flag("-std=c++11")
        .files(vec![
            "../src/redis_zset.cc",
            "../src/redis_set.cc",
            "../src/redis_list.cc",
            "../src/redis_hash.cc",
            "../src/redis_string.cc",
            "../src/redis_db.cc",
            "../src/redis_metadata.cc",
            "../src/encoding.cc",
            "../src/store.cc",
            "../src/util.cc",
            "../src/lock_manager.cc",
            "../src/redis_slot.cc",
            "../src/redis_key_encoding.cc",
            "../src/redis_processor.cc",
            "../src/redis_processor_c.cc",
            "../src/redis_request.cc",
            "../src/redis_cmd.cc",
            "../src/redis_reply.cc",
        ]);
    println!("cargo:rustc-link-lib=static=rockdis");
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=static=z");
    println!("cargo:rustc-link-lib=static=bz2");
    println!("cargo:rustc-link-lib=static=lz4");
    println!("cargo:rustc-link-lib=static=zstd");
    println!("cargo:rustc-link-lib=static=snappy");
    build
}

fn link_cpp(build: &mut Build) {
    let tool = build.get_compiler();
    let stdlib = if tool.is_like_gnu() {
        "libstdc++.a"
    } else if tool.is_like_clang() {
        "libc++.a"
    } else {
        // Don't link to c++ statically on windows.
        return;
    };
    let output = tool
        .to_command()
        .arg("--print-file-name")
        .arg(stdlib)
        .output()
        .unwrap();
    if !output.status.success() || output.stdout.is_empty() {
        // fallback to dynamically
        return;
    }
    let path = match str::from_utf8(&output.stdout) {
        Ok(path) => PathBuf::from(path),
        Err(_) => return,
    };
    if !path.is_absolute() {
        return;
    }
    // remove lib prefix and .a postfix.
    let libname = &stdlib[3..stdlib.len() - 2];
    println!("cargo:rustc-link-lib=static={}", &libname);
    println!("cargo:rustc-link-search=native={}", path.parent().unwrap().display());
    build.cpp_link_stdlib(None);
}

fn bindgen_rockdis(file_path: &Path, include_dir: &PathBuf) {
    let builder= bindgen::Builder::default()
        .header("../src/redis_processor_c.h")
        .clang_arg("-I".to_owned() + include_dir.to_str().unwrap());
    let bindings = builder.generate().expect("Unable to generate bindings");
    bindings.write_to_file(file_path).expect("Couldn't write bindings!");
}