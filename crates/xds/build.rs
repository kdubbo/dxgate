use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from("proto");
    let mut protos = Vec::new();
    collect_proto_files(&proto_root, &mut protos)?;
    protos.sort();

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    if !protoc_available() && copy_generated_sources()? {
        return Ok(());
    }

    tonic_build::configure()
        .build_server(false)
        .compile(&protos, &[proto_root])?;

    Ok(())
}

fn protoc_available() -> bool {
    let protoc = std::env::var_os("PROTOC").unwrap_or_else(|| OsString::from("protoc"));
    Command::new(protoc).arg("--version").output().is_ok()
}

fn copy_generated_sources() -> std::io::Result<bool> {
    let generated = PathBuf::from("src/generated");
    if !generated.is_dir() {
        return Ok(false);
    }

    println!("cargo:rerun-if-changed={}", generated.display());
    let out_dir =
        PathBuf::from(std::env::var_os("OUT_DIR").ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "OUT_DIR is not set")
        })?);
    for entry in fs::read_dir(generated)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "rs") {
            fs::copy(&path, out_dir.join(entry.file_name()))?;
        }
    }
    Ok(true)
}

fn collect_proto_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_proto_files(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            out.push(path);
        }
    }
    Ok(())
}
