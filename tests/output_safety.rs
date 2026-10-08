#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn standard_input_alias_is_rejected_before_output_is_opened() {
        let directory = tempfile::tempdir().unwrap();
        let input = directory.path().join("reads.fa");
        let data = b">a\nACGT\n";
        std::fs::write(&input, data).unwrap();
        let result = std::process::Command::new(env!("CARGO_BIN_EXE_fastq-stats"))
            .args(["-", "--output", input.to_str().unwrap(), "--threads", "1"])
            .stdin(std::fs::File::open(&input).unwrap())
            .output()
            .unwrap();
        assert_eq!(result.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&result.stderr).contains("would overwrite input"));
        assert_eq!(std::fs::read(input).unwrap(), data);
    }
}
