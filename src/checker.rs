mod events;

use events::rejections::explain_rejection;

/// Helper program to troubleshoot rejected events: pass event on stdin, get an explanation
pub fn main() {
    let input = match std::io::read_to_string(std::io::stdin()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to read stdin: {}", e);
            std::process::exit(1);
        }
    };

    let explanations = explain_rejection(&input);
    match explanations.is_empty() {
        true => {
            println!("No errors.");
        },
        false => {
            println!("{}", explanations.join("\n"));
            std::process::exit(1);
        }
    }
}
