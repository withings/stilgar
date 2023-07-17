/* stilgar - a lightweight, no-fuss, drop-in replacement for Rudderstack
 * Copyright (C) 2023 Withings
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>. */

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
