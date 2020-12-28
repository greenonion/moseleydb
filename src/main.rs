use std::io::{self, Write};

fn main() {
    loop {
        print_prompt();

        let input = read_input();

        match input.as_str() {
            ".exit" => break,
            _ => {
                println!("Unrecognized command '{}'.", input)
            }
        }
    }
}

fn print_prompt() {
    print!("db > ");
    io::stdout().flush().unwrap();
}

fn read_input() -> String {
    let mut input_buffer = String::new();
    io::stdin()
        .read_line(&mut input_buffer)
        .expect("Error reading input");

    input_buffer.trim_end().to_string()
}
