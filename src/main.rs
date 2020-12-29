use std::io::{self, Write};

enum MetaCommand {
    Exit,
}

type MetaCommandResult = Result<MetaCommand, String>;

enum Statement {
    Insert(String),
    Select(String),
}

type PrepareResult = Result<Statement, String>;

fn do_meta_command(input: &str) -> MetaCommandResult {
    match input {
        ".exit" => Ok(MetaCommand::Exit),
        _ => Err(format!("Unrecognized command '{}'.", input)),
    }
}

fn prepare_statement(input: &str) -> PrepareResult {
    if input.starts_with("insert") {
        Ok(Statement::Insert(input.to_string()))
    } else if input.starts_with("select") {
        Ok(Statement::Select(input.to_string()))
    } else {
        Err(format!("Unrecognized keyword at start of '{}'", input))
    }
}

fn execute_statement(statement: &Statement) {
    match &statement {
        Statement::Insert(s) => println!("This is where we would do a insert."),
        Statement::Select(s) => println!("This is where we would do a select."),
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

fn main() {
    loop {
        print_prompt();

        let input = read_input();

        if input.starts_with('.') {
            match do_meta_command(&input) {
                Ok(MetaCommand::Exit) => break,
                Err(msg) => {
                    println!("{}", msg);
                    continue;
                }
            }
        }

        let statement = prepare_statement(&input);
        match statement {
            Ok(s) => {
                execute_statement(&s);
            }
            Err(msg) => {
                println!("{}", msg);
                continue;
            }
        }

        println!("Executed");
    }
}
