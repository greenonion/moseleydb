use std::fmt;
use std::io::{self, Write};

type MetaCommandResult = Result<MetaCommand, MetaCommandError>;

enum MetaCommand {
    Exit,
}

impl MetaCommand {
    fn new(input: &str) -> MetaCommandResult {
        match input {
            ".exit" => Ok(MetaCommand::Exit),
            _ => Err(MetaCommandError::UnrecognizedCommand(input.to_string())),
        }
    }
}

enum MetaCommandError {
    UnrecognizedCommand(String),
}

impl fmt::Display for MetaCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MetaCommandError::UnrecognizedCommand(s) => {
                write!(f, "Unrecognized command '{}'.", s)
            }
        }
    }
}

type PrepareResult = Result<Statement, StatementError>;

enum Statement {
    Insert(String),
    Select(String),
}

impl Statement {
    fn new(input: &str) -> PrepareResult {
        if input.starts_with("insert") {
            Ok(Statement::Insert(input.to_string()))
        } else if input.starts_with("select") {
            Ok(Statement::Select(input.to_string()))
        } else {
            Err(StatementError::UnrecognizedKeyword(input.to_string()))
        }
    }

    fn execute(&self) {
        match self {
            Statement::Insert(s) => println!("This is where we would do a insert."),
            Statement::Select(s) => println!("This is where we would do a select."),
        }
    }
}

enum StatementError {
    UnrecognizedKeyword(String),
}

impl fmt::Display for StatementError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatementError::UnrecognizedKeyword(s) => {
                write!(f, "Unrecognized keyword at start of '{}'.", s)
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

fn main() {
    loop {
        print_prompt();

        let input = read_input();

        if input.starts_with('.') {
            match MetaCommand::new(&input) {
                Ok(MetaCommand::Exit) => break,
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
        }

        match Statement::new(&input) {
            Ok(s) => {
                s.execute();
            }
            Err(e) => {
                println!("{}", e);
                continue;
            }
        }

        println!("Executed");
    }
}
