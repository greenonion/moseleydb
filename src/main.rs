use regex::Regex;
use std::convert::TryInto;
use std::str;
use std::{fmt, str::FromStr};
use std::{
    io::{self, Write},
    mem,
};

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;

const ID_SIZE: usize = mem::size_of::<u32>();
const STR_LEN_SIZE: usize = mem::size_of::<usize>();
const USERNAME_SIZE: usize = 32 + STR_LEN_SIZE;
const EMAIL_SIZE: usize = 255 + STR_LEN_SIZE;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug)]
struct Table {
    num_rows: usize,
    pages: Vec<Option<Page>>,
}

impl Table {
    fn new() -> Self {
        Table {
            num_rows: 0,
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
        }
    }

    fn insert(&mut self, row: &Row) -> ExecuteResult {
        if self.num_rows == TABLE_MAX_ROWS {
            return Err(ExecuteError::TableFull);
        }

        let row_num = self.num_rows; // Append row at the end of the table
        let page_num: usize = row_num / ROWS_PER_PAGE;
        let row_offset: usize = row_num % ROWS_PER_PAGE;

        if let Some(Some(page)) = &mut self.pages.get_mut(page_num) {
            page.insert(&row, row_offset);
        } else {
            let mut page = Page::new();
            page.insert(&row, row_offset);

            self.pages.resize_with(page_num + 1, || Some(Page::new()));
            self.pages[page_num] = Some(page);
        }

        self.num_rows += 1;
        Ok(())
    }

    fn row_data_at_slot(&self, row_num: usize) -> Option<&[u8]> {
        let page_num: usize = row_num / ROWS_PER_PAGE;
        let row_offset: usize = row_num % ROWS_PER_PAGE;
        let byte_offset: usize = row_offset * ROW_SIZE;
        if let Some(page) = &self.pages[page_num] {
            Some(&page.data[byte_offset..byte_offset + ROW_SIZE])
        } else {
            None
        }
    }
    fn select(&self) -> ExecuteResult {
        for row_num in 0..self.num_rows {
            if let Some(s) = self.row_data_at_slot(row_num) {
                let row = Row::deserialize(s);
                println!("{}", row);
            }
        }
        Ok(())
    }
}
#[derive(Debug)]
struct Page {
    data: Vec<u8>,
}

impl Page {
    fn new() -> Self {
        Page {
            data: vec![0u8; PAGE_SIZE],
        }
    }

    // TOOD: Return result type here
    fn insert(&mut self, row: &Row, offset: usize) {
        let byte_offset: usize = offset * ROW_SIZE;
        self.data[byte_offset..byte_offset + ROW_SIZE].copy_from_slice(&row.serialize());
    }
}

#[derive(Debug)]
struct Row {
    id: u32,
    username: String,
    email: String,
}

impl Row {
    fn serialize(&self) -> Vec<u8> {
        let mut out = vec![0u8; ROW_SIZE];
        let email_bytes = self.email.as_bytes();
        let username_bytes = self.username.as_bytes();

        let mut offset: usize = ID_OFFSET;
        out[offset..ID_SIZE].copy_from_slice(&self.id.to_be_bytes());

        offset = USERNAME_OFFSET;
        let un_len = username_bytes.len();
        out[offset..offset + STR_LEN_SIZE].copy_from_slice(&un_len.to_be_bytes());
        offset += STR_LEN_SIZE;
        out[offset..offset + un_len].copy_from_slice(username_bytes);

        offset = EMAIL_OFFSET;
        let em_len = email_bytes.len();
        out[offset..offset + STR_LEN_SIZE].copy_from_slice(&em_len.to_be_bytes());
        offset += STR_LEN_SIZE;
        out[offset..offset + em_len].copy_from_slice(email_bytes);
        out
    }

    fn deserialize(row: &[u8]) -> Self {
        let id_bytes = &row[ID_OFFSET..ID_OFFSET + ID_SIZE];

        let username_bytes = &row[USERNAME_OFFSET..USERNAME_OFFSET + USERNAME_SIZE];
        let username_len_bytes = &username_bytes[..STR_LEN_SIZE];
        let username_len: usize = usize::from_be_bytes(username_len_bytes.try_into().unwrap());
        let username =
            str::from_utf8(&username_bytes[STR_LEN_SIZE..STR_LEN_SIZE + username_len]).unwrap();

        let email_bytes = &row[EMAIL_OFFSET..EMAIL_OFFSET + EMAIL_SIZE];
        let email_len_bytes = &email_bytes[..STR_LEN_SIZE];
        let email_len: usize = usize::from_be_bytes(email_len_bytes.try_into().unwrap());
        let email = str::from_utf8(&email_bytes[STR_LEN_SIZE..STR_LEN_SIZE + email_len]).unwrap();

        Row {
            id: u32::from_be_bytes(id_bytes.try_into().unwrap()),
            username: username.trim_end().to_string(),
            email: email.trim_end().to_string(),
        }
    }
}

impl FromStr for Row {
    type Err = StatementError;

    fn from_str(s: &str) -> Result<Self, StatementError> {
        let re = Regex::new(r"^insert\s(.+)\s(.+)\s(.+)$").unwrap();

        match re.captures(&s) {
            Some(cap) => match cap[1].parse::<u32>() {
                Ok(id) => {
                    if cap[2].len() > COLUMN_USERNAME_SIZE || cap[3].len() > COLUMN_EMAIL_SIZE {
                        Err(StatementError::StringTooLong)
                    } else {
                        Ok(Row {
                            id,
                            username: cap[2].to_string(),
                            email: cap[3].to_string(),
                        })
                    }
                }
                Err(_) => {
                    if let Ok(n) = cap[1].parse::<i32>() {
                        if n < 0 {
                            return Err(StatementError::NegativeId);
                        }
                    }
                    Err(StatementError::SyntaxError)
                }
            },
            None => Err(StatementError::SyntaxError),
        }
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {}, {})", self.id, self.username, self.email)
    }
}

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
type ExecuteResult = Result<(), ExecuteError>;

enum ExecuteError {
    TableFull,
}

impl fmt::Display for ExecuteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExecuteError::TableFull => {
                write!(f, "Error: Table full.")
            }
        }
    }
}

enum Statement {
    Insert(Row),
    Select(String),
}

impl Statement {
    fn new(input: &str) -> PrepareResult {
        if input.starts_with("insert") {
            let row = input.parse::<Row>()?;
            Ok(Statement::Insert(row))
        } else if input.starts_with("select") {
            Ok(Statement::Select(input.to_string()))
        } else {
            Err(StatementError::UnrecognizedKeyword(input.to_string()))
        }
    }

    fn execute(&self, table: &mut Table) -> ExecuteResult {
        match self {
            Statement::Insert(row) => table.insert(row),
            Statement::Select(_) => table.select(),
        }
    }
}

enum StatementError {
    UnrecognizedKeyword(String),
    SyntaxError,
    StringTooLong,
    NegativeId,
}

impl fmt::Display for StatementError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StatementError::UnrecognizedKeyword(s) => {
                write!(f, "Unrecognized keyword at start of '{}'.", s)
            }
            StatementError::SyntaxError => {
                write!(f, "Syntax error. Could not parse statement.")
            }
            StatementError::StringTooLong => {
                write!(f, "String is too long.")
            }
            StatementError::NegativeId => {
                write!(f, "ID must be positive.")
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
    let mut table = Table::new();
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
            Ok(s) => match s.execute(&mut table) {
                Ok(_) => println!("Executed."),
                Err(e) => {
                    println!("{}", e);
                }
            },
            Err(e) => {
                println!("{}", e);
                continue;
            }
        }
    }
}
