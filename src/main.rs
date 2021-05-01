use regex::Regex;
use std::io::SeekFrom;
use std::str;
use std::{convert::TryInto, fs::OpenOptions};
use std::{env, io::Seek};
use std::{fmt, str::FromStr};
use std::{
    fs::{self, File},
    io::Read,
};
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
struct Pager {
    db_io: File,
    file_length: u64,
    pages: Vec<Option<Page>>,
}

impl Pager {
    fn open(file_name: &str) -> Self {
        let db_io = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_name)
            .expect("Unable to open file");

        let file_length = fs::metadata(file_name)
            .expect("Failed to get file metadata")
            .len();

        let mut pages: Vec<Option<Page>> = Vec::with_capacity(TABLE_MAX_PAGES);
        for _ in 0..TABLE_MAX_PAGES {
            pages.push(None);
        }

        Pager {
            db_io,
            file_length,
            pages,
        }
    }

    fn get_page(&mut self, page_num: usize) -> Option<&mut Page> {
        if page_num > TABLE_MAX_PAGES {
            panic!(
                "Tried to fetch page number out of bounds. {} > {}",
                page_num, TABLE_MAX_PAGES
            );
        }

        if self.pages[page_num].is_none() {
            // Cache miss
            let mut page = Page::new();
            let mut num_pages: usize = self.file_length as usize / PAGE_SIZE;

            // We might save a partial page at the end of the file
            if self.file_length as usize % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            let offset = page_num * PAGE_SIZE;

            self.db_io
                .seek(SeekFrom::Start(offset as u64))
                .expect("Error reading file.");

            if page_num < num_pages {
                if page_num == num_pages - 1 {
                    let num_bytes = self.file_length as usize % PAGE_SIZE;
                    self.db_io.read_exact(&mut page.data[..num_bytes]).unwrap();
                } else {
                    self.db_io.read_exact(&mut page.data).unwrap();
                }
            }

            self.pages[page_num] = Some(page);
        }

        self.pages[page_num].as_mut()
    }

    fn insert(&mut self, page_num: usize, row: &Row, row_num: usize) {
        let row_offset: usize = row_num % ROWS_PER_PAGE;

        if let Some(page) = self.get_page(page_num) {
            page.insert(row, row_offset);
        }
    }

    fn flush_pages(&mut self, num_full_pages: usize, num_additional_rows: usize) {
        for i in 0..num_full_pages {
            self.flush_page(i, None);
            self.pages[i] = None;
        }

        if num_additional_rows > 0 {
            let page_num: usize = num_full_pages;
            self.flush_page(page_num, Some(num_additional_rows));
            self.pages[page_num] = None;
        }
    }

    fn flush_page(&mut self, page_num: usize, num_rows: Option<usize>) {
        if self.pages[page_num].is_none() {
            panic!("Tried to flush null page.");
        }

        if let Some(page) = &self.pages[page_num] {
            let offset = page_num * PAGE_SIZE;
            self.db_io
                .seek(SeekFrom::Start(offset as u64))
                .expect("Error seeking.");

            if let Some(num_rows) = num_rows {
                // Only write specific number of rows
                self.db_io
                    .write_all(&page.data[..num_rows * ROW_SIZE])
                    .expect("Error writing.")
            } else {
                self.db_io.write_all(&page.data).expect("Error writing.")
            }
        }
    }
}

#[derive(Debug)]
struct Cursor<'cursor> {
    table: &'cursor mut Table, // TODO: maybe just put the table inside a Box?
    row_num: usize,
    end_of_table: bool,
}

impl<'cursor> Cursor<'cursor> {
    fn table_start(table: &'cursor mut Table) -> Self {
        let end_of_table: bool = table.num_rows == 0;

        Cursor {
            table,
            row_num: 0,
            end_of_table,
        }
    }

    fn table_end(table: &'cursor mut Table) -> Self {
        let row_num = table.num_rows;

        Cursor {
            table,
            row_num,
            end_of_table: true,
        }
    }

    // Insert row where cursor is pointing
    fn insert_row(&mut self, row: &Row) {
        let page_num: usize = self.row_num / ROWS_PER_PAGE;
        self.table.pager.insert(page_num, &row, self.row_num);
    }

}

impl<'cursor> Iterator for Cursor<'cursor> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_num < self.table.num_rows {
            let page_num: usize = self.row_num / ROWS_PER_PAGE;
            let row_offset: usize = self.row_num % ROWS_PER_PAGE;
            let byte_offset: usize = row_offset * ROW_SIZE;

            self.row_num += 1;

            if let Some(page) = self.table.pager.get_page(page_num) {
                let row_data = &page.data[byte_offset..byte_offset + ROW_SIZE];
                return Some(Row::deserialize(row_data));
            }
        }
        None
    }
}

#[derive(Debug)]
struct Table {
    pager: Pager,
    num_rows: usize,
}

impl Table {
    fn db_open(file_name: &str) -> Self {
        let pager = Pager::open(file_name);
        let num_rows = pager.file_length as usize / ROW_SIZE;

        Table { pager, num_rows }
    }

    fn db_close(&mut self) {
        let num_full_pages: usize = self.num_rows / ROWS_PER_PAGE;
        let num_additional_rows: usize = self.num_rows % ROWS_PER_PAGE;

        self.pager.flush_pages(num_full_pages, num_additional_rows);
    }

    fn insert(&mut self, row: &Row) -> ExecuteResult {
        if self.num_rows == TABLE_MAX_ROWS {
            return Err(ExecuteError::TableFull);
        }

        let mut cursor = Cursor::table_end(self);
        cursor.insert_row(row);
        self.num_rows += 1;

        Ok(())
    }

    fn select(&mut self) -> ExecuteResult {
        let cursor = Cursor::table_start(self);
        for row in cursor.into_iter() {
            println!("{}", row);
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

    // TODO: Return result type here
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
            Statement::Insert(row) => {
                //let cursor = Cursor::table_end(&mut table);
                table.insert(row)
            }
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
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("Must supply a database filename.");
    }

    let filename = &args[1];
    let mut table = Table::db_open(&filename);
    loop {
        print_prompt();

        let input = read_input();

        if input.starts_with('.') {
            match MetaCommand::new(&input) {
                Ok(MetaCommand::Exit) => {
                    table.db_close();
                    break;
                }
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
