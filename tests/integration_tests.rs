use std::io::prelude::*;
use std::process::{Command, Stdio};

fn run_script(commands: Vec<String>) -> Vec<String> {
    let process = match Command::new("./target/debug/moseleydb")
        .arg("mytestdb.db")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
    {
        Err(why) => panic!("couldn't spawn db: {}", why),
        Ok(process) => process,
    };

    process
        .stdin
        .unwrap()
        .write_all(commands.join("\n").as_bytes())
        .expect("couldn't write to db stdin");

    let mut s = String::new();
    match process.stdout.unwrap().read_to_string(&mut s) {
        Err(why) => panic!("couldn't read db stdout: {}", why),
        Ok(_) => return s.as_str().split('\n').map(|s| s.to_string()).collect(),
    }
}
#[test]
fn insert_and_retrieve_a_row() {
    let commands = vec!["insert 1 user1 person1@example.com", "select", ".exit"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let out = run_script(commands);
    assert_eq!(
        out,
        vec![
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db > ",
        ]
    );
}

#[test]
fn print_error_when_table_is_full() {
    let mut commands: Vec<String> = vec![];
    for i in 1..1401 {
        commands.push(format!("insert {} user#{} person#{}@example.com", i, i, i));
    }
    commands.push(".exit".to_string());

    let out = run_script(commands);
    assert_eq!(out[out.len() - 2], "db > Error: Table full.")
}

#[test]
fn accept_maximum_length_strings() {
    let long_username = "a".repeat(32);
    let long_email = "a".repeat(255);
    let commands = vec![
        format!("insert 1 {} {}", long_username, long_email),
        String::from("select"),
        String::from(".exit"),
    ];

    let out = run_script(commands);

    assert_eq!(
        out,
        vec![
            "db > Executed.",
            format!("db > (1, {}, {})", long_username, long_email).as_str(),
            "Executed.",
            "db > ",
        ]
    )
}

#[test]
fn print_error_when_strings_too_long() {
    let long_username = "a".repeat(22);
    let long_email = "a".repeat(256);
    let commands = vec![
        format!("insert 1 {} {}", long_username, long_email),
        String::from("select"),
        String::from(".exit"),
    ];

    let out = run_script(commands);

    assert_eq!(
        out,
        vec!["db > String is too long.", "db > Executed.", "db > ",]
    )
}

#[test]
fn print_error_when_id_negative() {
    let out = run_script(vec![
        "insert -1 cstack foo@bar.com".to_string(),
        "select".to_string(),
        ".exit".to_string(),
    ]);

    assert_eq!(
        out,
        vec!["db > ID must be positive.", "db > Executed.", "db > ",]
    )
}

#[test]
fn keep_data_after_closing_connection() {
    let result1 = run_script(vec![
        "insert 1 user1 person1@example.com".to_string(),
        ".exit".to_string(),
    ]);

    assert_eq!(result1, vec!["db > Executed.", "db > ",]);

    let result2 = run_script(vec!["select".to_string(), ".exit".to_string()]);

    assert_eq!(
        result2,
        vec!["db > (1, user1, person1@example.com)", "Executed.", "db > ",]
    );
}
