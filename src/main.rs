extern crate zstd;
use chrono::format::ParseError;
use chrono::{offset::TimeZone, DateTime, FixedOffset, Local, NaiveDateTime, Utc};
use clap::{AppSettings, Clap};
use rusqlite::{params, Connection, Result, Transaction};
use std::fs::File;
use std::io::Read;
use std::{
    collections::{HashMap, HashSet},
    num::ParseFloatError,
    num::ParseIntError,
};
#[derive(Clap)]
#[clap(version = "1.0", author = "Rémy Grünblatt <remy@grunblatt.org>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    output: String,
    input_serial: String,
    input_server: String,
}

#[derive(Debug)]
enum MyError {
    E1(ParseIntError),
    E2(),
    E3(ParseError),
    E4(std::io::Error),
    E5(ParseFloatError),
    E6(rusqlite::Error),
}

impl From<ParseIntError> for MyError {
    fn from(err: ParseIntError) -> MyError {
        MyError::E1(err)
    }
}

impl From<ParseError> for MyError {
    fn from(err: ParseError) -> MyError {
        MyError::E3(err)
    }
}

impl From<std::io::Error> for MyError {
    fn from(err: std::io::Error) -> MyError {
        MyError::E4(err)
    }
}

impl From<ParseFloatError> for MyError {
    fn from(err: ParseFloatError) -> MyError {
        MyError::E5(err)
    }
}

impl From<rusqlite::Error> for MyError {
    fn from(err: rusqlite::Error) -> MyError {
        MyError::E6(err)
    }
}

fn handle_neighbor_stats(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'neighbor_stats'
        (
            'Timestamp',
            'Node',
            'L2 address',
            'fresh',
            'etx',
            'sent',
            'received',
            'rssi (dBm)',
            'lqi',
            'avg tx time (µs)'
          ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .unwrap();
    let res = stmt.execute(params![
        timestamp,
        node,
        data[1],
        data[2],
        data[3],
        data[4].replace(" ", "").parse::<i64>()?,
        data[5].replace(" ", "").parse::<i64>()?,
        data[6].replace(" ", "").parse::<i64>()?,
        data[7].replace(" ", "").parse::<i64>()?,
        data[8].replace(" ", "").parse::<i64>()?
    ]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_rpl_stats(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'rpl_stats'
        (
            'Timestamp',
            'Node',
            'Packet Type',
            'Measurement Type',
            'RX unicast',
            'TX unicast',
            'RX multicast',
            'TX multicast'
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![
        timestamp,
        node,
        data[1],
        data[2],
        data[3].replace(" ", "").parse::<i64>()?,
        data[4].replace(" ", "").parse::<i64>()?,
        data[5].replace(" ", "").parse::<i64>()?,
        data[6].replace(" ", "").parse::<i64>()?
    ]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_rpl_stats_dodag(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'rpl_stats_dodag'
        (
            'Timestamp',
            'Node',
            'Instance ID',
            'IPv6 Adress',
            'Rank',
            'Role',
            'Prefix Information',
            'Trickle Interval Size Min',
            'Trickle Interval Size Max',
            'Trickle Redundancy Constant',
            'Trickle Counter',
            'Trickle TC'
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![
        timestamp,
        node,
        data[1],
        data[2],
        data[3].replace(" ", "").parse::<i64>()?,
        data[4],
        data[5],
        data[6].replace(" ", "").parse::<i64>()?,
        data[7].replace(" ", "").parse::<i64>()?,
        data[8].replace(" ", "").parse::<i64>()?,
        data[9].replace(" ", "").parse::<i64>()?,
        data[10].replace(" ", "").parse::<i64>()?
    ]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_rpl_stats_instance(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'rpl_stats_instance'
        (
            'Timestamp',
            'Node',
            'Instance ID',
            'Interface ID',
            'Mode of Operation',
            'Objective Code Point',
            'Min Hop Rank Increase',
            'Max Rank Increase'
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![
        timestamp, node, data[1], data[2], data[3], data[4], data[5], data[6]
    ]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_rpl_stats_parent(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'rpl_stats_parent'
        (
            'Timestamp',
            'Node',
            'Instance ID',
            'IPv6 Adress',
            'Rank'
          ) VALUES (?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![timestamp, node, data[1], data[2], data[3]]);

    match res {
        Ok(1) => Ok(()),
        Ok(n) => {
            println!("Inserting multiple ?!: {}", n);
            Ok(())
        }
        Err(e) => {
            println!("Error: {:?}", e);
            Err(MyError::E2())
        }
    }
}

fn handle_rpl_status(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'rpl_status'
        (
            'Timestamp',
            'Node',
            'Type of table',
            'Index of the table',
            'Table status'
          ) VALUES (?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![timestamp, node, data[1], data[2], data[3]]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_stats(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'stats'
        (
            'Timestamp',
            'Node',
            'layer',
            'rx packets',
            'rx bytes',
            'tx packets',
            'tx multicast packets',
            'tx bytes',
            'tx succeeded',
            'tx errors'
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![
        timestamp,
        node,
        data[1].replace(" ", "").parse::<i64>()?,
        data[2],
        data[3].replace(" ", "").parse::<i64>()?,
        data[4].replace(" ", "").parse::<i64>()?,
        data[5].replace(" ", "").parse::<i64>()?,
        data[6].replace(" ", "").parse::<i64>()?,
        data[7].replace(" ", "").parse::<i64>()?,
        data[8].replace(" ", "").parse::<i64>()?,
    ]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_udp(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    data: &Vec<&str>,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'udp'
        (
            'Timestamp',
            'Node',
            'payload size',
            'destination address',
            'destination port',
            'payload'
          ) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![timestamp, node, data[1], data[2], data[3], data[4]]);

    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_server(
    transaction: &Transaction,
    timestamp: &str,
    ipv6: &str,
    port: &str,
    payload: &str,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'server'
        (
            'Timestamp',
            'IPv6 Adress',
            'receiver port',
            'payload'
          ) VALUES (?, ?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![timestamp, ipv6, port.parse::<i64>()?, payload]);
    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_output(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    stdout: &str,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'output'
        (
            'Timestamp',
            'Node',
            'Output Stdout'
          ) VALUES (?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![timestamp, node, stdout]);
    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn handle_info(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    message: &str,
) -> Result<(), MyError> {
    let mut stmt = transaction
        .prepare_cached(
            "INSERT INTO 'info'
        (
            'Timestamp',
            'Node',
            'Message'
          ) VALUES (?, ?, ?)",
        )
        .unwrap();
    let res = stmt.execute(params![timestamp, node, message]);
    match res {
        Ok(1) => Ok(()),
        _ => Err(MyError::E2()),
    }
}

fn dispatch(
    transaction: &Transaction,
    timestamp: &str,
    node: &str,
    splitted: Vec<&str>,
) -> std::result::Result<(), MyError> {
    let tag = splitted[0];
    let epoch: f64 = timestamp.parse()?;
    let timestamp =
        NaiveDateTime::from_timestamp(epoch.trunc() as i64, (1e9 * epoch.fract()) as u32);
    let tz_offset = FixedOffset::west(2 * 3600);
    let timestamp: DateTime<FixedOffset> = tz_offset.from_local_datetime(&timestamp).unwrap();
    let timestamp: DateTime<Utc> = Utc.from_utc_datetime(&timestamp.naive_utc());
    let timestamp = timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string();
    match tag {
        "neighbor_stats" => handle_neighbor_stats(transaction, &timestamp, node, &splitted),
        "rpl_stats" => handle_rpl_stats(transaction, &timestamp, node, &splitted),
        "rpl_stats_dodag" => handle_rpl_stats_dodag(transaction, &timestamp, node, &splitted),
        "rpl_stats_instance" => handle_rpl_stats_instance(transaction, &timestamp, node, &splitted),
        "rpl_stats_parent" => handle_rpl_stats_parent(transaction, &timestamp, node, &splitted),
        "rpl_status" => handle_rpl_status(transaction, &timestamp, node, &splitted),
        "stats" => handle_stats(transaction, &timestamp, node, &splitted),
        "udp" => handle_udp(transaction, &timestamp, node, &splitted),
        _ => {
            println!("Unknown tag: {}", tag);
            Ok(())
        }
    }
}

fn setup_database(connection: &Connection) {
    /* Create the tables */
    connection
        .execute_batch(
            r#"
    CREATE TABLE IF NOT EXISTS "info" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "Message" TEXT
    );

    CREATE TABLE IF NOT EXISTS "neighbor_stats" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "L2 address" TEXT,
      "fresh" INTEGER,
      "etx" TEXT,
      "sent" INTEGER,
      "received" INTEGER,
      "rssi (dBm)" INTEGER,
      "lqi" INTEGER,
      "avg tx time (µs)" INTEGER
    );

    CREATE TABLE IF NOT EXISTS "output" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "Output Stdout" TEXT
    );

    CREATE TABLE IF NOT EXISTS "rpl_stats" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "Packet Type" TEXT,
      "Measurement Type" TEXT,
      "RX unicast" INTEGER,
      "TX unicast" INTEGER,
      "RX multicast" INTEGER,
      "TX multicast" INTEGER
    );

    CREATE TABLE IF NOT EXISTS "rpl_stats_dodag" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "Instance ID" TEXT,
      "IPv6 Adress" TEXT,
      "Rank" TEXT,
      "Role" TEXT,
      "Prefix Information" TEXT,
      "Trickle Interval Size Min" INTEGER,
      "Trickle Interval Size Max" INTEGER,
      "Trickle Redundancy Constant" INTEGER,
      "Trickle Counter" INTEGER,
      "Trickle TC" INTEGER
    );

    CREATE TABLE IF NOT EXISTS "rpl_stats_instance" (
        "Timestamp" TEXT,
        "Node" TEXT,
        "Instance ID" TEXT,
        "Interface ID" TEXT,
        "Mode of Operation" TEXT,
        "Objective Code Point" TEXT,
        "Min Hop Rank Increase" TEXT,
        "Max Rank Increase" TEXT
      );

    CREATE TABLE IF NOT EXISTS "rpl_stats_parent" (
        "Timestamp" TEXT,
        "Node" TEXT,
        "Instance ID" TEXT,
        "IPv6 Adress" TEXT,
        "Rank" TEXT
    );

    CREATE TABLE IF NOT EXISTS "rpl_status" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "Type of table" TEXT,
      "Index of the table" TEXT,
      "Table status" TEXT
    );

    CREATE TABLE IF NOT EXISTS "stats" (
      "Timestamp" TEXT,
      "Node" TEXT,
      "success" INTEGER,
      "layer" TEXT,
      "rx packets" INTEGER,
      "rx bytes" INTEGER,
      "tx packets" INTEGER,
      "tx multicast packets" INTEGER,
      "tx bytes" INTEGER,
      "tx succeeded" INTEGER,
      "tx errors" INTEGER
    );

    CREATE TABLE IF NOT EXISTS "udp" (
        "Timestamp" TEXT,
        "Node" TEXT,
        "payload size" TEXT,
        "destination address" TEXT,
        "destination port" INTEGER,
        "payload" TEXT
      );
      
      CREATE TABLE IF NOT EXISTS "server" (
        "Timestamp" TEXT,
        "IPv6 Adress" TEXT,
        "receiver port" INTEGER,
        "payload" TEXT
      );
      "#,
        )
        .unwrap();
}

fn handle_serial_data(connection: &mut Connection, filename: String) -> Result<(), MyError> {
    /* Used to detect whether this is the header line for the node output */
    let mut node_headers: HashSet<(String, String)> = HashSet::new();

    /* Actual header mapping */
    let mut headers: HashMap<String, Vec<String>> = HashMap::new();

    let f = File::open(filename)?;
    let chunk_size = 100_000_000;
    let mut decoder = zstd::stream::Decoder::new(f)?;
    let mut buffer = Vec::with_capacity(chunk_size);
    let mut leftover: String = String::new();

    headers.insert("info".to_string(), vec!["Message".to_string()]);
    headers.insert(
        "rpl_stats_parent".to_string(),
        vec![
            "Instance ID".to_string(),
            "IPv6 Adress".to_string(),
            "Rank".to_string(),
        ],
    );

    let mut do_break = false;
    loop {
        println!("Reading new chunk.");
        /* We clear the previous data */
        buffer.clear();
        println!("New transaction");
        let t = connection.transaction().unwrap();

        /* We read at most chunk_size bytes and put them into the buffer */
        let n = decoder
            .by_ref()
            .take(chunk_size as u64)
            .read_to_end(&mut buffer)?;
        match n {
            0 => {
                /* No data left ! We just handle the remaining data. */
                match leftover.as_str() {
                    "" => {
                        println!("No data left.");
                        break;
                    }
                    _ => {
                        println!("Handling remaining data: {}", leftover);
                        do_break = true;
                    }
                }
            }
            _ => {
                println!("{} bytes read.", n);
                leftover += std::str::from_utf8(&buffer).unwrap();
            }
        };

        /* Find the last line */
        let last_line_pos = leftover.rfind('\n').unwrap();

        /* Only process full lines */
        let lines = (&leftover[0..last_line_pos]).lines();

        /* Iterate over the lines */
        for line in lines {
            let splitted: Vec<&str> = line.split(";").collect();
            let timestamp = splitted[0];
            let node = splitted[1];
            let data = splitted[2];
            let nested_splitted: Vec<&str> = data.split(",").collect();
            if nested_splitted.len() > 1 {
                /* Formatted Data */
                let line_type = nested_splitted[0].to_string();

                match (
                    &line_type[..],
                    node_headers.contains(&(node.to_string(), line_type.to_string())),
                ) {
                    ("info", true) => {
                        handle_info(&t, &timestamp, &node, &data)?;
                    }
                    (_, true) => {
                        /* It's data and it's not the first line: we add it to the database */
                        match dispatch(&t, &timestamp, &node, nested_splitted) {
                            Err(e) => {
                                println!("Error parsing line {}: {:?}", line, e);
                                continue;
                            }
                            _ => {}
                        }
                    }
                    ("info", false) => {
                        /* We manually add the header because we print it too early so the serial aggregator does not have time to read it */
                        node_headers.insert((node.to_string(), "info".to_string()));
                        handle_info(&t, &timestamp, &node, &data)?;
                    }
                    (line_type, false) => {
                        /* It's the first line: we add it as an header */
                        node_headers.insert((node.to_string(), line_type.to_string()));

                        /* Check wheter we already have seen this kind of data */
                        match headers.get(&(line_type.to_string())) {
                            None => {
                                headers.insert(
                                    line_type.to_string(),
                                    nested_splitted[1..].iter().map(|s| s.to_string()).collect(),
                                );
                            }
                            Some(_) => { /* The line already exists */ }
                        }
                    }
                }
            } else {
                handle_output(&t, &timestamp, &node, &data)?;
            }
        }
        /* We commit the data to the database */
        match t.commit() {
            Err(e) => {
                println!("Error commiting: {:?}", e);
            }
            _ => {
                println!("Commit successful.");
            } //thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: SqliteFailure(Error { code: Unknown, extended_code: 1 }, Some("cannot commit - no transaction is active"))', src/main.rs:614:20
        }

        /* We skip the last new line */
        leftover = leftover[(last_line_pos + 1)..].to_string();

        if do_break {
            println!("End of parsing. Leftover: {}", leftover);
            break;
        }
    }

    Ok(())
}

fn handle_server_data(connection: &mut Connection, filename: String) -> Result<(), MyError> {
    let f = File::open(filename)?;

    let chunk_size = 100_000_000;
    let mut decoder = zstd::stream::Decoder::new(f)?;
    let mut buffer = Vec::with_capacity(chunk_size);
    let mut leftover: String = String::new();
    let mut is_header: bool = true;
    let mut do_break = false;

    loop {
        /* We clear the previous data */
        buffer.clear();
        let t = connection.transaction().unwrap();

        /* We read at most chunk_size bytes and put them into the buffer */
        let n = decoder
            .by_ref()
            .take(chunk_size as u64)
            .read_to_end(&mut buffer)?;
        match n {
            0 => {
                /* No data left ! We just handle the remaining data. */
                match leftover.as_str() {
                    "" => {
                        t.commit().unwrap();
                        break;
                    }
                    _ => {
                        do_break = true;
                    }
                }
            }
            _ => {
                leftover += std::str::from_utf8(&buffer).unwrap();
            }
        };

        /* Find the last line */
        let last_line_pos = leftover.rfind('\n').unwrap();

        /* Only process full lines */
        let lines = (&leftover[0..last_line_pos]).lines();

        /* Iterate over the lines */
        for line in lines {
            if is_header {
                is_header = false;
                continue;
            }
            let splitted: Vec<&str> = line.split(",").collect();
            let timestamp = NaiveDateTime::parse_from_str(splitted[0], "%Y-%m-%d %H:%M:%S%.f");
            match timestamp {
                Err(e) => {
                    /* Problem parsing line ! */
                    println!("Problem parsing line {}, skipping the data !", line);
                    println!("Error: {}", e);
                    continue;
                }
                _ => {}
            }

            let timestamp = timestamp.unwrap();
            let timestamp: DateTime<Local> = Local.from_local_datetime(&timestamp).unwrap();
            let timestamp = timestamp.format("%Y-%m-%d %H:%M:%S.%f").to_string();
            let ipv6 = splitted[1];
            let port = splitted[2];
            let payload = splitted[3];
            handle_server(&t, &timestamp, ipv6, port, payload)?;
        }
        /* We commit the data to the database */
        t.commit().unwrap();
        /* We skip the last new line */
        leftover = leftover[(last_line_pos + 1)..].to_string();

        if do_break {
            println!("End of parsing. Leftover: {}", leftover);
            break;
        }
    }
    Ok(())
}

fn main() -> Result<(), MyError> {
    let opts: Opts = Opts::parse();
    println!("Using serial file: {}", opts.input_serial);
    println!("Using server file: {}", opts.input_server);

    /* Connect to the database */
    let mut conn = Connection::open(opts.output).unwrap();

    /* Create the tables if needed */
    setup_database(&conn);

    println!("Parsing serial data.");
    handle_serial_data(&mut conn, opts.input_serial)?;
    println!("Parsing server data");
    handle_server_data(&mut conn, opts.input_server)?;
    println!("Vacuuming");
    conn.execute_batch(
        r#"VACUUM;
        "#,
    )?;
    Ok(())
}
