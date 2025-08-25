use std::{error::Error, fs::File, io::{Cursor, Write}, path::{Path, PathBuf}};

use clap::Parser;
use futures_util::TryStreamExt;
use rpki_rewind::{database::Database, utils};


#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {

    /// The output file, e.g. /tmp/rpki-rewind.tar
    #[arg(short, long)]
    output: PathBuf,

    /// The date and time to get state of the RPKI from (e.g. 2025-08-25T12:35:00)
    #[arg(short, long)]
    timestamp: chrono::NaiveDateTime
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let database = Database::new().await;
    let timestamp = args.timestamp.and_utc().timestamp_millis();

    let mut stream = database.retrieve_objects(timestamp).await;

    let path = args.output;
    let file = File::create(&path)?;

    // let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());

    let mut tar = tar::Builder::new(std::io::BufWriter::new(file));

    while let Some(obj) = stream.try_next().await? {
        let file_name = obj.uri.replace("rsync://", "");
        // println!("Adding {}", file_name);

        let mut header = tar::Header::new_gnu();
        header.set_size(obj.content.len() as u64);
        header.set_mode(0o644);
        header.set_mtime((obj.visible_on / 1000) as u64);
        header.set_cksum();

        tar.append_data(&mut header, file_name, Cursor::new(obj.content))?;
    }

    tar.finish()?;
    println!("Done.");

    Ok(())
}