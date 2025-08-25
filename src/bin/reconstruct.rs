use std::{error::Error, fs::File, io::{Cursor, Write}, path::Path};

use futures_util::TryStreamExt;
use rpki_rewind::{database::Database, utils};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database = Database::new().await;
    let timestamp = utils::timestamp();

    let mut stream = database.retrieve_objects(timestamp).await;

    let path = Path::new("/tmp/rpki-rewind.tar");
    let file = File::create(&path)?;

    // let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());

    let mut tar = tar::Builder::new(file);

    while let Some(obj) = stream.try_next().await? {
        let file_name = obj.uri.replace("rsync://", "");
        println!("Adding {}", file_name);

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