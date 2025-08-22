pub const ROUTINATOR_URL: &str = "https://routinator.nlnetlabs.nl/api/v1/status";
pub const USER_AGENT: &str = "NLnet Labs RPKI-Rewind";
pub const DOWNLOAD_FOLDER: &str = "/tmp/rpki-rewind/";

/// Update frequency for the RRDP endpoints in seconds
pub const UPDATE_RRDP: u32 = 600;

/// Update frequency for the endpoint in seconds
pub const UPDATE_NOTIFICATION: u32 = 60;

/// The database connection details
pub const DB_POOL: u32  = 10;
pub const DB_USER: &str = "";
pub const DB_PASS: &str = "";
pub const DB_HOST: &str = "localhost";
pub const DB_NAME: &str = "";

