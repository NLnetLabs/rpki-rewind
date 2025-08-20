pub const ROUTINATOR_URL: &str = "https://routinator.nlnetlabs.nl/api/v1/status";
pub const USER_AGENT: &str = "NLnet Labs RPKI-Rewind";

/// Update frequency for the RRDP endpoints in seconds
pub const UPDATE_RRDP: u32 = 600;

/// Update frequency for the endpoint in seconds
pub const UPDATE_NOTIFICATION: u32 = 60;