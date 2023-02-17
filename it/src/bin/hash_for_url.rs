use fetch_data::hash_download;
use std::env;

// The only purpose of this executable is to obtain hashes for the data module
pub fn main() {

    // ------------- CHANGE THIS TO THE URL OF YOUR FILE -------------
    let url = "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv";
    // ---------------------------------------------------------------

    let tmp_dir = env::temp_dir();
    let path = tmp_dir.join("file_to_hash");
    let hash = hash_download(url, &path).unwrap();
    println!("--------------------------------------------------------------------------");
    println!(" hash: '{hash}'");
    println!("--------------------------------------------------------------------------");
}
