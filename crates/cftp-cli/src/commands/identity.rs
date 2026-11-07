use super::*;

pub(crate) fn keygen(args: KeygenArgs) -> Result<()> {
    let paths: SshPaths = SshPaths::user()?;
    let path: PathBuf = args
        .output
        .unwrap_or_else(|| paths.ssh_dir.join("id_ed25519"));
    if path.exists() && !args.force {
        bail!(
            "{} already exists; pass --force to replace it",
            path.display()
        );
    }
    // Generate before touching an existing file, so `--force` cannot leave the
    // user with neither the old key nor a new one.
    let private = generate_keypair()?;
    if path.exists() {
        std::fs::remove_file(&path)
            .with_context(|| format!("failed to replace {}", path.display()))?;
    }
    write_private_key(&path, &private)?;
    let public = private
        .public_key()
        .to_openssh()
        .context("failed to encode the SSH public key")?;
    let mut public_path = path.clone().into_os_string();
    public_path.push(".pub");
    let public_path = PathBuf::from(public_path);
    std::fs::write(&public_path, format!("{public}\n"))
        .with_context(|| format!("failed to write {}", public_path.display()))?;
    let fingerprint = SshPublicKey::from_openssh(&public)?
        .fingerprint()
        .to_string();

    println!("generated SSH key pair");
    println!("  private key: {}", path.display());
    println!("  public key:  {}", public_path.display());
    println!("  fingerprint: {fingerprint}");
    println!();
    println!("Authorize it on the server with:");
    println!(
        "  ssh {user}@<server> 'cat >> ~/.ssh/authorized_keys' < {}",
        public_path.display(),
        user = whoami()
    );
    Ok(())
}

pub(crate) fn pubkey(args: PubkeyArgs) -> Result<()> {
    let paths: SshPaths = SshPaths::user()?;
    let key_path: PathBuf = match args.key {
        Some(path) => path,
        None => paths.existing_client_key().with_context(|| {
            format!(
                "no SSH private key found in {}; create one with `cftp keygen`",
                paths.ssh_dir.display()
            )
        })?,
    };
    let key = load_private_key(&key_path, prompt_passphrase)?;
    println!("{}", key.public_openssh()?);
    Ok(())
}

/// Best-effort local username, used only in suggested commands.
pub(crate) fn whoami() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "<user>".to_string())
}

pub(crate) fn init(args: IdentityInitArgs) -> Result<()> {
    let paths = match args.dir {
        Some(dir) => IdentityPaths::in_dir(dir),
        None => IdentityPaths::user()?,
    };
    let info = init_identity(&paths, args.force)?;
    println!("identity directory: {}", info.paths.dir.display());
    println!("CA certificate:     {}", info.paths.ca.display());
    println!("leaf certificate:   {}", info.paths.cert.display());
    println!("private key:        {}", info.paths.key.display());
    println!("CA SHA-256:         {}", info.ca_fingerprint);
    println!("leaf SHA-256:       {}", info.cert_fingerprint);
    println!();
    println!("Share ca.pem with peers that should trust this identity.");
    Ok(())
}

pub(super) fn prompt_passphrase(path: &Path) -> Result<String> {
    if !std::io::stdin().is_terminal() {
        bail!(
            "SSH key {} is encrypted and stdin is not a terminal; decrypt it or use an unencrypted key",
            path.display()
        );
    }
    rpassword::prompt_password(format!("Passphrase for {}: ", path.display()))
        .with_context(|| format!("failed to read passphrase for {}", path.display()))
}

pub(super) fn confirm_unknown_host(unknown: &UnknownHostKey) -> Result<bool> {
    if !std::io::stdin().is_terminal() {
        bail!(
            "the authenticity of host '{}:{}' can't be established ({}); add the key to known_hosts or pass --accept-new",
            unknown.host,
            unknown.port,
            unknown.fingerprint
        );
    }

    eprintln!(
        "The authenticity of host '{}:{}' can't be established.",
        unknown.host, unknown.port
    );
    eprintln!("Key fingerprint is {}.", unknown.fingerprint);
    eprint!("Are you sure you want to continue connecting (yes/no)? ");
    use std::io::Write as _;
    std::io::stderr().flush()?;

    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "yes" | "y"
    ))
}
