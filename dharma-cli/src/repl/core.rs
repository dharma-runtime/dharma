use super::context::{Profile, ReplContext};
use super::diff::{diff_values, DiffEntry};
use super::history::structural_counts;
use super::render::{render_json, value_to_json, value_to_text};
use crate::assertion::{add_signer_meta, signer_from_meta, AssertionHeader, AssertionPlaintext};
use crate::crypto;
use crate::identity_store;
use crate::net::policy::{OverlayAccess, OverlayPolicy, PeerClaims};
use crate::net::trust::PeerPolicy;
use crate::pdl::schema::{
    validate_args, ActionSchema, ConcurrencyMode, CqrsSchema, TypeSpec, Visibility,
};
use crate::pkg;
use crate::repl::aliases::{alias_for_subject, save_aliases};
use crate::repl::subjects::recent_subjects;
use crate::runtime::cqrs::{decode_state, default_state_memory, load_state_until, merge_args};
use crate::runtime::vm::RuntimeVm;
use crate::store::index::FrontierIndex;
use crate::store::state::{
    append_assertion, append_overlay, list_assertions, list_overlays, save_snapshot,
    AssertionRecord, Snapshot, SnapshotHeader,
};
use crate::store::Store;
use crate::sync::{
    Get, Hello, Obj, ObjChunkAssembler, ObjectRef, Subscriptions, SyncMessage, CAP_OBJ_CHUNK,
    CAP_SYNC_RANGE,
};
use crate::types::{AssertionId, ContractId, EnvelopeId, SchemaId, SubjectId};
use crate::DharmaError;
use crate::IdentityState;
use ciborium::value::Value;
use crossterm::style::{Color, Stylize};
use dharma::env::Fs;
use indicatif::{ProgressBar, ProgressStyle};
use inquire::{Confirm, Password, Select, Text};
use rand_core::OsRng;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tabled::settings::Style;
use tabled::{Table, Tabled};

const OVERLAY_DISABLED: &str = "overlays.disabled";
const PEERS_FILE: &str = "peers.list";
const DISCOVERY_FILE: &str = "discovery.enabled";
const DHARMAQ_DIR: &str = "dharmaq";
const DHARMAQ_TABLE: &str = "assertions";
const SYNC_TYPE_INV: u8 = 10;
const SYNC_TYPE_GET: u8 = 11;
const SYNC_TYPE_OBJ: u8 = 12;
const SYNC_TYPE_ERR: u8 = 13;
const SYNC_TYPE_AD: u8 = 14;
const SYNC_TYPE_ADS: u8 = 15;
const SYNC_TYPE_GET_ADS: u8 = 16;

pub fn handle_command(ctx: &mut ReplContext, line: &str) -> Result<bool, DharmaError> {
    let parts = normalize_command(&split_command_line(line));
    if parts.is_empty() {
        return Ok(false);
    }
    let parts_ref: Vec<&str> = parts.iter().map(|s| s.as_str()).collect();

    match parts_ref[0] {
        "exit" | "quit" => return Ok(true),
        "help" => handle_help(&parts_ref[1..]),
        "clear" => clear_screen(),
        "version" => print_version(),
        ":set" => handle_set(ctx, &parts_ref[1..])?,
        "config" => handle_config(ctx, &parts_ref[1..])?,
        "identity" => handle_identity(ctx, &parts_ref[1..])?,
        "alias" => handle_alias(ctx, &parts_ref[1..])?,
        "subjects" => handle_subjects(ctx, &parts_ref[1..])?,
        "use" => handle_use(ctx, &parts_ref[1..])?,
        "new" => handle_new(ctx, &parts_ref[1..])?,
        "state" => handle_state(ctx, &parts_ref[1..])?,
        "info" => handle_info(ctx, &parts_ref[1..])?,
        "tail" => handle_tail(ctx, &parts_ref[1..])?,
        "log" => handle_log(ctx, &parts_ref[1..])?,
        "show" => handle_show(ctx, &parts_ref[1..])?,
        "status" => handle_status(ctx, &parts_ref[1..])?,
        "contracts" => handle_contracts(ctx, &parts_ref[1..])?,
        "dryrun" => handle_dryrun(ctx, &parts_ref[1..])?,
        "commit" => handle_commit(ctx, &parts_ref[1..])?,
        "why" => handle_why(ctx, &parts_ref[1..])?,
        "prove" => handle_prove(ctx, &parts_ref[1..])?,
        "authority" => handle_authority(ctx, &parts_ref[1..])?,
        "diff" => handle_diff(ctx, &parts_ref[1..])?,
        "pkg" => handle_pkg(ctx, &parts_ref[1..])?,
        "overlay" => handle_overlay(ctx, &parts_ref[1..])?,
        "peers" => handle_peers(ctx, &parts_ref[1..])?,
        "connect" => handle_connect(ctx, &parts_ref[1..])?,
        "sync" => handle_sync(ctx, &parts_ref[1..])?,
        "discover" => handle_discover(ctx, &parts_ref[1..])?,
        "index" => handle_index(ctx, &parts_ref[1..])?,
        "find" => handle_find(ctx, &parts_ref[1..])?,
        "q" => handle_q(ctx, &parts_ref[1..])?,
        "open" => handle_open(ctx, &parts_ref[1..])?,
        "tables" => handle_tables(ctx, &parts_ref[1..])?,
        "table" => handle_table(ctx, &parts_ref[1..])?,
        "pwd" => print_pwd(ctx),
        _ => println!("Unknown command: '{}'. Type 'help' for info.", parts_ref[0]),
    }

    Ok(false)
}

pub(crate) fn split_command_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
            }
            continue;
        }
        if in_single {
            if ch == '\'' {
                in_single = false;
            } else {
                current.push(ch);
            }
            continue;
        }
        if in_double {
            if ch == '"' {
                in_double = false;
            } else {
                current.push(ch);
            }
            continue;
        }
        match ch {
            '\'' => in_single = true,
            '"' => in_double = true,
            _ if ch.is_whitespace() => {
                if !current.is_empty() {
                    out.push(current);
                    current = String::new();
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

fn normalize_command(parts: &[String]) -> Vec<String> {
    if parts.is_empty() {
        return Vec::new();
    }
    let mut out = parts.to_vec();
    match out[0].as_str() {
        "id" => {
            out[0] = "identity".to_string();
        }
        "conf" => {
            out[0] = "config".to_string();
        }
        "ct" => {
            out[0] = "contracts".to_string();
            if out.len() >= 2 {
                match out[1].as_str() {
                    "ls" => {
                        out.remove(1);
                    }
                    "info" | "schema" | "actions" | "reactors" => {}
                    _ => {}
                }
            }
        }
        "ls" => {
            out[0] = "subjects".to_string();
            if out.len() >= 2 && out[1] == "c" {
                out[1] = "contract".to_string();
            }
        }
        "net" => {
            if out.len() >= 2 {
                let sub = out.remove(1);
                out[0] = match sub.as_str() {
                    "peers" => "peers".to_string(),
                    "connect" => "connect".to_string(),
                    "sync" => "sync".to_string(),
                    other => other.to_string(),
                };
            }
        }
        "do" => {
            out[0] = "commit".to_string();
            out.insert(1, "action".to_string());
        }
        "try" => {
            out[0] = "dryrun".to_string();
            out.insert(1, "action".to_string());
        }
        "can" => {
            out[0] = "authority".to_string();
        }
        "alias" => {
            if out.len() >= 2 && out[1] == "ls" {
                out[1] = "list".to_string();
            }
        }
        "pkg" => {
            if out.len() >= 2 {
                match out[1].as_str() {
                    "ls" | "installed" => out[1] = "list".to_string(),
                    "uninstall" => out[1] = "remove".to_string(),
                    _ => {}
                }
            }
        }
        _ => {}
    }
    out
}

fn print_help_section(title: &str, entries: &[(&str, &str)]) {
    println!("{title}");
    println!("-----------------");
    let width = entries.iter().map(|(cmd, _)| cmd.len()).max().unwrap_or(0) + 2;
    for (cmd, desc) in entries {
        println!("{cmd:<width$}{desc}", width = width);
    }
}

fn print_help_identity() {
    let entries = [
        ("id init <name> <email> <password>", "Initialize identity"),
        ("id status", "Show identity status"),
        ("id unlock <password>", "Unlock identity"),
        ("id lock", "Lock identity"),
        ("id whoami", "Show identity information"),
        ("id export", "Export identity"),
    ];
    print_help_section("IDENTITY", &entries);
}

fn print_help_alias() {
    let entries = [
        ("alias set <name> <value>", "Set alias"),
        ("alias rm <name>", "Remove alias"),
        ("alias ls", "List aliases"),
    ];
    print_help_section("ALIAS", &entries);
}

fn print_help_config() {
    let entries = [("conf show", "Show resolved config")];
    print_help_section("CONFIG", &entries);
}

fn print_help_contracts() {
    let entries = [
        ("ct ls", "List contracts"),
        ("ct info <contract>", "Show contract information"),
        ("ct schema <contract>", "Show contract schema"),
        ("ct actions <contract>", "Show contract actions"),
        ("ct reactors <contract>", "Show contract reactors"),
    ];
    print_help_section("CONTRACTS", &entries);
}

fn print_help_subjects() {
    let entries = [
        ("ls", "List subjects"),
        ("ls recent", "List recent subjects"),
        ("ls mine", "List subjects owned by current identity"),
        ("ls c <contract>", "List subjects for contract"),
    ];
    print_help_section("SUBJECTS", &entries);
}

fn print_help_subject() {
    let entries = [
        ("new <contract>", "Create subject for contract"),
        ("do <Action> [k=v...]", "Perform an action on the subject"),
        ("try <Action> [k=v...]", "Simulate an action"),
        (
            "can",
            "Show all allowed actions on subject and their arguments",
        ),
        (
            "can <Action> [k=v...]",
            "Check if action is allowed on subject",
        ),
        (
            "why",
            "Explain the current state. Show history, who, what, when",
        ),
        ("why <path>", "Explain a state field"),
        ("prove", "Prove latest assertion"),
        ("prove <assertion_id>", "Prove a specific assertion"),
        ("state", "Show subject state"),
        ("status", "Show subject status"),
        ("diff", "Compare current and previous state"),
        ("diff --at <idA> <idB>", "Compare two states"),
    ];
    print_help_section("SUBJECT", &entries);
}

fn print_help_db() {
    let entries = [
        ("tables", "List tables"),
        (
            "table <table>",
            "Show table information: fields, number of rows.",
        ),
        ("q <query pipeline>", "Execute a query pipeline"),
        ("find \"<query>\"", "Find rows matching a query"),
        ("index [status|build|drop]", "Manage DHARMA-Q indexes"),
        ("open <result_id_or_object_id>", "Open a search result"),
    ];
    print_help_section("DB", &entries);
}

fn print_help_pkg_section() {
    let entries = [
        ("pkg ls", "List packages"),
        ("pkg search", "Search for packages"),
        ("pkg local", "List local packages"),
        ("pkg installed", "List installed packages"),
        ("pkg show <package_name>", "Show package information"),
        ("pkg install <package_name>", "Install a package"),
        ("pkg uninstall <package_name>", "Uninstall a package"),
        ("pkg verify", "Verify package integrity"),
        ("pkg pin <package_name>", "Pin a package"),
        ("pkg build <path>", "Build a package"),
        ("pkg publish <package_name>", "Publish a package"),
    ];
    print_help_section("PACKAGE MANAGEMENT", &entries);
}

fn print_help_network() {
    let entries = [
        ("peers [--json|--verbose]", "List known peers"),
        ("connect <addr> [--verbose]", "Connect + sync with peer"),
        ("sync now | sync subject [id] [--verbose]", "Trigger sync"),
    ];
    print_help_section("NETWORK", &entries);
}

fn print_help_session() {
    let entries = [
        ("tail [n]", "Show recent assertions"),
        ("log [n]", "Show verbose history"),
        ("show <id> [--json|--raw]", "Show assertion/envelope"),
        (
            "overlay <status|list|enable|disable|show>",
            "Overlay view controls",
        ),
        ("pwd", "Show current context"),
        ("version", "Show build info"),
        ("help", "Show help"),
        ("help <command>", "Show help for a specific command"),
        ("exit", "Exit"),
    ];
    print_help_section("SESSION", &entries);
}

fn print_help() {
    print_help_identity();
    println!();
    print_help_alias();
    println!();
    print_help_config();
    println!();
    print_help_contracts();
    println!();
    print_help_subjects();
    println!();
    print_help_subject();
    println!();
    print_help_db();
    println!();
    print_help_pkg_section();
    println!();
    print_help_network();
    println!();
    print_help_session();
}

fn handle_help(args: &[&str]) {
    if args.is_empty() {
        print_help();
        return;
    }
    match args[0] {
        "id" | "identity" => print_help_identity(),
        "alias" => print_help_alias(),
        "conf" | "config" => print_help_config(),
        "ct" | "contracts" => print_help_contracts(),
        "ls" | "subjects" => print_help_subjects(),
        "new" | "use" | "do" | "try" | "can" | "why" | "prove" | "state" | "status" | "diff"
        | "commit" | "dryrun" | "authority" | "info" => print_help_subject(),
        "tables" | "table" | "q" | "find" | "index" | "open" => print_help_db(),
        "pkg" => print_help_pkg_section(),
        "peers" | "connect" | "sync" | "net" => print_help_network(),
        "tail" | "log" | "show" | "overlay" | "pwd" | "version" | "help" | "exit" => {
            print_help_session()
        }
        _ => print_help(),
    }
}

fn print_version() {
    let version = crate::APP_VERSION;
    println!("DHARMA REPL v{version}");
    println!("features: compiler={}", cfg!(feature = "compiler"));
}

fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
    let _ = io::stdout().flush();
}

fn handle_config(_ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() || args[0] == "show" {
        let root = std::env::current_dir()?;
        let config = dharma::config::Config::load(&root)?;
        println!("{}", config.to_toml_string());
        return Ok(());
    }
    println!("Usage: config show");
    Ok(())
}

fn print_pwd(ctx: &ReplContext) {
    if let Some(subject) = &ctx.current_subject {
        if let Some(alias) = ctx.alias_for_subject(subject) {
            println!("Subject: {} ({})", alias, subject.to_hex());
        } else {
            println!("Subject: {}", subject.to_hex());
        }
    } else {
        println!("Subject: <none>");
    }
    println!("Lens: {}", ctx.current_lens);
    println!("Profile: {:?}", ctx.profile);
}

fn handle_set(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("profile = {:?}", ctx.profile);
        println!("json = {}", ctx.json);
        println!("color = {}", ctx.color);
        println!("confirmations = {}", ctx.confirmations);
        return Ok(());
    }
    match args[0] {
        "profile" => {
            if args.len() < 2 {
                println!("Current: {:?}", ctx.profile);
            } else {
                match args[1] {
                    "home" => ctx.profile = Profile::Home,
                    "pro" => ctx.profile = Profile::Pro,
                    "highsec" => ctx.profile = Profile::HighSec,
                    _ => println!("Invalid profile. Choices: home, pro, highsec"),
                }
            }
        }
        "json" => ctx.json = parse_bool_setting(args)?,
        "color" => ctx.color = parse_bool_setting(args)?,
        "confirmations" => ctx.confirmations = parse_bool_setting(args)?,
        _ => println!("Unknown setting: {}", args[0]),
    }
    Ok(())
}

fn parse_bool_setting(args: &[&str]) -> Result<bool, DharmaError> {
    if args.len() < 2 {
        return Err(DharmaError::Validation("missing value".to_string()));
    }
    match args[1] {
        "on" | "true" | "yes" => Ok(true),
        "off" | "false" | "no" => Ok(false),
        _ => Err(DharmaError::Validation("expected on/off".to_string())),
    }
}

fn handle_identity(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    if args.is_empty() || args[0] == "status" {
        return identity_status(ctx);
    }

    match args[0] {
        "init" => {
            if args.len() < 2 {
                println!("Usage: identity init <alias> [email] [password]");
                return Ok(());
            }
            let alias = args[1];
            let passphrase = if args.len() >= 4 {
                args[3].to_string()
            } else if args.len() >= 3 {
                args[2].to_string()
            } else {
                prompt_password("Password: ")?
            };
            match identity_store::init_identity(&env, alias, &passphrase)? {
                Some(identity) => {
                    ctx.identity = Some(identity.clone());
                    ctx.current_subject = Some(identity.subject_id);
                    ctx.current_alias = Some(alias.to_string());
                    ctx.aliases.insert(alias.to_string(), identity.subject_id);
                    let _ = save_aliases(&ctx.data_dir, &ctx.aliases);
                    println!("Created identity.");
                }
                None => println!("Status: Already initialized."),
            }
        }
        "unlock" => {
            if identity_store::ensure_identity_present(&env).is_err() {
                return Ok(());
            }
            let passphrase = if args.len() >= 2 {
                args[1].to_string()
            } else {
                prompt_password("Password: ")?
            };
            let identity = identity_store::load_identity(&env, &passphrase)?;
            ctx.current_subject = Some(identity.subject_id);
            ctx.current_alias = ctx.alias_for_subject(&identity.subject_id);
            ctx.identity = Some(identity);
            println!("Identity unlocked.");
        }
        "lock" => {
            ctx.identity = None;
            println!("Identity locked.");
        }
        "whoami" => {
            if let Some(id) = &ctx.identity {
                println!("Subject: {}", id.subject_id.to_hex());
                println!("Root Pubkey: {}", id.root_public_key.to_hex());
                println!("Device Pubkey: {}", id.public_key.to_hex());
                let env = dharma::env::StdEnv::new(&ctx.data_dir);
                if let Ok(Some(handle)) = identity_store::read_local_handle(&env) {
                    println!("Local Handle: {handle}");
                }
            } else {
                println!("Not logged in.");
            }
        }
        "export" => {
            if ctx.identity.is_none() {
                println!("Identity locked. Run 'identity unlock' first.");
                return Ok(());
            }
            if ctx.confirmations && !confirm("Reveal paper key?")? {
                println!("Aborted.");
                return Ok(());
            }
            let secret = ctx
                .identity
                .as_ref()
                .expect("checked")
                .signing_key
                .to_bytes();
            let paper_key = crate::types::hex_encode(secret);
            println!("{paper_key}");
        }
        _ => println!("Unknown identity command"),
    }
    Ok(())
}

fn identity_status(ctx: &ReplContext) -> Result<(), DharmaError> {
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    if !identity_store::identity_exists(&env) {
        println!("Status: Uninitialized. Run 'dh identity init <name>'");
        return Ok(());
    }
    if let Some(identity) = &ctx.identity {
        println!("Status: Unlocked");
        println!("Subject: {}", identity.subject_id.to_hex());
        println!("Root Pubkey: {}", identity.root_public_key.to_hex());
        println!("Device Pubkey: {}", identity.public_key.to_hex());
        if let Ok(Some(handle)) = identity_store::read_local_handle(&env) {
            println!("Local Handle: {handle}");
        }
        return Ok(());
    }
    println!("Status: Locked");
    if let Ok(subject) = identity_store::read_identity_subject(&env) {
        println!("Subject: {}", subject.to_hex());
    }
    if let Ok(Some(handle)) = identity_store::read_local_handle(&env) {
        println!("Local Handle: {handle}");
    }
    Ok(())
}

fn handle_alias(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() || args[0] == "list" {
        if ctx.aliases.is_empty() {
            println!("No aliases.");
        } else {
            for (alias, subject) in ctx.aliases.iter() {
                println!("{alias} = {}", subject.to_hex());
            }
        }
        return Ok(());
    }

    match args[0] {
        "set" => {
            if args.len() < 2 {
                println!("Usage: alias set <name> [subject]");
                return Ok(());
            }
            let alias = args[1].to_string();
            let subject = if args.len() >= 3 {
                resolve_subject(ctx, args[2])?
            } else if let Some(current) = ctx.current_subject {
                current
            } else {
                println!("No current subject. Provide a subject id.");
                return Ok(());
            };
            ctx.aliases.insert(alias.clone(), subject);
            save_aliases(&ctx.data_dir, &ctx.aliases)?;
            println!("Alias set: {alias} -> {}", subject.to_hex());
        }
        "rm" => {
            if args.len() < 2 {
                println!("Usage: alias rm <name>");
                return Ok(());
            }
            let removed = ctx.aliases.remove(args[1]);
            save_aliases(&ctx.data_dir, &ctx.aliases)?;
            if removed.is_some() {
                println!("Alias removed.");
            } else {
                println!("Alias not found.");
            }
        }
        _ => println!("Unknown alias command"),
    }
    Ok(())
}

fn handle_subjects(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let store = Store::from_root(&ctx.data_dir);
    if args.is_empty() {
        let subjects = store.list_subjects()?;
        if subjects.is_empty() {
            println!("No subjects.");
            return Ok(());
        }
        print_subjects_table(ctx, &subjects);
        return Ok(());
    }

    match args[0] {
        "recent" => {
            let limit = args
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(10);
            let subjects = recent_subjects(&ctx.data_dir, limit)?;
            if subjects.is_empty() {
                println!("No recent subjects.");
            } else {
                print_subjects_table(ctx, &subjects);
            }
        }
        "mine" => {
            if let Some(identity) = &ctx.identity {
                print_subjects_table(ctx, &[identity.subject_id]);
                return Ok(());
            }
            let env = dharma::env::StdEnv::new(&ctx.data_dir);
            if let Ok(subject) = identity_store::read_identity_subject(&env) {
                print_subjects_table(ctx, &[subject]);
            } else {
                println!("No identity.");
            }
        }
        "contract" => {
            if args.len() < 2 {
                println!("Usage: subjects contract <contract>");
                return Ok(());
            }
            let subjects = subjects_for_contract(ctx, args[1])?;
            if subjects.is_empty() {
                println!("No subjects.");
            } else {
                print_subjects_table(ctx, &subjects);
            }
        }
        _ => println!("Unknown subjects command"),
    }
    Ok(())
}

fn handle_use(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        if !io::stdin().is_terminal() {
            print_pwd(ctx);
            return Ok(());
        }
        let Some(subject) = select_subject_interactive(ctx)? else {
            return Ok(());
        };
        ctx.current_subject = Some(subject);
        ctx.current_alias = ctx.alias_for_subject(&subject);
        if let Some(alias) = &ctx.current_alias {
            println!("Using {alias} ({})", subject.to_hex());
        } else {
            println!("Using {}", subject.to_hex());
        }
        return Ok(());
    }
    let subject = resolve_subject(ctx, args[0])?;
    ctx.current_subject = Some(subject);
    ctx.current_alias = ctx.alias_for_subject(&subject);
    if let Some(alias) = &ctx.current_alias {
        println!("Using {alias} ({})", subject.to_hex());
    } else {
        println!("Using {}", subject.to_hex());
    }
    Ok(())
}

fn handle_new(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: new <contract>");
        return Ok(());
    }
    let contract_name = args[0];
    let entry = resolve_contract_by_name(ctx, contract_name)?;
    update_config_for_lens(
        &ctx.data_dir,
        &entry.schema_id,
        &entry.contract_id,
        entry.reactor_id.as_ref(),
        ctx.current_lens,
    )?;
    let subject = SubjectId::random(&mut OsRng);
    ctx.current_subject = Some(subject);
    ctx.current_alias = ctx.alias_for_subject(&subject);
    println!("{}", subject.to_hex());
    Ok(())
}

fn resolve_subject(ctx: &ReplContext, token: &str) -> Result<SubjectId, DharmaError> {
    if token == "mine" {
        if let Some(identity) = &ctx.identity {
            return Ok(identity.subject_id);
        }
        let env = dharma::env::StdEnv::new(&ctx.data_dir);
        if let Ok(subject) = identity_store::read_identity_subject(&env) {
            return Ok(subject);
        }
        return Err(DharmaError::Validation("no identity".to_string()));
    }
    if let Ok(subject) = SubjectId::from_hex(token) {
        return Ok(subject);
    }
    if let Some(subject) = ctx.aliases.get(token) {
        return Ok(*subject);
    }
    Err(DharmaError::Validation("unknown subject".to_string()))
}

fn subjects_for_contract(ctx: &ReplContext, contract: &str) -> Result<Vec<SubjectId>, DharmaError> {
    let entry = resolve_contract_by_name(ctx, contract)?;
    let store = Store::from_root(&ctx.data_dir);
    let mut out = Vec::new();
    for subject in store.list_subjects()? {
        let records = list_assertions(store.env(), &subject)?;
        if records.iter().any(|record| {
            if let Ok(assertion) = AssertionPlaintext::from_cbor(&record.bytes) {
                assertion.header.schema == entry.schema_id
            } else {
                false
            }
        }) {
            out.push(subject);
        }
    }
    Ok(out)
}

fn handle_state(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let subject = current_subject_or_identity(ctx)?;
    let (json, raw, lens, stop_at) = parse_state_args(ctx, args)?;
    let (schema_id, contract_id) = load_contract_ids_for_ver(&ctx.data_dir, lens)?;
    let schema_bytes = load_schema_bytes(&ctx.data_dir, &schema_id)?;
    let contract_bytes = load_contract_bytes(&ctx.data_dir, &contract_id)?;
    let schema = crate::pdl::schema::CqrsSchema::from_cbor(&schema_bytes)?;

    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let state = load_state_until(&env, &subject, &schema, &contract_bytes, lens, stop_at)?;
    let mut value = decode_state(&state.memory, &schema)?;
    let disabled = load_overlay_disabled(&ctx.data_dir)?;
    if !overlay_enabled_for_schema(&schema, &disabled) {
        value = filter_private_fields(&schema, &value)?;
    }
    if raw {
        let bytes = crate::cbor::encode_canonical_value(&value)?;
        println!("{}", hex_bytes(&bytes));
    } else if json {
        println!("{}", render_json(&value, ctx.color));
    } else {
        for line in value_to_text(&value) {
            println!("{line}");
        }
    }
    Ok(())
}

fn handle_info(ctx: &mut ReplContext, _args: &[&str]) -> Result<(), DharmaError> {
    handle_status(ctx, &["--verbose"])?;
    handle_state(ctx, &[])?;
    Ok(())
}

fn handle_contracts(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        let contracts = collect_contracts(ctx)?;
        if contracts.is_empty() {
            println!("No contracts found.");
            return Ok(());
        }
        print_contracts_table(&contracts);
        return Ok(());
    }

    match args[0] {
        "ls" | "list" => {
            let contracts = collect_contracts(ctx)?;
            if contracts.is_empty() {
                println!("No contracts found.");
                return Ok(());
            }
            print_contracts_table(&contracts);
        }
        "schema" => {
            if args.len() < 2 {
                println!("Usage: contracts schema <contract>");
                return Ok(());
            }
            let entry = resolve_contract_by_name(ctx, args[1])?;
            print_contract_schema(&entry);
        }
        "actions" => {
            if args.len() < 2 {
                println!("Usage: contracts actions <contract>");
                return Ok(());
            }
            let entry = resolve_contract_by_name(ctx, args[1])?;
            print_contract_actions(&entry);
        }
        "info" => {
            if args.len() < 2 {
                println!("Usage: contracts info <contract>");
                return Ok(());
            }
            let entry = resolve_contract_by_name(ctx, args[1])?;
            print_contract_schema(&entry);
            print_contract_actions(&entry);
        }
        "reactors" => {
            if args.len() < 2 {
                println!("Usage: contracts reactors <contract>");
                return Ok(());
            }
            let entry = resolve_contract_by_name(ctx, args[1])?;
            if let Some(reactor) = entry.reactor_id {
                println!("{}", reactor.to_hex());
            } else {
                println!("No reactor.");
            }
        }
        _ => println!("Unknown contracts command"),
    }
    Ok(())
}

fn parse_state_args(
    ctx: &ReplContext,
    args: &[&str],
) -> Result<(bool, bool, u64, Option<AssertionId>), DharmaError> {
    let mut json = ctx.json;
    let mut raw = false;
    let mut lens = ctx.current_lens;
    let mut stop_at = None;
    let mut iter = args.iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            json = true;
            continue;
        }
        if arg == "--raw" {
            raw = true;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--lens=") {
            if let Ok(parsed) = value.parse::<u64>() {
                lens = parsed;
            }
            continue;
        }
        if arg == "--lens" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = value.parse::<u64>() {
                    lens = parsed;
                }
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--at=") {
            stop_at = Some(resolve_assertion_id_for_subject(ctx, value)?);
            continue;
        }
        if arg == "--at" {
            if let Some(value) = iter.next() {
                stop_at = Some(resolve_assertion_id_for_subject(ctx, value)?);
            }
            continue;
        }
    }
    Ok((json, raw, lens, stop_at))
}

fn handle_tail(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let subject = current_subject_or_identity(ctx)?;
    let count = args
        .get(0)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let records = list_assertions(&env, &subject)?;
    if records.is_empty() {
        println!("No assertions.");
        return Ok(());
    }
    let start = records.len().saturating_sub(count);
    for record in &records[start..] {
        if let Ok(assertion) = crate::assertion::AssertionPlaintext::from_cbor(&record.bytes) {
            println!(
                "#{:>4} {} {}",
                record.seq,
                assertion.header.typ,
                record.assertion_id.to_hex()
            );
        } else {
            println!("#{:>4} {}", record.seq, record.assertion_id.to_hex());
        }
    }
    Ok(())
}

fn handle_log(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let subject = current_subject_or_identity(ctx)?;
    let count = args
        .get(0)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let records = list_assertions(&env, &subject)?;
    if records.is_empty() {
        println!("No assertions.");
        return Ok(());
    }
    let start = records.len().saturating_sub(count);
    for record in &records[start..] {
        println!("#{:>4} {}", record.seq, record.assertion_id.to_hex());
        if let Ok(assertion) = crate::assertion::AssertionPlaintext::from_cbor(&record.bytes) {
            print_assertion_header(&assertion);
        } else {
            println!("  <unable to decode>");
        }
    }
    Ok(())
}

fn handle_show(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let (object_token, json, raw) = parse_show_args(args)?;
    if object_token.is_none() {
        println!("Usage: show <id> [--json|--raw]");
        return Ok(());
    }
    let store = Store::from_root(&ctx.data_dir);
    let envelope_id = resolve_envelope_id_any(&store, object_token.expect("checked"))?;
    let bytes = store.get_object(&envelope_id)?;
    if raw {
        println!("{}", hex_bytes(&bytes));
        return Ok(());
    }
    if let Ok(assertion) = crate::assertion::AssertionPlaintext::from_cbor(&bytes) {
        if json {
            println!("{}", render_json(&assertion.to_value(), ctx.color));
        } else {
            print_assertion(&assertion);
        }
        return Ok(());
    }
    if let Ok(envelope) = crate::envelope::AssertionEnvelope::from_cbor(&bytes) {
        if json {
            println!("{}", render_json(&envelope.to_value(), ctx.color));
            return Ok(());
        }
        println!("Envelope:");
        println!("  v: {}", envelope.v);
        println!("  suite: {}", envelope.suite);
        println!("  kid: {}", envelope.kid.to_hex());
        println!(
            "  nonce: {}",
            crate::types::hex_encode(*envelope.nonce.as_bytes())
        );
        println!("  ct_len: {}", envelope.ct.len());
        if let Some(identity) = &ctx.identity {
            if let Ok(plaintext) =
                crate::envelope::decrypt_assertion(&envelope, &identity.subject_key)
            {
                if let Ok(assertion) = crate::assertion::AssertionPlaintext::from_cbor(&plaintext) {
                    println!("Decrypted:");
                    print_assertion(&assertion);
                }
            }
        }
        return Ok(());
    }
    println!("Unrecognized object format.");
    Ok(())
}

fn handle_status(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let verbose = args.iter().any(|arg| *arg == "--verbose");
    let store = Store::from_root(&ctx.data_dir);
    let index = FrontierIndex::new(&ctx.data_dir)?;
    if let Some(subject) = ctx
        .current_subject
        .or_else(|| ctx.identity.as_ref().map(|id| id.subject_id))
    {
        print_subject_status(ctx, &store, &index, &subject, verbose)?;
    } else {
        println!("No subject selected.");
    }
    Ok(())
}

fn handle_dryrun(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.len() < 2 || args[0] != "action" {
        println!("Usage: dryrun action <Action> [k=v...] [--json]");
        return Ok(());
    }
    let action = args[1];
    let (flags, arg_values) = extract_action_flags(ctx, &args[2..])?;
    let identity = ctx
        .identity
        .as_ref()
        .ok_or_else(|| DharmaError::Validation("identity locked".to_string()))?;
    let plan = prepare_action(ctx, action, &arg_values, flags.lens)?;
    let preview = simulate_action(identity, &plan)?;
    if flags.json {
        println!("{}", value_to_json(&preview.to_value()));
    } else {
        print_action_preview(&preview, ctx.color);
    }
    Ok(())
}

fn handle_commit(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.len() < 2 || args[0] != "action" {
        println!("Usage: commit action <Action> [k=v...] [--json] [--force]");
        return Ok(());
    }
    let action = args[1];
    let (flags, arg_values) = extract_action_flags(ctx, &args[2..])?;
    let identity = ctx
        .identity
        .as_ref()
        .ok_or_else(|| DharmaError::Validation("identity locked".to_string()))?;
    let plan = prepare_action(ctx, action, &arg_values, flags.lens)?;
    let preview = simulate_action(identity, &plan)?;
    if !preview.allowed {
        if flags.json {
            println!("{}", value_to_json(&preview.to_value()));
        } else {
            print_action_preview(&preview, ctx.color);
        }
        return Err(DharmaError::Validation("contract rejected".to_string()));
    }
    let needs_confirm = ctx.confirmations || ctx.profile == Profile::HighSec;
    if needs_confirm && !flags.force {
        print_transaction_card(&plan, &preview, ctx.color);
        if !confirm("Commit?")? {
            println!("Aborted.");
            return Ok(());
        }
    }
    if ctx.profile == Profile::HighSec && flags.force {
        return Err(DharmaError::Validation(
            "force disabled in highsec profile".to_string(),
        ));
    }
    let committed = commit_action(identity, &plan)?;
    if flags.json {
        println!("{}", value_to_json(&committed));
    } else {
        println!("Committed assertions:");
        for id in committed_ids(&committed) {
            println!("  {}", id.to_hex());
        }
    }
    Ok(())
}

fn handle_authority(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        let lens = ctx.current_lens;
        let (schema_id, _contract_id) = load_contract_ids_for_ver(&ctx.data_dir, lens)?;
        let schema_bytes = load_schema_bytes(&ctx.data_dir, &schema_id)?;
        let schema = CqrsSchema::from_cbor(&schema_bytes)?;
        print_schema_actions(&schema);
        return Ok(());
    }
    let (action, rest) = if args[0] == "typ" {
        if args.len() < 2 {
            println!("Usage: authority typ <typ> [--json]");
            return Ok(());
        }
        if let Some(action) = args[1].strip_prefix("action.") {
            (action, &args[2..])
        } else {
            println!("Unknown action type.");
            return Ok(());
        }
    } else {
        (args[0], &args[1..])
    };
    let (flags, arg_values) = extract_action_flags(ctx, rest)?;
    let identity = ctx
        .identity
        .as_ref()
        .ok_or_else(|| DharmaError::Validation("identity locked".to_string()))?;
    let plan = prepare_action(ctx, action, &arg_values, flags.lens)?;
    let preview = simulate_action(identity, &plan)?;
    let mut report = Vec::new();
    report.push((
        Value::Text("allowed".to_string()),
        Value::Bool(preview.allowed),
    ));
    if let Some(reason) = preview.reason.clone() {
        report.push((Value::Text("reason".to_string()), Value::Text(reason)));
    }
    report.push((
        Value::Text("action".to_string()),
        Value::Text(action.to_string()),
    ));
    report.push((
        Value::Text("subject".to_string()),
        Value::Bytes(plan.subject.as_bytes().to_vec()),
    ));
    if flags.json {
        println!(
            "{}",
            value_to_json(&Value::Map(report.into_iter().collect()))
        );
    } else {
        println!("Allowed: {}", preview.allowed);
        if let Some(reason) = preview.reason {
            println!("Reason: {reason}");
        }
    }
    Ok(())
}

fn handle_prove(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let mut token = None;
    let mut json = false;
    for arg in args {
        if *arg == "--json" {
            json = true;
        } else if token.is_none() {
            token = Some(*arg);
        }
    }
    let token = match token {
        Some(token) => token.to_string(),
        None => {
            let subject = current_subject_or_identity(ctx)?;
            let env = dharma::env::StdEnv::new(&ctx.data_dir);
            let records = list_assertions(&env, &subject)?;
            let Some(record) = records.last() else {
                println!("No assertions.");
                return Ok(());
            };
            record.assertion_id.to_hex()
        }
    };
    let store = Store::from_root(&ctx.data_dir);
    let envelope_id = resolve_envelope_id_any(&store, &token)?;
    let bytes = store.get_object(&envelope_id)?;
    let report = prove_object(ctx, &store, &bytes)?;
    if json {
        println!("{}", value_to_json(&report));
    } else {
        print_proof_report(&report);
    }
    Ok(())
}

fn handle_diff(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let subject = current_subject_or_identity(ctx)?;
    let has_at = args.iter().any(|arg| *arg == "--at");
    let (lens_a, lens_b, at_a, at_b, json) = if has_at {
        let (diff_opts, json) = parse_diff_args(args, ctx.current_lens)?;
        let at_a = diff_opts
            .at
            .0
            .as_deref()
            .map(|token| resolve_assertion_id_for_subject(ctx, token))
            .transpose()?;
        let at_b = diff_opts
            .at
            .1
            .as_deref()
            .map(|token| resolve_assertion_id_for_subject(ctx, token))
            .transpose()?;
        (diff_opts.lens.0, diff_opts.lens.1, at_a, at_b, json)
    } else {
        let (lens_a, lens_b, json) = parse_diff_default_args(args, ctx.current_lens)?;
        let env = dharma::env::StdEnv::new(&ctx.data_dir);
        let records = list_assertions(&env, &subject)?;
        if records.len() < 2 {
            println!("Not enough assertions to diff.");
            return Ok(());
        }
        let at_a = records[records.len() - 2].assertion_id;
        let at_b = records[records.len() - 1].assertion_id;
        (lens_a, lens_b, Some(at_a), Some(at_b), json)
    };
    let (schema_a, contract_a) = load_contract_ids_for_ver(&ctx.data_dir, lens_a)?;
    let (schema_b, contract_b) = load_contract_ids_for_ver(&ctx.data_dir, lens_b)?;
    let schema_bytes_a = load_schema_bytes(&ctx.data_dir, &schema_a)?;
    let schema_bytes_b = load_schema_bytes(&ctx.data_dir, &schema_b)?;
    let contract_bytes_a = load_contract_bytes(&ctx.data_dir, &contract_a)?;
    let contract_bytes_b = load_contract_bytes(&ctx.data_dir, &contract_b)?;
    let schema_a = CqrsSchema::from_cbor(&schema_bytes_a)?;
    let schema_b = CqrsSchema::from_cbor(&schema_bytes_b)?;
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let state_a = load_state_until(&env, &subject, &schema_a, &contract_bytes_a, lens_a, at_a)?;
    let state_b = load_state_until(&env, &subject, &schema_b, &contract_bytes_b, lens_b, at_b)?;
    let mut value_a = decode_state(&state_a.memory, &schema_a)?;
    let mut value_b = decode_state(&state_b.memory, &schema_b)?;
    let disabled = load_overlay_disabled(&ctx.data_dir)?;
    if !overlay_enabled_for_schema(&schema_a, &disabled) {
        value_a = filter_private_fields(&schema_a, &value_a)?;
    }
    if !overlay_enabled_for_schema(&schema_b, &disabled) {
        value_b = filter_private_fields(&schema_b, &value_b)?;
    }
    let diff = diff_values(&value_a, &value_b);
    if json {
        println!("{}", value_to_json(&diff_to_value(&diff)));
    } else {
        print_diff(&diff, ctx.color);
    }
    Ok(())
}

fn handle_why(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Current state:");
        handle_state(ctx, &[])?;
        let subject = current_subject_or_identity(ctx)?;
        let env = dharma::env::StdEnv::new(&ctx.data_dir);
        let records = list_assertions(&env, &subject)?;
        if records.is_empty() {
            println!("No assertions.");
            return Ok(());
        }
        println!();
        println!("History:");
        let mut rows = Vec::new();
        for record in records {
            if let Ok(assertion) = crate::assertion::AssertionPlaintext::from_cbor(&record.bytes) {
                let action = assertion
                    .header
                    .typ
                    .strip_prefix("action.")
                    .unwrap_or(&assertion.header.typ)
                    .to_string();
                let ts = assertion
                    .header
                    .ts
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".to_string());
                rows.push(WhyRow {
                    seq: record.seq,
                    assertion: record.assertion_id.to_hex(),
                    action,
                    author: assertion.header.auth.to_hex(),
                    time: ts,
                });
            } else {
                rows.push(WhyRow {
                    seq: record.seq,
                    assertion: record.assertion_id.to_hex(),
                    action: "<unreadable>".to_string(),
                    author: "<unknown>".to_string(),
                    time: "n/a".to_string(),
                });
            }
        }
        println!("{}", Table::new(rows).with(Style::rounded()).to_string());
        return Ok(());
    }
    let path = args[0];
    let mut lens = ctx.current_lens;
    let mut iter = args[1..].iter().copied();
    while let Some(arg) = iter.next() {
        if let Some(value) = arg.strip_prefix("--lens=") {
            if let Ok(parsed) = value.parse::<u64>() {
                lens = parsed;
            }
            continue;
        }
        if arg == "--lens" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = value.parse::<u64>() {
                    lens = parsed;
                }
            }
        }
    }
    let subject = current_subject_or_identity(ctx)?;
    let (schema_id, contract_id) = load_contract_ids_for_ver(&ctx.data_dir, lens)?;
    let schema_bytes = load_schema_bytes(&ctx.data_dir, &schema_id)?;
    let contract_bytes = load_contract_bytes(&ctx.data_dir, &contract_id)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;
    let mut memory = default_state_memory(&schema);
    let vm = RuntimeVm::new(contract_bytes);
    let disabled = load_overlay_disabled(&ctx.data_dir)?;
    let overlays = if overlay_enabled_for_schema(&schema, &disabled) {
        build_overlay_map(&ctx.data_dir, &subject, lens)?
    } else {
        BTreeMap::new()
    };
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let assertions = load_assertions_for_ver(&env, &subject, lens)?;
    let order = crate::validation::order_assertions(&assertions)?;
    let mut last_value = None;
    let mut changes = Vec::new();
    for id in order {
        let assertion = assertions
            .get(&id)
            .ok_or_else(|| DharmaError::Validation("missing assertion".to_string()))?;
        if assertion.header.typ == "core.merge" {
            continue;
        }
        let action_name = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        let action_schema = schema
            .action(action_name)
            .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
        let action_index = crate::runtime::cqrs::action_index(&schema, action_name)?;
        let overlay = overlays.get(&id);
        let merged = merge_args(&assertion.body, overlay)?;
        let args_buffer = crate::runtime::cqrs::encode_args_buffer(
            action_schema,
            &schema.structs,
            action_index,
            &merged,
            true,
        )?;
        vm.reduce(&env, &mut memory, &args_buffer, None)?;
        let value = decode_state(&memory, &schema)?;
        let current = value_at_path(&value, path);
        if current != last_value {
            changes.push((assertion.header.seq, id, current.clone()));
            last_value = current;
        }
    }
    if changes.is_empty() {
        println!("No changes recorded for {path}.");
        return Ok(());
    }
    println!("Changes for {path}:");
    for (seq, id, value) in &changes {
        let rendered = value
            .as_ref()
            .map(value_to_json)
            .unwrap_or_else(|| "null".to_string());
        println!("  seq {:>4} {} => {}", seq, id.to_hex(), rendered);
    }
    Ok(())
}

fn handle_pkg(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        print_pkg_help();
        return Ok(());
    }
    match args[0] {
        "list" => pkg_list(ctx, &args[1..]),
        "installed" => pkg_list(ctx, &args[1..]),
        "local" => pkg_list(ctx, &args[1..]),
        "search" => {
            println!("pkg search is not implemented yet.");
            Ok(())
        }
        "build" => {
            println!("pkg build is not implemented yet.");
            Ok(())
        }
        "publish" => {
            println!("pkg publish is not implemented yet.");
            Ok(())
        }
        "show" => pkg_show(ctx, &args[1..]),
        "install" => pkg_install(ctx, &args[1..]),
        "verify" => pkg_verify(ctx, &args[1..]),
        "pin" => pkg_pin(ctx, &args[1..]),
        "remove" => pkg_remove(ctx, &args[1..]),
        "uninstall" => pkg_remove(ctx, &args[1..]),
        _ => {
            print_pkg_help();
            Ok(())
        }
    }
}

fn print_pkg_help() {
    println!("pkg commands:");
    println!("  pkg ls [--json]");
    println!("  pkg installed");
    println!("  pkg local");
    println!("  pkg search");
    println!("  pkg show <name> [--json]");
    println!("  pkg install <name> [--from <registry_subject>] [--version <ver>] [--json]");
    println!("  pkg uninstall <name>");
    println!("  pkg verify <name> [--json]");
    println!("  pkg pin <name> <artifact_hash>");
    println!("  pkg build <path>");
    println!("  pkg publish <name>");
    println!("  pkg remove <name> [--keep-cache]");
}

fn pkg_list(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let json = args.iter().any(|arg| *arg == "--json");
    let manifests = pkg::list_installed(&ctx.data_dir)?;
    if json {
        let entries = manifests
            .iter()
            .map(|m| pkg_manifest_summary_value(&ctx.data_dir, m))
            .collect::<Vec<_>>();
        println!("{}", value_to_json(&Value::Array(entries)));
        return Ok(());
    }
    if manifests.is_empty() {
        println!("No packages installed.");
        return Ok(());
    }
    for manifest in manifests {
        let status = pkg::verify_manifest(&ctx.data_dir, &manifest)?;
        let trust = if status.missing.is_empty()
            && status.mismatched.is_empty()
            && status.registry_sig_ok != Some(false)
        {
            "ok"
        } else {
            "invalid"
        };
        let versions = manifest
            .versions
            .keys()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "{}  versions [{}]  trust={}",
            manifest.name, versions, trust
        );
    }
    Ok(())
}

fn pkg_show(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let (name, json) = parse_single_with_json(args)?;
    let Some(manifest) = pkg::load_manifest(&ctx.data_dir, name)? else {
        println!("Package not installed.");
        return Ok(());
    };
    if json {
        println!("{}", value_to_json(&pkg::manifest_to_json_value(&manifest)));
        return Ok(());
    }
    println!("Package: {}", manifest.name);
    if let Some(pinned) = manifest.pinned {
        println!("Pinned: {pinned}");
    }
    for (ver, version) in &manifest.versions {
        println!("  ver {ver}");
        println!("    schema: {}", version.schema.to_hex());
        println!("    contract: {}", version.contract.to_hex());
        if let Some(reactor) = version.reactor {
            println!("    reactor: {}", reactor.to_hex());
        }
        if !version.deps.is_empty() {
            let deps = version
                .deps
                .iter()
                .map(|d| {
                    if let Some(v) = d.ver {
                        format!("{}@{v}", d.name)
                    } else {
                        d.name.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            println!("    deps: {deps}");
        }
    }
    Ok(())
}

fn pkg_install(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        print_pkg_help();
        return Ok(());
    }
    let mut name = None;
    let mut registry_subject = None;
    let mut json = false;
    let mut version = None;
    let mut iter = args.iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            json = true;
            continue;
        }
        if arg == "--from" {
            if let Some(value) = iter.next() {
                registry_subject = Some(SubjectId::from_hex(value)?);
            }
            continue;
        }
        if arg == "--version" {
            if let Some(value) = iter.next() {
                version = value.parse::<u64>().ok();
            }
            continue;
        }
        if name.is_none() {
            name = Some(arg.to_string());
        }
    }
    let Some(name) = name else {
        return Err(DharmaError::Validation("missing package name".to_string()));
    };
    if registry_subject.is_none() {
        registry_subject = read_registry_subject(&ctx.data_dir);
    }
    let registries = pkg::find_registry_packages(&ctx.data_dir, &name, registry_subject)?;
    let Some(registry) = registries.first() else {
        return Err(DharmaError::Validation("package not found".to_string()));
    };
    let mut fetch = |missing: &[EnvelopeId]| fetch_missing_artifacts(ctx, missing);
    let manifest =
        pkg::install_from_registry_with_fetch(&ctx.data_dir, registry, version, &mut fetch)?;
    pkg::update_config_for_manifest(&ctx.data_dir, &manifest)?;
    let mut deps = Vec::new();
    for version in manifest.versions.values() {
        deps.extend(version.deps.clone());
    }
    let mut visited = BTreeSet::new();
    pkg::ensure_dependencies_with_fetch(
        &ctx.data_dir,
        &deps,
        registry_subject,
        &mut visited,
        &mut fetch,
    )?;
    if json {
        println!("{}", value_to_json(&pkg::manifest_to_json_value(&manifest)));
    } else {
        println!("Installed {}", manifest.name);
    }
    Ok(())
}

fn pkg_verify(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let (name, json) = parse_single_with_json(args)?;
    let Some(manifest) = pkg::load_manifest(&ctx.data_dir, name)? else {
        println!("Package not installed.");
        return Ok(());
    };
    let report = pkg::verify_manifest(&ctx.data_dir, &manifest)?;
    let mut entries = Vec::new();
    entries.push((
        Value::Text("missing".to_string()),
        Value::Array(
            report
                .missing
                .iter()
                .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                .collect(),
        ),
    ));
    entries.push((
        Value::Text("mismatched".to_string()),
        Value::Array(
            report
                .mismatched
                .iter()
                .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                .collect(),
        ),
    ));
    if let Some(sig) = report.registry_sig_ok {
        entries.push((
            Value::Text("registry_signature".to_string()),
            Value::Bool(sig),
        ));
    }
    let value = Value::Map(entries);
    if json {
        println!("{}", value_to_json(&value));
    } else {
        println!("Missing: {}", report.missing.len());
        println!("Mismatched: {}", report.mismatched.len());
        if let Some(sig) = report.registry_sig_ok {
            println!("Registry signature: {}", sig);
        }
    }
    Ok(())
}

fn pkg_pin(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.len() < 2 {
        println!("Usage: pkg pin <name> <artifact_hash>");
        return Ok(());
    }
    let name = args[0];
    let artifact = EnvelopeId::from_hex(args[1])?;
    let Some(mut manifest) = pkg::load_manifest(&ctx.data_dir, name)? else {
        println!("Package not installed.");
        return Ok(());
    };
    let pinned = pkg::pin_manifest(&ctx.data_dir, &mut manifest, &artifact)?;
    if let Some(ver) = pinned {
        println!("Pinned {name} to ver {ver}");
    } else {
        println!("Artifact hash not found in {name}.");
    }
    Ok(())
}

fn pkg_remove(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: pkg remove <name> [--keep-cache]");
        return Ok(());
    }
    let name = args[0];
    let keep_cache = args.iter().any(|arg| *arg == "--keep-cache");
    let Some(manifest) = pkg::load_manifest(&ctx.data_dir, name)? else {
        println!("Package not installed.");
        return Ok(());
    };
    pkg::remove_manifest(&ctx.data_dir, &manifest, keep_cache)?;
    println!("Removed {name}.");
    Ok(())
}

fn fetch_missing_artifacts(ctx: &ReplContext, missing: &[EnvelopeId]) -> Result<(), DharmaError> {
    if missing.is_empty() {
        return Ok(());
    }
    let identity = ctx
        .identity
        .as_ref()
        .ok_or_else(|| DharmaError::Validation("identity locked".to_string()))?;
    let peers = load_peers(&ctx.data_dir)?;
    if peers.is_empty() {
        return Err(DharmaError::Validation(
            "no peers configured to fetch artifacts".to_string(),
        ));
    }
    let store = Store::from_root(&ctx.data_dir);
    let mut pending: BTreeSet<EnvelopeId> = missing.iter().copied().collect();
    for peer in peers {
        if pending.is_empty() {
            break;
        }
        let fetched = fetch_objects_from_peer(identity, &store, &peer.addr, &pending)?;
        for id in fetched {
            pending.remove(&id);
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    Err(DharmaError::Validation(format!(
        "missing {} artifact(s) after fetch",
        pending.len()
    )))
}

fn fetch_objects_from_peer(
    identity: &IdentityState,
    store: &Store,
    addr: &str,
    pending: &BTreeSet<EnvelopeId>,
) -> Result<Vec<EnvelopeId>, DharmaError> {
    let config = std::env::current_dir()
        .ok()
        .and_then(|root| dharma::config::Config::load(&root).ok());
    let mut stream = match addr.parse::<SocketAddr>() {
        Ok(sock) => {
            if let Some(cfg) = &config {
                TcpStream::connect_timeout(&sock, cfg.connect_timeout())?
            } else {
                TcpStream::connect(sock)?
            }
        }
        Err(_) => TcpStream::connect(addr)?,
    };
    if let Some(cfg) = &config {
        cfg.apply_timeouts(&stream);
    } else {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    }
    let mut session = crate::net::handshake::client_handshake(&mut stream, identity)?;
    exchange_hello_for_fetch(&mut stream, &mut session, identity)?;
    let mut remaining: BTreeSet<EnvelopeId> = pending.clone();
    let mut chunks = ObjChunkAssembler::new(crate::net::sync::sync_obj_buffer_bytes());
    let get = SyncMessage::Get(Get {
        ids: remaining.iter().copied().map(ObjectRef::Envelope).collect(),
    });
    send_sync_msg(&mut stream, &mut session, &get)?;
    let mut received = Vec::new();
    loop {
        if remaining.is_empty() {
            break;
        }
        let msg = match read_sync_msg(&mut stream, &mut session)? {
            Some(msg) => msg,
            None => break,
        };
        match msg {
            SyncMessage::Obj(obj) => {
                chunks.discard(&obj.id);
                ingest_fetched_obj(store, &mut remaining, &mut received, obj)?;
            }
            SyncMessage::ObjChunk(chunk) => {
                if let Some(obj) = chunks.push(chunk)? {
                    ingest_fetched_obj(store, &mut remaining, &mut received, obj)?;
                }
            }
            SyncMessage::Err(err) => {
                return Err(DharmaError::Validation(format!(
                    "peer error: {}",
                    err.message
                )));
            }
            _ => {}
        }
    }
    Ok(received)
}

fn ingest_fetched_obj(
    store: &Store,
    remaining: &mut BTreeSet<EnvelopeId>,
    received: &mut Vec<EnvelopeId>,
    obj: Obj,
) -> Result<(), DharmaError> {
    let ObjectRef::Envelope(env_id) = obj.id else {
        return Ok(());
    };
    if !remaining.contains(&env_id) {
        return Ok(());
    }
    let actual = crate::crypto::envelope_id(&obj.bytes);
    if actual != env_id {
        return Err(DharmaError::Validation(
            "artifact hash mismatch".to_string(),
        ));
    }
    store.put_object(&env_id, &obj.bytes)?;
    remaining.remove(&env_id);
    received.push(env_id);
    Ok(())
}

fn exchange_hello_for_fetch(
    stream: &mut TcpStream,
    session: &mut crate::net::handshake::Session,
    identity: &IdentityState,
) -> Result<(), DharmaError> {
    let hello = Hello {
        v: 1,
        peer_id: identity.public_key,
        hpke_pk: crate::types::HpkePublicKey::from_bytes([0u8; 32]),
        suites: vec![1],
        caps: vec![CAP_SYNC_RANGE.to_string(), CAP_OBJ_CHUNK.to_string()],
        subs: Some(Subscriptions::all()),
        subject: Some(identity.subject_id),
        note: None,
    };
    send_sync_msg(stream, session, &SyncMessage::Hello(hello))?;
    let msg = read_sync_msg(stream, session)?
        .ok_or_else(|| DharmaError::Validation("peer closed".to_string()))?;
    match msg {
        SyncMessage::Hello(_) => Ok(()),
        _ => Err(DharmaError::Validation("expected hello".to_string())),
    }
}

fn send_sync_msg(
    stream: &mut TcpStream,
    session: &mut crate::net::handshake::Session,
    msg: &SyncMessage,
) -> Result<(), DharmaError> {
    let (t, payload) = match msg {
        SyncMessage::Hello(_) | SyncMessage::Inv(_) => (SYNC_TYPE_INV, msg.to_cbor()?),
        SyncMessage::Get(_) => (SYNC_TYPE_GET, msg.to_cbor()?),
        SyncMessage::Obj(_) | SyncMessage::ObjChunk(_) => (SYNC_TYPE_OBJ, msg.to_cbor()?),
        SyncMessage::Ad(_) => (SYNC_TYPE_AD, msg.to_cbor()?),
        SyncMessage::Ads(_) => (SYNC_TYPE_ADS, msg.to_cbor()?),
        SyncMessage::GetAds(_) => (SYNC_TYPE_GET_ADS, msg.to_cbor()?),
        SyncMessage::Err(_) => (SYNC_TYPE_ERR, msg.to_cbor()?),
    };
    let frame = session.encrypt(t, &payload)?;
    crate::net::codec::write_frame(stream, &frame)
}

fn read_sync_msg(
    stream: &mut TcpStream,
    session: &mut crate::net::handshake::Session,
) -> Result<Option<SyncMessage>, DharmaError> {
    let frame = match crate::net::codec::read_frame_optional(stream) {
        Ok(Some(frame)) => frame,
        Ok(None) => return Ok(None),
        Err(err) => return Err(err),
    };
    let (_t, payload) = session.decrypt(&frame)?;
    let msg = SyncMessage::from_cbor(&payload)?;
    Ok(Some(msg))
}

fn handle_overlay(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        print_overlay_help();
        return Ok(());
    }
    match args[0] {
        "status" => overlay_status(ctx, &args[1..]),
        "list" => overlay_list(ctx, &args[1..]),
        "enable" => overlay_enable(ctx, &args[1..]),
        "disable" => overlay_disable(ctx, &args[1..]),
        "show" => overlay_show(ctx, &args[1..]),
        _ => {
            print_overlay_help();
            Ok(())
        }
    }
}

fn handle_peers(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let json = args.iter().any(|arg| *arg == "--json");
    let verbose = args.iter().any(|arg| *arg == "--verbose");
    let peers = load_peers(&ctx.data_dir)?;
    if json {
        let values = peers
            .iter()
            .map(|peer| peer_value(ctx, peer, verbose))
            .collect::<Vec<_>>();
        println!("{}", value_to_json(&Value::Array(values)));
        return Ok(());
    }
    if peers.is_empty() {
        println!("No peers.");
        return Ok(());
    }
    println!("Peers ({})", peers.len());
    for (idx, peer) in peers.iter().enumerate() {
        println!("{} ) {}", idx + 1, peer.addr);
        if verbose {
            if let Some(subject) = &peer.subject {
                println!("   subject: {}", subject.to_hex());
            }
            if let Some(pubkey) = &peer.pubkey {
                println!("   pubkey: {}", pubkey.to_hex());
            }
            if let Some(last) = peer.last_seen {
                println!("   last_seen: {}", last);
            }
            let trust = peer_trust(ctx, peer);
            println!("   trust: {}", trust);
        }
    }
    Ok(())
}

fn handle_connect(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: connect <addr> [--verbose]");
        return Ok(());
    }
    let verbose = args.iter().any(|arg| *arg == "--verbose" || *arg == "-v");
    let addr = args
        .iter()
        .find(|arg| !arg.starts_with('-'))
        .copied()
        .unwrap_or("");
    if addr.is_empty() {
        println!("Usage: connect <addr> [--verbose]");
        return Ok(());
    }
    let subject_override = None;
    if io::stdout().is_terminal() {
        let spinner = ProgressBar::new_spinner();
        let style = ProgressStyle::with_template("{spinner} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner());
        spinner.set_style(style);
        spinner.enable_steady_tick(std::time::Duration::from_millis(120));
        spinner.set_message(format!("Connecting to {addr}"));
        let result = sync_with_peer(ctx, addr, subject_override, verbose);
        match &result {
            Ok(_) => spinner.finish_with_message("Connected."),
            Err(_) => spinner.finish_with_message("Connection failed."),
        }
        result?;
    } else {
        sync_with_peer(ctx, addr, subject_override, verbose)?;
    }
    Ok(())
}

fn handle_sync(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: sync now | sync subject [id] [--verbose]");
        return Ok(());
    }
    let verbose = args.iter().any(|arg| *arg == "--verbose" || *arg == "-v");
    match args[0] {
        "now" => sync_now(ctx, None, args.iter().any(|arg| *arg == "--json"), verbose),
        "subject" => {
            let subject = if args.len() > 1 {
                Some(resolve_subject(ctx, args[1])?)
            } else if let Some(current) = ctx.current_subject {
                Some(current)
            } else {
                ctx.identity.as_ref().map(|id| id.subject_id)
            };
            sync_now(
                ctx,
                subject,
                args.iter().any(|arg| *arg == "--json"),
                verbose,
            )
        }
        _ => {
            println!("Usage: sync now | sync subject [id] [--verbose]");
            Ok(())
        }
    }
}

fn handle_discover(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() || args[0] == "status" {
        let (enabled, configured) = discovery_state(ctx);
        let source = if configured { "configured" } else { "default" };
        println!(
            "Discovery: {} ({source})",
            if enabled { "ON" } else { "OFF" }
        );
        return Ok(());
    }
    match args[0] {
        "on" => {
            set_discovery_state(&ctx.data_dir, true)?;
            println!("Discovery enabled.");
        }
        "off" => {
            set_discovery_state(&ctx.data_dir, false)?;
            println!("Discovery disabled.");
        }
        _ => println!("Usage: discover [status|on|off]"),
    }
    Ok(())
}

fn handle_index(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        print_index_help();
        return Ok(());
    }
    match args[0] {
        "status" => {
            let json = args.iter().any(|arg| *arg == "--json");
            let status = collect_index_status(&ctx.data_dir)?;
            if json {
                let value = Value::Map(vec![
                    (Value::Text("built".to_string()), Value::Bool(status.built)),
                    (
                        Value::Text("partitions".to_string()),
                        Value::Integer((status.partitions as i64).into()),
                    ),
                    (
                        Value::Text("rows".to_string()),
                        Value::Integer((status.rows as i64).into()),
                    ),
                    (
                        Value::Text("wal_bytes".to_string()),
                        Value::Integer((status.wal_bytes as i64).into()),
                    ),
                ]);
                println!("{}", value_to_json(&value));
            } else {
                print_index_status(&status);
            }
        }
        "build" => {
            let json = args.iter().any(|arg| *arg == "--json");
            let kind = args
                .iter()
                .skip(1)
                .find(|arg| !arg.starts_with("--"))
                .copied()
                .unwrap_or("text");
            if !matches!(kind, "text" | "all") {
                println!("Usage: index build [text|all] [--json]");
                return Ok(());
            }
            let spinner = if !json && io::stdout().is_terminal() {
                let spinner = ProgressBar::new_spinner();
                let style = ProgressStyle::with_template("{spinner} {msg}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner());
                spinner.set_style(style);
                spinner.set_message("Building index…");
                spinner.enable_steady_tick(std::time::Duration::from_millis(120));
                Some(spinner)
            } else {
                None
            };
            crate::dharmaq_core::rebuild(&ctx.data_dir)?;
            if let Some(spinner) = spinner {
                spinner.finish_with_message("Index built.");
            } else if json {
                let status = collect_index_status(&ctx.data_dir)?;
                let value = Value::Map(vec![
                    (
                        Value::Text("status".to_string()),
                        Value::Text("built".to_string()),
                    ),
                    (
                        Value::Text("partitions".to_string()),
                        Value::Integer((status.partitions as i64).into()),
                    ),
                    (
                        Value::Text("rows".to_string()),
                        Value::Integer((status.rows as i64).into()),
                    ),
                ]);
                println!("{}", value_to_json(&value));
            } else {
                println!("Index built.");
            }
        }
        "drop" => {
            let json = args.iter().any(|arg| *arg == "--json");
            if ctx.confirmations {
                let ok = confirm("Drop DHARMA-Q index?")?;
                if !ok {
                    println!("Aborted.");
                    return Ok(());
                }
            }
            drop_index(&ctx.data_dir)?;
            if json {
                let value = Value::Map(vec![(
                    Value::Text("status".to_string()),
                    Value::Text("dropped".to_string()),
                )]);
                println!("{}", value_to_json(&value));
            } else {
                println!("Index dropped.");
            }
        }
        _ => print_index_help(),
    }
    Ok(())
}

fn handle_find(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: find \"<query>\" [--limit <n>] [--json]");
        return Ok(());
    }
    let (query, limit, json) = parse_find_args(args)?;
    let mut plan = crate::dharmaq_core::QueryPlan {
        table: "assertions".to_string(),
        filter: Some(crate::dharmaq_core::Filter::Leaf(
            crate::dharmaq_core::Predicate::TextSearch(query.clone()),
        )),
        limit,
    };
    if let Some(table) = resolve_contract_table_for_query(ctx, &plan.table, None)? {
        plan.table = table;
    }
    let results = crate::dharmaq_core::execute(&ctx.data_dir, &plan)?;
    ctx.last_results = results.clone();
    if json {
        let values = results
            .iter()
            .map(|row| find_row_value(row))
            .collect::<Vec<_>>();
        println!("{}", value_to_json(&Value::Array(values)));
        return Ok(());
    }
    if results.is_empty() {
        println!("No results.");
        return Ok(());
    }
    print_find_table(ctx, &results);
    Ok(())
}

fn handle_q(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: q <query>");
        return Ok(());
    }
    let (query, json) = parse_query_line(args)?;
    let mut plan = crate::dharmaq::parse_query(&query)?;
    if let Some(table) = resolve_contract_table_for_query(ctx, &plan.table, Some(&query))? {
        plan.table = table;
    }
    let results = crate::dharmaq_core::execute(&ctx.data_dir, &plan)?;
    ctx.last_results = results.clone();
    if json {
        let values = results
            .iter()
            .map(|row| find_row_value(row))
            .collect::<Vec<_>>();
        println!("{}", value_to_json(&Value::Array(values)));
        return Ok(());
    }
    if results.is_empty() {
        println!("No results.");
        return Ok(());
    }
    print_query_table(ctx, &results);
    Ok(())
}

fn handle_open(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: open <result_id_or_object_id>");
        return Ok(());
    }
    let token = args[0];
    let store = Store::from_root(&ctx.data_dir);
    let envelope_id = if let Ok(idx) = token.parse::<usize>() {
        if idx == 0 {
            return Err(DharmaError::Validation("result id starts at 1".to_string()));
        }
        let row = ctx
            .last_results
            .get(idx - 1)
            .ok_or_else(|| DharmaError::Validation("result id out of range".to_string()))?;
        resolve_envelope_id_any(&store, &row.assertion_id.to_hex())?
    } else {
        resolve_envelope_id_any(&store, token)?
    };
    let bytes = store
        .get_object_any(&envelope_id)?
        .ok_or_else(|| DharmaError::Validation("unknown object".to_string()))?;
    if let Ok(assertion) = AssertionPlaintext::from_cbor(&bytes) {
        print_open_assertion(&envelope_id, &assertion);
        return Ok(());
    }
    if let Ok(envelope) = crate::envelope::AssertionEnvelope::from_cbor(&bytes) {
        if let Some(identity) = &ctx.identity {
            if let Ok(plaintext) =
                crate::envelope::decrypt_assertion(&envelope, &identity.subject_key)
            {
                if let Ok(assertion) = AssertionPlaintext::from_cbor(&plaintext) {
                    print_open_assertion(&envelope_id, &assertion);
                    return Ok(());
                }
            }
        }
        println!("Object {}", short_hex(&envelope_id.to_hex(), 12));
        println!("Envelope: v{} suite {}", envelope.v, envelope.suite);
        println!("Locked.");
        return Ok(());
    }
    Err(DharmaError::Validation("unrecognized object".to_string()))
}

fn handle_tables(ctx: &mut ReplContext, _args: &[&str]) -> Result<(), DharmaError> {
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let base = ctx.data_dir.join(DHARMAQ_DIR).join("tables");
    if !env.exists(&base) {
        println!("No tables. Build the index first.");
        return Ok(());
    }
    let mut tables = Vec::new();
    for path in env.list_dir(&base)? {
        if env.is_dir(&path) {
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                tables.push(name.to_string());
            }
        }
    }
    if tables.is_empty() {
        println!("No tables.");
        return Ok(());
    }
    tables.sort();
    for table in tables {
        println!("{table}");
    }
    Ok(())
}

fn handle_table(ctx: &mut ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: table <table>");
        return Ok(());
    }
    let table = args[0];
    if table == DHARMAQ_TABLE {
        let status = collect_index_status(&ctx.data_dir)?;
        let rows = if status.built { status.rows } else { 0 };
        println!("Table: {table}");
        println!("Rows: {rows}");
        if !status.built {
            println!("Index: not built");
        }
        let fields = vec![
            TableFieldRow {
                name: "assertion_id".to_string(),
                typ: "Bytes32".to_string(),
            },
            TableFieldRow {
                name: "subject".to_string(),
                typ: "Bytes32".to_string(),
            },
            TableFieldRow {
                name: "seq".to_string(),
                typ: "Int".to_string(),
            },
            TableFieldRow {
                name: "typ".to_string(),
                typ: "Text".to_string(),
            },
            TableFieldRow {
                name: "snippet".to_string(),
                typ: "Text".to_string(),
            },
            TableFieldRow {
                name: "score".to_string(),
                typ: "Int".to_string(),
            },
        ];
        println!("{}", Table::new(fields).with(Style::rounded()).to_string());
        return Ok(());
    }

    let (contract, lens, kind) = parse_contract_table_spec_name(table, ctx.current_lens)?;
    let entry = resolve_contract_by_name(ctx, &contract)?;
    let table_name = ensure_contract_table(ctx, &entry, lens, kind.clone())?;
    let rows = table_row_count(&ctx.data_dir, &table_name)?;
    println!("Table: {table_name}");
    println!("Rows: {rows}");
    let mut fields = vec![
        TableFieldRow {
            name: "assertion_id".to_string(),
            typ: "Bytes32".to_string(),
        },
        TableFieldRow {
            name: "subject".to_string(),
            typ: "Bytes32".to_string(),
        },
        TableFieldRow {
            name: "seq".to_string(),
            typ: "Int".to_string(),
        },
        TableFieldRow {
            name: "typ".to_string(),
            typ: "Text".to_string(),
        },
    ];
    match kind {
        dharma::dharmaq::ContractTableKind::State => {
            for (name, field) in &entry.schema.fields {
                fields.push(TableFieldRow {
                    name: name.clone(),
                    typ: format_type(&field.typ),
                });
            }
        }
        dharma::dharmaq::ContractTableKind::Assertions => {
            fields.push(TableFieldRow {
                name: "snippet".to_string(),
                typ: "Text".to_string(),
            });
            fields.push(TableFieldRow {
                name: "score".to_string(),
                typ: "Int".to_string(),
            });
            let mut args = BTreeMap::new();
            for action in entry.schema.actions.values() {
                for (name, typ) in &action.args {
                    args.entry(name.clone()).or_insert_with(|| format_type(typ));
                }
            }
            for (name, typ) in args {
                fields.push(TableFieldRow { name, typ });
            }
        }
    }
    println!("{}", Table::new(fields).with(Style::rounded()).to_string());
    Ok(())
}

fn table_row_count(root: &PathBuf, table: &str) -> Result<u64, DharmaError> {
    let env = dharma::env::StdEnv::new(root);
    let base = root.join(DHARMAQ_DIR).join("tables").join(table);
    let partitions_dir = base.join("partitions");
    if !env.exists(&partitions_dir) {
        return Ok(0);
    }
    let mut rows = 0u64;
    for path in env.list_dir(&partitions_dir)? {
        if !env.is_dir(&path) {
            continue;
        }
        let seq_path = path.join("cols").join("seq.bin");
        if env.exists(&seq_path) {
            rows += env.file_len(&seq_path)? / 8;
        }
    }
    Ok(rows)
}

#[derive(Clone, Debug, Default)]
struct IndexStatus {
    built: bool,
    partitions: usize,
    rows: u64,
    wal_bytes: u64,
}

fn print_index_help() {
    println!("index commands:");
    println!("  index status [--json]");
    println!("  index build [text|all] [--json]");
    println!("  index drop [--json]");
}

fn print_index_status(status: &IndexStatus) {
    if !status.built {
        println!("Index: not built");
        return;
    }
    println!("Index: DHARMA-Q");
    println!("Partitions: {}", status.partitions);
    println!("Rows: {}", status.rows);
    let wal_state = if status.wal_bytes > 0 {
        "dirty"
    } else {
        "clean"
    };
    println!("WAL: {} bytes ({wal_state})", status.wal_bytes);
}

fn collect_index_status(root: &PathBuf) -> Result<IndexStatus, DharmaError> {
    let env = dharma::env::StdEnv::new(root);
    let base = root.join(DHARMAQ_DIR);
    if !env.exists(&base) {
        return Ok(IndexStatus::default());
    }
    let table_root = base.join("tables").join(DHARMAQ_TABLE);
    let partitions_dir = table_root.join("partitions");
    let mut partitions = Vec::new();
    if env.exists(&partitions_dir) {
        for path in env.list_dir(&partitions_dir)? {
            if env.is_dir(&path) {
                partitions.push(path);
            }
        }
    }
    let mut rows = 0u64;
    for partition in &partitions {
        let seq_path = partition.join("cols").join("seq.bin");
        if env.exists(&seq_path) {
            rows += env.file_len(&seq_path)? / 8;
        }
    }
    let wal_path = table_root.join("hot").join("wal.bin");
    let wal_bytes = if env.exists(&wal_path) {
        env.file_len(&wal_path)?
    } else {
        0
    };
    Ok(IndexStatus {
        built: true,
        partitions: partitions.len(),
        rows,
        wal_bytes,
    })
}

fn drop_index(root: &PathBuf) -> Result<(), DharmaError> {
    let env = dharma::env::StdEnv::new(root);
    let base = root.join(DHARMAQ_DIR);
    if env.exists(&base) {
        env.remove_dir_all(&base)?;
    }
    Ok(())
}

fn print_overlay_help() {
    println!("overlay commands:");
    println!("  overlay status [--json]");
    println!("  overlay list [--json]");
    println!("  overlay enable <namespace>");
    println!("  overlay disable <namespace>");
    println!("  overlay show <namespace> [--tail <n>] [--json]");
}

fn overlay_status(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let json = args.iter().any(|arg| *arg == "--json");
    let subject = current_subject_or_identity(ctx)?;
    let lens = ctx.current_lens;
    let overlays = overlay_namespace_info(&ctx.data_dir, &subject, lens)?;
    let disabled = load_overlay_disabled(&ctx.data_dir)?;
    let mut entries = Vec::new();
    let mut enabled_any = false;
    for (namespace, info) in &overlays {
        let enabled = !disabled.contains(namespace);
        if enabled && info.decryptable {
            enabled_any = true;
        }
        entries.push(overlay_info_value(namespace, info, enabled));
    }
    let view = if enabled_any { "merged" } else { "base-only" };
    if json {
        let mut map = Vec::new();
        map.push((
            Value::Text("view".to_string()),
            Value::Text(view.to_string()),
        ));
        map.push((Value::Text("overlays".to_string()), Value::Array(entries)));
        println!("{}", value_to_json(&Value::Map(map)));
        return Ok(());
    }
    let model = if enabled_any { "ENABLED" } else { "DISABLED" };
    println!("Overlay model: {model}");
    println!("View: {view}");
    if overlays.is_empty() {
        println!("Overlays present: 0");
        return Ok(());
    }
    println!("Overlays present ({}):", overlays.len());
    for (namespace, info) in overlays {
        let enabled = !disabled.contains(&namespace);
        let lock = if info.decryptable { "✅" } else { "🔒" };
        let status = if enabled { "enabled" } else { "disabled" };
        println!(
            "  - {namespace} (decryptable {lock}) [{status}] chain: {} assertions",
            info.count
        );
    }
    Ok(())
}

fn overlay_list(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    let json = args.iter().any(|arg| *arg == "--json");
    let subject = current_subject_or_identity(ctx)?;
    let lens = ctx.current_lens;
    let overlays = overlay_namespace_info(&ctx.data_dir, &subject, lens)?;
    if json {
        let list = overlays.keys().map(|n| Value::Text(n.clone())).collect();
        println!("{}", value_to_json(&Value::Array(list)));
        return Ok(());
    }
    if overlays.is_empty() {
        println!("No overlays.");
        return Ok(());
    }
    println!("Overlays:");
    for namespace in overlays.keys() {
        println!("  {namespace}");
    }
    Ok(())
}

fn overlay_enable(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: overlay enable <namespace>");
        return Ok(());
    }
    let namespace = args[0];
    let subject = current_subject_or_identity(ctx)?;
    let lens = ctx.current_lens;
    let overlays = overlay_namespace_info(&ctx.data_dir, &subject, lens)?;
    let Some(info) = overlays.get(namespace) else {
        println!("No overlays for namespace {namespace}.");
        return Ok(());
    };
    if !info.decryptable {
        return Err(DharmaError::Validation("E_NO_KEYS".to_string()));
    }
    let mut disabled = load_overlay_disabled(&ctx.data_dir)?;
    if disabled.remove(namespace) {
        save_overlay_disabled(&ctx.data_dir, &disabled)?;
        println!("Enabled overlays for {namespace}.");
    } else {
        println!("Overlays already enabled for {namespace}.");
    }
    Ok(())
}

fn overlay_disable(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: overlay disable <namespace>");
        return Ok(());
    }
    let namespace = args[0];
    let mut disabled = load_overlay_disabled(&ctx.data_dir)?;
    if disabled.insert(namespace.to_string()) {
        save_overlay_disabled(&ctx.data_dir, &disabled)?;
        println!("Disabled overlays for {namespace}.");
    } else {
        println!("Overlays already disabled for {namespace}.");
    }
    Ok(())
}

fn overlay_show(ctx: &ReplContext, args: &[&str]) -> Result<(), DharmaError> {
    if args.is_empty() {
        println!("Usage: overlay show <namespace> [--tail <n>] [--json]");
        return Ok(());
    }
    let namespace = args[0];
    let mut tail = None;
    let mut json = false;
    let mut iter = args[1..].iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            json = true;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--tail=") {
            if let Ok(parsed) = value.parse::<usize>() {
                tail = Some(parsed);
            }
            continue;
        }
        if arg == "--tail" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = value.parse::<usize>() {
                    tail = Some(parsed);
                }
            }
        }
    }
    let subject = current_subject_or_identity(ctx)?;
    let lens = ctx.current_lens;
    let records = overlay_records_for_namespace(&ctx.data_dir, &subject, lens, namespace)?;
    if records.is_empty() {
        println!("No overlays for {namespace}.");
        return Ok(());
    }
    let mut records = records;
    if let Some(limit) = tail {
        if records.len() > limit {
            records = records.split_off(records.len() - limit);
        }
    }
    if json {
        let values = records
            .iter()
            .map(|entry| overlay_record_value(entry))
            .collect::<Vec<_>>();
        println!("{}", value_to_json(&Value::Array(values)));
        return Ok(());
    }
    for entry in records {
        let action = entry.action.as_deref().unwrap_or("<unknown>");
        let base = entry
            .base
            .as_ref()
            .map(|id| id.to_hex())
            .unwrap_or_else(|| "none".to_string());
        println!(
            "#{:>4} {} action={} ref={}",
            entry.seq,
            entry.assertion_id.to_hex(),
            action,
            base
        );
    }
    Ok(())
}

fn pkg_manifest_summary_value(root: &PathBuf, manifest: &pkg::PackageManifest) -> Value {
    let report = pkg::verify_manifest(root, manifest).ok();
    let trust = report
        .as_ref()
        .map(|r| {
            r.missing.is_empty() && r.mismatched.is_empty() && r.registry_sig_ok != Some(false)
        })
        .unwrap_or(false);
    let versions = manifest
        .versions
        .keys()
        .map(|v| Value::Integer((*v as i64).into()))
        .collect::<Vec<_>>();
    let mut entries = Vec::new();
    entries.push((
        Value::Text("name".to_string()),
        Value::Text(manifest.name.clone()),
    ));
    entries.push((Value::Text("versions".to_string()), Value::Array(versions)));
    entries.push((Value::Text("trusted".to_string()), Value::Bool(trust)));
    if let Some(pinned) = manifest.pinned {
        entries.push((
            Value::Text("pinned".to_string()),
            Value::Integer((pinned as i64).into()),
        ));
    }
    Value::Map(entries)
}

fn read_registry_subject(root: &PathBuf) -> Option<SubjectId> {
    let path = root.join("dharma.toml");
    let contents = fs::read_to_string(path).ok()?;
    for line in contents.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let value = v.trim().trim_matches('"');
            if key == "registry_subject" {
                if let Ok(subject) = SubjectId::from_hex(value) {
                    return Some(subject);
                }
            }
        }
    }
    None
}

struct ActionFlags {
    json: bool,
    force: bool,
    lens: u64,
}

struct ActionPlan {
    root: PathBuf,
    subject: SubjectId,
    action: String,
    ver: u64,
    schema_id: SchemaId,
    contract_id: ContractId,
    schema: CqrsSchema,
    contract_bytes: Vec<u8>,
    args_value: Value,
    args_buffer: Vec<u8>,
    base_args: Value,
    overlay_args: Value,
}

struct ActionPreview {
    allowed: bool,
    reason: Option<String>,
    diff: Vec<DiffEntry>,
    writes: usize,
    pre_state: Value,
    post_state: Option<Value>,
}

impl ActionPreview {
    fn to_value(&self) -> Value {
        let mut entries = Vec::new();
        entries.push((
            Value::Text("allowed".to_string()),
            Value::Bool(self.allowed),
        ));
        if let Some(reason) = &self.reason {
            entries.push((
                Value::Text("reason".to_string()),
                Value::Text(reason.clone()),
            ));
        }
        entries.push((
            Value::Text("writes".to_string()),
            Value::Integer((self.writes as i64).into()),
        ));
        entries.push((Value::Text("diff".to_string()), diff_to_value(&self.diff)));
        entries.push((
            Value::Text("state_before".to_string()),
            self.pre_state.clone(),
        ));
        if let Some(after) = &self.post_state {
            entries.push((Value::Text("state_after".to_string()), after.clone()));
        }
        Value::Map(entries)
    }
}

fn extract_action_flags(
    ctx: &ReplContext,
    args: &[&str],
) -> Result<(ActionFlags, Vec<String>), DharmaError> {
    let mut flags = ActionFlags {
        json: false,
        force: false,
        lens: ctx.current_lens,
    };
    let mut values = Vec::new();
    let mut iter = args.iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            flags.json = true;
            continue;
        }
        if arg == "--force" {
            flags.force = true;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--lens=") {
            if let Ok(parsed) = value.parse::<u64>() {
                flags.lens = parsed;
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--ver=") {
            if let Ok(parsed) = value.parse::<u64>() {
                flags.lens = parsed;
            }
            continue;
        }
        if arg == "--lens" || arg == "--ver" || arg == "--data_ver" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = value.parse::<u64>() {
                    flags.lens = parsed;
                }
            }
            continue;
        }
        values.push(arg.to_string());
    }
    Ok((flags, values))
}

fn prepare_action(
    ctx: &ReplContext,
    action: &str,
    args: &[String],
    ver: u64,
) -> Result<ActionPlan, DharmaError> {
    let subject = current_subject_or_identity(ctx)?;
    let store = Store::from_root(&ctx.data_dir);
    if !store.subject_dir(&subject).join("assertions").exists() {
        let mut legacy_keys = std::collections::HashMap::new();
        if let Some(identity) = &ctx.identity {
            legacy_keys.insert(identity.subject_id, identity.subject_key);
        }
        let mut keys = dharma::keys::Keyring::from_subject_keys(&legacy_keys);
        if let Some(identity) = &ctx.identity {
            keys.insert_hpke_secret(identity.public_key, identity.noise_sk);
        }
        store.rebuild_subject_views(&keys)?;
    }
    let (schema_id, contract_id) = load_contract_ids_for_ver(&ctx.data_dir, ver)?;
    let schema_bytes = load_schema_bytes(&ctx.data_dir, &schema_id)?;
    let contract_bytes = load_contract_bytes(&ctx.data_dir, &contract_id)?;
    let schema = CqrsSchema::from_cbor(&schema_bytes)?;
    let action_schema = schema
        .action(action)
        .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
    let args_value = parse_action_args(action_schema, args)?;
    validate_args(action_schema, &args_value)?;
    let action_index = crate::runtime::cqrs::action_index(&schema, action)?;
    let args_buffer = crate::runtime::cqrs::encode_args_buffer(
        action_schema,
        &schema.structs,
        action_index,
        &args_value,
        false,
    )?;
    let (base_args, overlay_args) = split_action_args(action_schema, &args_value)?;
    Ok(ActionPlan {
        root: ctx.data_dir.clone(),
        subject,
        action: action.to_string(),
        ver,
        schema_id,
        contract_id,
        schema,
        contract_bytes,
        args_value,
        args_buffer,
        base_args,
        overlay_args,
    })
}

fn simulate_action(
    identity: &crate::IdentityState,
    plan: &ActionPlan,
) -> Result<ActionPreview, DharmaError> {
    let env = dharma::env::StdEnv::new(&plan.root);
    let mut state = load_state_until(
        &env,
        &plan.subject,
        &plan.schema,
        &plan.contract_bytes,
        plan.ver,
        None,
    )?;
    let pre_state = decode_state(&state.memory, &plan.schema)?;
    let vm = RuntimeVm::new(plan.contract_bytes.clone());
    let context = build_context(identity);
    let validation = vm.validate(&env, &mut state.memory, &plan.args_buffer, Some(&context));
    if let Err(err) = validation {
        return Ok(ActionPreview {
            allowed: false,
            reason: Some(err.to_string()),
            diff: Vec::new(),
            writes: 0,
            pre_state,
            post_state: None,
        });
    }
    vm.reduce(&env, &mut state.memory, &plan.args_buffer, Some(&context))?;
    let post_state = decode_state(&state.memory, &plan.schema)?;
    let diff = diff_values(&pre_state, &post_state);
    let writes = 1 + if is_empty_args(&plan.overlay_args) {
        0
    } else {
        1
    };
    Ok(ActionPreview {
        allowed: true,
        reason: None,
        diff,
        writes,
        pre_state,
        post_state: Some(post_state),
    })
}

fn commit_action(identity: &crate::IdentityState, plan: &ActionPlan) -> Result<Value, DharmaError> {
    let store = Store::from_root(&plan.root);
    let env = dharma::env::StdEnv::new(&plan.root);
    let mut state = load_state_until(
        &env,
        &plan.subject,
        &plan.schema,
        &plan.contract_bytes,
        plan.ver,
        None,
    )?;
    let index = FrontierIndex::new(&plan.root)?;
    ensure_concurrency(
        &plan.schema,
        &index,
        &plan.subject,
        plan.ver,
        state.last_object,
    )?;
    let vm = RuntimeVm::new(plan.contract_bytes.clone());
    let context = build_context(identity);
    vm.validate(&env, &mut state.memory, &plan.args_buffer, Some(&context))?;
    vm.reduce(&env, &mut state.memory, &plan.args_buffer, Some(&context))?;
    let last_seq = state.last_seq;
    let prev = state.last_object;
    let last_overlay_seq = state.last_overlay_seq;
    let last_overlay_object = state.last_overlay_object;

    let header = AssertionHeader {
        v: crypto::PROTOCOL_VERSION,
        ver: plan.ver,
        sub: plan.subject,
        typ: format!("action.{}", plan.action),
        auth: identity.public_key,
        seq: last_seq + 1,
        prev,
        refs: Vec::new(),
        ts: None,
        schema: plan.schema_id,
        contract: plan.contract_id,
        note: None,
        meta: add_signer_meta(None, &identity.subject_id),
    };
    let assertion =
        AssertionPlaintext::sign(header, plan.base_args.clone(), &identity.signing_key)?;
    let bytes = assertion.to_cbor()?;
    let assertion_id = assertion.assertion_id()?;
    let envelope_id = crypto::envelope_id(&bytes);
    store.put_assertion(&plan.subject, &envelope_id, &bytes)?;
    store.record_semantic(&assertion_id, &envelope_id)?;
    let env = dharma::env::StdEnv::new(&plan.root);
    append_assertion(
        &env,
        &plan.subject,
        last_seq + 1,
        assertion_id,
        envelope_id,
        &plan.action,
        &bytes,
    )?;

    let mut overlays_written = Vec::new();
    if !is_empty_args(&plan.overlay_args) {
        let overlay_header = AssertionHeader {
            v: crypto::PROTOCOL_VERSION,
            ver: plan.ver,
            sub: plan.subject,
            typ: format!("action.{}", plan.action),
            auth: identity.public_key,
            seq: last_overlay_seq + 1,
            prev: last_overlay_object,
            refs: vec![assertion_id],
            ts: None,
            schema: plan.schema_id,
            contract: plan.contract_id,
            note: None,
            meta: add_signer_meta(
                Some(Value::Map(vec![(
                    Value::Text("overlay".to_string()),
                    Value::Bool(true),
                )])),
                &identity.subject_id,
            ),
        };
        let overlay_assertion = AssertionPlaintext::sign(
            overlay_header,
            plan.overlay_args.clone(),
            &identity.signing_key,
        )?;
        let overlay_bytes = overlay_assertion.to_cbor()?;
        let overlay_assertion_id = overlay_assertion.assertion_id()?;
        let overlay_envelope_id = crypto::envelope_id(&overlay_bytes);
        store.put_assertion(&plan.subject, &overlay_envelope_id, &overlay_bytes)?;
        store.record_semantic(&overlay_assertion_id, &overlay_envelope_id)?;
        append_overlay(
            &env,
            &plan.subject,
            last_overlay_seq + 1,
            overlay_assertion_id,
            overlay_envelope_id,
            &plan.action,
            &overlay_bytes,
        )?;
        overlays_written.push(overlay_assertion_id);
    }

    if (last_seq + 1) % 50 == 0 {
        let snapshot = Snapshot {
            header: SnapshotHeader {
                seq: last_seq + 1,
                ver: plan.ver,
                last_assertion: assertion_id,
                timestamp: now_timestamp(),
            },
            memory: state.memory.clone(),
        };
        let env = dharma::env::StdEnv::new(&plan.root);
        save_snapshot(&env, &plan.subject, &snapshot)?;
    }

    let mut entries = Vec::new();
    entries.push((
        Value::Text("base".to_string()),
        Value::Bytes(assertion_id.as_bytes().to_vec()),
    ));
    if !overlays_written.is_empty() {
        let overlays = overlays_written
            .iter()
            .map(|id| Value::Bytes(id.as_bytes().to_vec()))
            .collect();
        entries.push((Value::Text("overlays".to_string()), Value::Array(overlays)));
    }
    Ok(Value::Map(entries))
}

fn ensure_concurrency(
    schema: &CqrsSchema,
    index: &FrontierIndex,
    subject: &SubjectId,
    ver: u64,
    prev: Option<AssertionId>,
) -> Result<(), DharmaError> {
    if schema.concurrency != ConcurrencyMode::Strict {
        return Ok(());
    }
    let tips = index.get_tips_for_ver(subject, ver);
    if tips.len() > 1 {
        return Err(DharmaError::Validation(
            "fork detected; merge required".to_string(),
        ));
    }
    if let Some(prev_id) = prev {
        if tips.len() == 1 && tips[0] != prev_id {
            return Err(DharmaError::Validation(
                "fork detected; merge required".to_string(),
            ));
        }
    }
    Ok(())
}

fn handle_diff_usage() {
    println!("Usage: diff --at <idA> <idB> [--lens <verA> <verB>] [--json]");
}

fn parse_diff_default_args(
    args: &[&str],
    default_lens: u64,
) -> Result<(u64, u64, bool), DharmaError> {
    let mut lens_values = Vec::new();
    let mut json = false;
    let mut iter = args.iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            json = true;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--lens=") {
            let parsed = value
                .parse::<u64>()
                .map_err(|_| DharmaError::Validation("invalid lens".to_string()))?;
            lens_values.push(parsed);
            continue;
        }
        if arg == "--lens" {
            let Some(value) = iter.next() else {
                handle_diff_usage();
                return Err(DharmaError::Validation("missing lens".to_string()));
            };
            let parsed = value
                .parse::<u64>()
                .map_err(|_| DharmaError::Validation("invalid lens".to_string()))?;
            lens_values.push(parsed);
            continue;
        }
        handle_diff_usage();
        return Err(DharmaError::Validation("invalid argument".to_string()));
    }
    let lens_a = lens_values.get(0).copied().unwrap_or(default_lens);
    let lens_b = lens_values.get(1).copied().unwrap_or(lens_a);
    Ok((lens_a, lens_b, json))
}

struct DiffOpts {
    at: (Option<String>, Option<String>),
    lens: (u64, u64),
}

fn parse_diff_args(args: &[&str], default_lens: u64) -> Result<(DiffOpts, bool), DharmaError> {
    let mut lens_a: Option<u64> = None;
    let mut lens_b: Option<u64> = None;
    let mut at_a: Option<String> = None;
    let mut at_b: Option<String> = None;
    let mut json = false;
    let mut iter = args.iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            json = true;
            continue;
        }
        if arg == "--at" {
            let Some(a) = iter.next() else {
                handle_diff_usage();
                return Err(DharmaError::Validation("missing tip".to_string()));
            };
            let Some(b) = iter.next() else {
                handle_diff_usage();
                return Err(DharmaError::Validation("missing tip".to_string()));
            };
            at_a = Some(a.to_string());
            at_b = Some(b.to_string());
            continue;
        }
        if arg == "--lens" {
            if lens_a.is_none() {
                if let Some(value) = iter.next() {
                    lens_a = value.parse::<u64>().ok();
                }
            } else if let Some(value) = iter.next() {
                lens_b = value.parse::<u64>().ok();
            }
            continue;
        }
    }
    if at_a.is_none() || at_b.is_none() {
        handle_diff_usage();
        return Err(DharmaError::Validation("missing tips".to_string()));
    }
    let lens_a = lens_a.unwrap_or(default_lens);
    let lens_b = lens_b.unwrap_or(lens_a);
    Ok((
        DiffOpts {
            at: (at_a, at_b),
            lens: (lens_a, lens_b),
        },
        json,
    ))
}

fn diff_to_value(diff: &[DiffEntry]) -> Value {
    let entries = diff
        .iter()
        .map(|entry| {
            let mut fields = Vec::new();
            fields.push((
                Value::Text("path".to_string()),
                Value::Text(entry.path.clone()),
            ));
            let before = entry.before.clone().unwrap_or(Value::Null);
            let after = entry.after.clone().unwrap_or(Value::Null);
            fields.push((Value::Text("before".to_string()), before));
            fields.push((Value::Text("after".to_string()), after));
            Value::Map(fields)
        })
        .collect::<Vec<_>>();
    Value::Array(entries)
}

fn print_diff(diff: &[DiffEntry], color: bool) {
    if diff.is_empty() {
        println!("No differences.");
        return;
    }
    for entry in diff {
        match (&entry.before, &entry.after) {
            (None, Some(after)) => {
                let sign = paint_diff("+", Color::Green, color);
                println!("{sign} {} = {}", entry.path, value_to_json(after));
            }
            (Some(before), None) => {
                let sign = paint_diff("-", Color::Red, color);
                println!("{sign} {} = {}", entry.path, value_to_json(before));
            }
            (Some(before), Some(after)) => {
                let sign = paint_diff("~", Color::Yellow, color);
                println!(
                    "{sign} {}: {} -> {}",
                    entry.path,
                    value_to_json(before),
                    value_to_json(after)
                );
            }
            _ => {}
        }
    }
}

fn parse_single_with_json<'a>(args: &'a [&'a str]) -> Result<(&'a str, bool), DharmaError> {
    let mut token = None;
    let mut json = false;
    for arg in args {
        if *arg == "--json" {
            json = true;
        } else if token.is_none() {
            token = Some(*arg);
        }
    }
    let token = token.ok_or_else(|| DharmaError::Validation("missing argument".to_string()))?;
    Ok((token, json))
}

fn parse_find_args(args: &[&str]) -> Result<(String, usize, bool), DharmaError> {
    let mut json = false;
    let mut limit = 25usize;
    let mut parts = Vec::new();
    let mut iter = args.iter().copied().peekable();
    while let Some(arg) = iter.next() {
        if arg == "--json" {
            json = true;
            continue;
        }
        if arg == "--limit" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = value.parse::<usize>() {
                    limit = parsed;
                }
            }
            continue;
        }
        if let Some(value) = arg.strip_prefix("--limit=") {
            if let Ok(parsed) = value.parse::<usize>() {
                limit = parsed;
            }
            continue;
        }
        parts.push(arg);
    }
    let joined = parts.join(" ");
    let query = if let Some(start) = joined.find('"') {
        let tail = &joined[start + 1..];
        if let Some(end) = tail.find('"') {
            tail[..end].to_string()
        } else {
            joined.trim_matches('"').to_string()
        }
    } else {
        joined.trim().to_string()
    };
    if query.is_empty() {
        return Err(DharmaError::Validation("missing query".to_string()));
    }
    Ok((query, limit, json))
}

fn parse_query_line(args: &[&str]) -> Result<(String, bool), DharmaError> {
    let mut json = false;
    let mut parts = Vec::new();
    for arg in args {
        if *arg == "--json" {
            json = true;
        } else {
            parts.push(*arg);
        }
    }
    let query = parts.join(" ");
    if query.trim().is_empty() {
        return Err(DharmaError::Validation("missing query".to_string()));
    }
    Ok((query, json))
}

fn find_row_value(row: &crate::dharmaq_core::QueryRow) -> Value {
    let mut entries = Vec::new();
    entries.push((
        Value::Text("assertion".to_string()),
        Value::Bytes(row.assertion_id.as_bytes().to_vec()),
    ));
    entries.push((
        Value::Text("subject".to_string()),
        Value::Bytes(row.subject.as_bytes().to_vec()),
    ));
    entries.push((
        Value::Text("seq".to_string()),
        Value::Integer((row.seq as i64).into()),
    ));
    entries.push((Value::Text("typ".to_string()), Value::Text(row.typ.clone())));
    entries.push((
        Value::Text("score".to_string()),
        Value::Integer((row.score as i64).into()),
    ));
    if let Some(snippet) = &row.snippet {
        entries.push((
            Value::Text("snippet".to_string()),
            Value::Text(snippet.clone()),
        ));
    }
    Value::Map(entries)
}

fn parse_action_args(action: &ActionSchema, args: &[String]) -> Result<Value, DharmaError> {
    let mut supplied = BTreeMap::new();
    for arg in args {
        let (key, value) = arg
            .split_once('=')
            .ok_or_else(|| DharmaError::Validation("invalid arg".to_string()))?;
        supplied.insert(key.to_string(), value.to_string());
    }
    for key in supplied.keys() {
        if !action.args.contains_key(key) {
            return Err(DharmaError::Validation("unknown arg".to_string()));
        }
    }
    let mut entries = Vec::new();
    for (name, typ) in &action.args {
        let raw = supplied
            .get(name)
            .ok_or_else(|| DharmaError::Validation("missing arg".to_string()))?;
        let value = parse_value(raw, typ)?;
        entries.push((Value::Text(name.clone()), value));
    }
    Ok(Value::Map(entries))
}

fn split_action_args(
    action: &ActionSchema,
    args_value: &Value,
) -> Result<(Value, Value), DharmaError> {
    let map = crate::value::expect_map(args_value)?;
    let mut public_entries = Vec::new();
    let mut private_entries = Vec::new();
    for (k, v) in map {
        let name = crate::value::expect_text(k)?;
        let visibility = action
            .arg_vis
            .get(&name)
            .copied()
            .unwrap_or(Visibility::Public);
        let entry = (Value::Text(name), v.clone());
        match visibility {
            Visibility::Public => public_entries.push(entry),
            Visibility::Private => private_entries.push(entry),
        }
    }
    Ok((Value::Map(public_entries), Value::Map(private_entries)))
}

fn parse_value(raw: &str, typ: &TypeSpec) -> Result<Value, DharmaError> {
    match typ {
        TypeSpec::Optional(inner) => {
            if raw.trim() == "null" {
                return Ok(Value::Null);
            }
            parse_value(raw, inner)
        }
        TypeSpec::Int | TypeSpec::Duration | TypeSpec::Timestamp => raw
            .parse::<i64>()
            .map(|v| Value::Integer(v.into()))
            .map_err(|_| DharmaError::Validation("invalid int".to_string())),
        TypeSpec::Decimal(scale) => {
            let mantissa = parse_decimal_arg(raw, *scale)?;
            Ok(Value::Integer(mantissa.into()))
        }
        TypeSpec::Bool => match raw {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => Err(DharmaError::Validation("invalid bool".to_string())),
        },
        TypeSpec::Text(_) | TypeSpec::Currency => Ok(Value::Text(raw.to_string())),
        TypeSpec::Enum(_) => Ok(Value::Text(raw.to_string())),
        TypeSpec::Identity | TypeSpec::Ref(_) => {
            let bytes = crate::types::hex_decode(raw)?;
            if bytes.len() != 32 {
                return Err(DharmaError::Validation("invalid identity".to_string()));
            }
            Ok(Value::Bytes(bytes))
        }
        TypeSpec::SubjectRef(_) => {
            let (id_raw, seq_raw) = raw
                .split_once('@')
                .or_else(|| raw.split_once(':'))
                .ok_or_else(|| {
                    DharmaError::Validation("subject_ref expects hex@seq".to_string())
                })?;
            let bytes = crate::types::hex_decode(id_raw)?;
            if bytes.len() != 32 {
                return Err(DharmaError::Validation(
                    "invalid subject_ref id".to_string(),
                ));
            }
            let seq = seq_raw
                .trim()
                .parse::<u64>()
                .map_err(|_| DharmaError::Validation("invalid subject_ref seq".to_string()))?;
            Ok(Value::Map(vec![
                (Value::Text("id".to_string()), Value::Bytes(bytes)),
                (Value::Text("seq".to_string()), Value::Integer(seq.into())),
            ]))
        }
        TypeSpec::GeoPoint => {
            let parts: Vec<&str> = raw.split(',').collect();
            if parts.len() != 2 {
                return Err(DharmaError::Validation("invalid geopoint".to_string()));
            }
            let lat = parts[0]
                .trim()
                .parse::<i64>()
                .map_err(|_| DharmaError::Validation("invalid geopoint".to_string()))?;
            let lon = parts[1]
                .trim()
                .parse::<i64>()
                .map_err(|_| DharmaError::Validation("invalid geopoint".to_string()))?;
            Ok(Value::Array(vec![
                Value::Integer(lat.into()),
                Value::Integer(lon.into()),
            ]))
        }
        TypeSpec::Ratio => {
            let (num, den) = parse_ratio_arg(raw)?;
            Ok(Value::Map(vec![
                (Value::Text("num".to_string()), Value::Integer(num.into())),
                (Value::Text("den".to_string()), Value::Integer(den.into())),
            ]))
        }
        TypeSpec::Struct(_) => Err(DharmaError::Validation(
            "struct args unsupported".to_string(),
        )),
        TypeSpec::List(_) | TypeSpec::Map(_, _) => Err(DharmaError::Validation(
            "collection args unsupported".to_string(),
        )),
    }
}

fn parse_decimal_arg(raw: &str, scale: Option<u32>) -> Result<i64, DharmaError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(DharmaError::Validation("invalid decimal".to_string()));
    }
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((left, right)) => (left, Some(right)),
        None => (unsigned, None),
    };
    let scale = scale.unwrap_or(0);
    if frac_part.is_some() && scale == 0 {
        return Err(DharmaError::Validation(
            "decimal scale required".to_string(),
        ));
    }
    let int_str = if int_part.is_empty() { "0" } else { int_part };
    let int_val = int_str
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid decimal".to_string()))?;
    let factor = pow10(scale)?;
    let mut mantissa = int_val
        .checked_mul(factor)
        .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    if let Some(frac) = frac_part {
        if frac.len() > scale as usize {
            return Err(DharmaError::Validation(
                "decimal scale overflow".to_string(),
            ));
        }
        let mut frac_buf = String::from(frac);
        while frac_buf.len() < scale as usize {
            frac_buf.push('0');
        }
        let frac_val = if frac_buf.is_empty() {
            0i64
        } else {
            frac_buf
                .parse::<i64>()
                .map_err(|_| DharmaError::Validation("invalid decimal".to_string()))?
        };
        mantissa = mantissa
            .checked_add(frac_val)
            .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    }
    if negative {
        Ok(-mantissa)
    } else {
        Ok(mantissa)
    }
}

fn parse_ratio_arg(raw: &str) -> Result<(i64, i64), DharmaError> {
    let trimmed = raw.trim();
    let (num_raw, den_raw) = if let Some(pair) = trimmed.split_once('/') {
        pair
    } else if let Some(pair) = trimmed.split_once(',') {
        pair
    } else {
        return Err(DharmaError::Validation("invalid ratio".to_string()));
    };
    let num = num_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid ratio".to_string()))?;
    let den = den_raw
        .trim()
        .parse::<i64>()
        .map_err(|_| DharmaError::Validation("invalid ratio".to_string()))?;
    Ok((num, den))
}

fn pow10(scale: u32) -> Result<i64, DharmaError> {
    let mut out = 1i64;
    for _ in 0..scale {
        out = out
            .checked_mul(10)
            .ok_or_else(|| DharmaError::Validation("decimal overflow".to_string()))?;
    }
    Ok(out)
}

fn is_empty_args(value: &Value) -> bool {
    matches!(value, Value::Map(map) if map.is_empty())
}

fn build_context(identity: &crate::IdentityState) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    buf[..32].copy_from_slice(identity.subject_id.as_bytes());
    let timestamp = now_timestamp() as i64;
    buf[32..40].copy_from_slice(&timestamp.to_le_bytes());
    buf
}

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn print_action_preview(preview: &ActionPreview, color: bool) {
    println!("Allowed: {}", preview.allowed);
    if let Some(reason) = &preview.reason {
        println!("Reason: {reason}");
    }
    println!("Would write: {}", preview.writes);
    print_diff(&preview.diff, color);
}

fn print_transaction_card(plan: &ActionPlan, preview: &ActionPreview, color: bool) {
    let mut lines = Vec::new();
    lines.push(format!("Subject: {}", plan.subject.to_hex()));
    lines.push(format!("Action: {}", plan.action));
    lines.push(format!("Lens: v{}", plan.ver));
    lines.push(format!("Args: {}", value_to_json(&plan.args_value)));
    lines.push(format!("Would write: {}", preview.writes));
    let header = "Transaction Card";
    let width = lines
        .iter()
        .map(|l| l.len())
        .max()
        .unwrap_or(0)
        .max(header.len());
    let border = "─".repeat(width + 2);
    println!("┌{border}┐");
    println!(
        "│ {}{} │",
        header,
        " ".repeat(width.saturating_sub(header.len()))
    );
    println!("├{border}┤");
    for line in lines {
        let padding = " ".repeat(width.saturating_sub(line.len()));
        println!("│ {line}{padding} │");
    }
    println!("└{border}┘");
    print_diff(&preview.diff, color);
}

fn paint_diff(text: &str, color: Color, enabled: bool) -> String {
    if enabled {
        format!("{}", text.with(color))
    } else {
        text.to_string()
    }
}

fn committed_ids(value: &Value) -> Vec<AssertionId> {
    let Ok(map) = crate::value::expect_map(value) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(base) = crate::value::map_get(map, "base") {
        if let Ok(bytes) = crate::value::expect_bytes(base) {
            if let Ok(id) = AssertionId::from_slice(&bytes) {
                out.push(id);
            }
        }
    }
    if let Some(overlays) = crate::value::map_get(map, "overlays") {
        if let Ok(list) = crate::value::expect_array(overlays) {
            for item in list {
                if let Ok(bytes) = crate::value::expect_bytes(item) {
                    if let Ok(id) = AssertionId::from_slice(&bytes) {
                        out.push(id);
                    }
                }
            }
        }
    }
    out
}

fn build_overlay_map(
    root: &PathBuf,
    subject: &SubjectId,
    ver: u64,
) -> Result<BTreeMap<AssertionId, Value>, DharmaError> {
    let mut overlay_by_ref: BTreeMap<AssertionId, Value> = BTreeMap::new();
    let env = dharma::env::StdEnv::new(root);
    let overlay_records = list_overlays(&env, subject)?;
    for record in overlay_records {
        let overlay = AssertionPlaintext::from_cbor(&record.bytes)?;
        if overlay.header.ver != ver {
            continue;
        }
        if let Some(ref_id) = overlay.header.refs.first() {
            overlay_by_ref.insert(*ref_id, overlay.body.clone());
        }
    }
    Ok(overlay_by_ref)
}

#[derive(Clone, Debug, Default)]
struct OverlayInfo {
    count: usize,
    decryptable: bool,
}

#[derive(Clone, Debug)]
struct OverlayRecordView {
    seq: u64,
    assertion_id: AssertionId,
    action: Option<String>,
    base: Option<AssertionId>,
}

#[derive(Clone, Debug)]
struct PeerEntry {
    addr: String,
    subject: Option<SubjectId>,
    pubkey: Option<crate::types::IdentityKey>,
    last_seen: Option<u64>,
}

fn overlay_namespace_info(
    root: &PathBuf,
    subject: &SubjectId,
    ver: u64,
) -> Result<BTreeMap<String, OverlayInfo>, DharmaError> {
    let store = Store::from_root(root);
    let env = dharma::env::StdEnv::new(root);
    let mut info: BTreeMap<String, OverlayInfo> = BTreeMap::new();
    let mut schema_cache: HashMap<SchemaId, Option<String>> = HashMap::new();
    for record in list_overlays(&env, subject)? {
        let overlay = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(value) => value,
            Err(_) => {
                let entry = info.entry("unknown".to_string()).or_default();
                entry.count += 1;
                continue;
            }
        };
        if overlay.header.ver != ver {
            continue;
        }
        let namespace = schema_cache
            .entry(overlay.header.schema)
            .or_insert_with(|| schema_namespace(&store, &overlay.header.schema))
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let entry = info.entry(namespace).or_default();
        entry.count += 1;
        entry.decryptable = true;
    }
    Ok(info)
}

fn overlay_records_for_namespace(
    root: &PathBuf,
    subject: &SubjectId,
    ver: u64,
    namespace: &str,
) -> Result<Vec<OverlayRecordView>, DharmaError> {
    let store = Store::from_root(root);
    let env = dharma::env::StdEnv::new(root);
    let mut out = Vec::new();
    let mut schema_cache: HashMap<SchemaId, Option<String>> = HashMap::new();
    for record in list_overlays(&env, subject)? {
        let overlay = match AssertionPlaintext::from_cbor(&record.bytes) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if overlay.header.ver != ver {
            continue;
        }
        let ns = schema_cache
            .entry(overlay.header.schema)
            .or_insert_with(|| schema_namespace(&store, &overlay.header.schema))
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        if ns != namespace {
            continue;
        }
        let action = overlay
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&overlay.header.typ)
            .to_string();
        let base = overlay.header.refs.first().copied();
        out.push(OverlayRecordView {
            seq: record.seq,
            assertion_id: record.assertion_id,
            action: Some(action),
            base,
        });
    }
    Ok(out)
}

fn overlay_info_value(namespace: &str, info: &OverlayInfo, enabled: bool) -> Value {
    let mut entries = Vec::new();
    entries.push((
        Value::Text("namespace".to_string()),
        Value::Text(namespace.to_string()),
    ));
    entries.push((
        Value::Text("count".to_string()),
        Value::Integer((info.count as i64).into()),
    ));
    entries.push((
        Value::Text("decryptable".to_string()),
        Value::Bool(info.decryptable),
    ));
    entries.push((Value::Text("enabled".to_string()), Value::Bool(enabled)));
    Value::Map(entries)
}

fn overlay_record_value(entry: &OverlayRecordView) -> Value {
    let mut entries = Vec::new();
    entries.push((
        Value::Text("seq".to_string()),
        Value::Integer((entry.seq as i64).into()),
    ));
    entries.push((
        Value::Text("assertion".to_string()),
        Value::Bytes(entry.assertion_id.as_bytes().to_vec()),
    ));
    if let Some(action) = &entry.action {
        entries.push((
            Value::Text("action".to_string()),
            Value::Text(action.clone()),
        ));
    }
    if let Some(base) = &entry.base {
        entries.push((
            Value::Text("ref".to_string()),
            Value::Bytes(base.as_bytes().to_vec()),
        ));
    }
    Value::Map(entries)
}

fn schema_namespace(store: &Store, schema: &SchemaId) -> Option<String> {
    let envelope_id = EnvelopeId::from_bytes(*schema.as_bytes());
    let bytes = store.get_object(&envelope_id).ok()?;
    let schema = CqrsSchema::from_cbor(&bytes).ok()?;
    Some(schema.namespace)
}

fn overlay_enabled_for_schema(schema: &CqrsSchema, disabled: &BTreeSet<String>) -> bool {
    !disabled.contains(&schema.namespace)
}

fn filter_private_fields(schema: &CqrsSchema, value: &Value) -> Result<Value, DharmaError> {
    let map = crate::value::expect_map(value)?;
    let mut out = Vec::new();
    for (k, v) in map {
        if let Value::Text(name) = k {
            if let Some(field) = schema.fields.get(name) {
                if field.visibility == Visibility::Private {
                    continue;
                }
            }
        }
        out.push((k.clone(), v.clone()));
    }
    Ok(Value::Map(out))
}

fn load_overlay_disabled(root: &PathBuf) -> Result<BTreeSet<String>, DharmaError> {
    let path = root.join(OVERLAY_DISABLED);
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let contents = fs::read_to_string(path)?;
    let mut out = BTreeSet::new();
    for line in contents.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        out.insert(line.to_string());
    }
    Ok(out)
}

fn save_overlay_disabled(root: &PathBuf, disabled: &BTreeSet<String>) -> Result<(), DharmaError> {
    let path = root.join(OVERLAY_DISABLED);
    if disabled.is_empty() {
        if path.exists() {
            fs::remove_file(path)?;
        }
        return Ok(());
    }
    let contents = disabled.iter().cloned().collect::<Vec<_>>().join("\n") + "\n";
    fs::write(path, contents)?;
    Ok(())
}

fn load_peers(root: &PathBuf) -> Result<Vec<PeerEntry>, DharmaError> {
    let path = root.join(PEERS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let contents = fs::read_to_string(path)?;
    let mut out = Vec::new();
    for line in contents.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        let addr = parts[0].to_string();
        let subject = parts.get(1).and_then(|v| SubjectId::from_hex(v).ok());
        let pubkey = parts
            .get(2)
            .and_then(|v| crate::types::hex_decode(v).ok())
            .and_then(|bytes| crate::types::IdentityKey::from_slice(&bytes).ok());
        let last_seen = parts.get(3).and_then(|v| v.parse::<u64>().ok());
        out.push(PeerEntry {
            addr,
            subject,
            pubkey,
            last_seen,
        });
    }
    Ok(out)
}

fn save_peers(root: &PathBuf, peers: &[PeerEntry]) -> Result<(), DharmaError> {
    let path = root.join(PEERS_FILE);
    if peers.is_empty() {
        if path.exists() {
            fs::remove_file(path)?;
        }
        return Ok(());
    }
    let mut lines = Vec::new();
    for peer in peers {
        let subject = peer
            .subject
            .as_ref()
            .map(|s| s.to_hex())
            .unwrap_or_else(|| "-".to_string());
        let pubkey = peer
            .pubkey
            .as_ref()
            .map(|k| k.to_hex())
            .unwrap_or_else(|| "-".to_string());
        let last_seen = peer
            .last_seen
            .map(|t| t.to_string())
            .unwrap_or_else(|| "-".to_string());
        lines.push(format!(
            "{} {} {} {}",
            peer.addr, subject, pubkey, last_seen
        ));
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

fn upsert_peer(root: &PathBuf, addr: &str, last_seen: u64) -> Result<(), DharmaError> {
    let mut peers = load_peers(root)?;
    let mut found = false;
    for peer in peers.iter_mut() {
        if peer.addr == addr {
            peer.last_seen = Some(last_seen);
            found = true;
            break;
        }
    }
    if !found {
        peers.push(PeerEntry {
            addr: addr.to_string(),
            subject: None,
            pubkey: None,
            last_seen: Some(last_seen),
        });
    }
    save_peers(root, &peers)
}

fn peer_value(ctx: &ReplContext, peer: &PeerEntry, verbose: bool) -> Value {
    let mut entries = Vec::new();
    entries.push((
        Value::Text("addr".to_string()),
        Value::Text(peer.addr.clone()),
    ));
    if let Some(subject) = &peer.subject {
        entries.push((
            Value::Text("subject".to_string()),
            Value::Bytes(subject.as_bytes().to_vec()),
        ));
    }
    if let Some(pubkey) = &peer.pubkey {
        entries.push((
            Value::Text("pubkey".to_string()),
            Value::Bytes(pubkey.as_bytes().to_vec()),
        ));
    }
    if let Some(last) = peer.last_seen {
        entries.push((
            Value::Text("last_seen".to_string()),
            Value::Integer((last as i64).into()),
        ));
    }
    if verbose {
        entries.push((
            Value::Text("trust".to_string()),
            Value::Text(peer_trust(ctx, peer)),
        ));
    }
    Value::Map(entries)
}

fn peer_trust(ctx: &ReplContext, peer: &PeerEntry) -> String {
    let Some(subject) = peer.subject else {
        return "unknown".to_string();
    };
    let Some(pubkey) = &peer.pubkey else {
        return "unknown".to_string();
    };
    let policy = PeerPolicy::load(&ctx.data_dir);
    if policy.allows(subject, *pubkey) {
        "allow".to_string()
    } else {
        "deny".to_string()
    }
}

fn sync_now(
    ctx: &mut ReplContext,
    subject: Option<SubjectId>,
    json: bool,
    verbose: bool,
) -> Result<(), DharmaError> {
    let peers = load_peers(&ctx.data_dir)?;
    if peers.is_empty() {
        println!("No peers configured. Use `connect <addr>` to add one.");
        return Ok(());
    }
    let use_spinner = !json && io::stdout().is_terminal();
    let spinner = if use_spinner {
        let bar = ProgressBar::new_spinner();
        let style = ProgressStyle::with_template("{spinner} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner());
        bar.set_style(style);
        bar.enable_steady_tick(std::time::Duration::from_millis(120));
        Some(bar)
    } else {
        None
    };
    let mut results = Vec::new();
    for peer in peers {
        if let Some(bar) = &spinner {
            bar.set_message(format!("Syncing {}", peer.addr));
        }
        let result = sync_with_peer(ctx, &peer.addr, subject, verbose && !json);
        match result {
            Ok(()) => {
                results.push(sync_result_value(&peer.addr, None));
            }
            Err(err) => {
                results.push(sync_result_value(&peer.addr, Some(err.to_string())));
            }
        }
    }
    if let Some(bar) = spinner {
        bar.finish_with_message("Sync complete.");
    }
    if json {
        println!("{}", value_to_json(&Value::Array(results)));
    }
    Ok(())
}

fn sync_result_value(addr: &str, err: Option<String>) -> Value {
    let mut entries = Vec::new();
    entries.push((
        Value::Text("addr".to_string()),
        Value::Text(addr.to_string()),
    ));
    if let Some(err) = err {
        entries.push((
            Value::Text("status".to_string()),
            Value::Text("error".to_string()),
        ));
        entries.push((Value::Text("error".to_string()), Value::Text(err)));
    } else {
        entries.push((
            Value::Text("status".to_string()),
            Value::Text("ok".to_string()),
        ));
    }
    Value::Map(entries)
}

fn sync_with_peer(
    ctx: &mut ReplContext,
    addr: &str,
    subject_override: Option<SubjectId>,
    verbose: bool,
) -> Result<(), DharmaError> {
    let identity = ctx
        .identity
        .as_ref()
        .ok_or_else(|| DharmaError::Validation("identity locked".to_string()))?;
    let store = Store::from_root(&ctx.data_dir);
    let mut stream = TcpStream::connect(addr)?;
    let session = crate::net::handshake::client_handshake(&mut stream, identity)?;
    let mut legacy_keys: HashMap<SubjectId, [u8; 32]> = HashMap::new();
    legacy_keys.insert(identity.subject_id, identity.subject_key);
    let mut keys = dharma::keys::Keyring::from_subject_keys(&legacy_keys);
    keys.insert_hpke_secret(identity.public_key, identity.noise_sk);
    let mut index = FrontierIndex::build(&store, &keys)?;
    let policy = OverlayPolicy::load(store.root());
    let claims = PeerClaims::default();
    let access = OverlayAccess::new(&policy, None, false, &claims);

    let result = with_subscription_override(&ctx.data_dir, subject_override, || {
        crate::net::sync::sync_loop_with(
            &mut stream,
            session,
            &store,
            &mut index,
            &mut keys,
            identity,
            &access,
            crate::net::sync::SyncOptions {
                relay: false,
                ad_store: None,
                local_subs: None,
                verbose,
                exit_on_idle: true,
                trace: None,
            },
        )
    });
    if result.is_ok() {
        upsert_peer(&ctx.data_dir, addr, now_timestamp())?;
    }
    result
}

fn with_subscription_override<F, T>(
    root: &PathBuf,
    subject: Option<SubjectId>,
    f: F,
) -> Result<T, DharmaError>
where
    F: FnOnce() -> Result<T, DharmaError>,
{
    let Some(subject) = subject else {
        return f();
    };
    let policy_path = root.join("subscriptions.policy");
    if policy_path.exists() {
        eprintln!("Warning: subscriptions.policy present; cannot override for subject sync.");
        return f();
    }
    let allow_path = root.join("subscriptions.allow");
    let backup = if allow_path.exists() {
        Some(fs::read_to_string(&allow_path)?)
    } else {
        None
    };
    fs::write(&allow_path, format!("{}\n", subject.to_hex()))?;
    let result = f();
    match backup {
        Some(contents) => {
            fs::write(&allow_path, contents)?;
        }
        None => {
            let _ = fs::remove_file(&allow_path);
        }
    }
    result
}

fn discovery_state(ctx: &ReplContext) -> (bool, bool) {
    let path = ctx.data_dir.join(DISCOVERY_FILE);
    if let Ok(contents) = fs::read_to_string(path) {
        let enabled = contents.trim() != "off";
        return (enabled, true);
    }
    let enabled = match ctx.profile {
        Profile::HighSec => false,
        _ => true,
    };
    (enabled, false)
}

fn set_discovery_state(root: &PathBuf, enabled: bool) -> Result<(), DharmaError> {
    let path = root.join(DISCOVERY_FILE);
    let value = if enabled { "on" } else { "off" };
    fs::write(path, format!("{value}\n"))?;
    Ok(())
}

fn load_assertions_for_ver(
    env: &dyn dharma::env::Env,
    subject: &SubjectId,
    ver: u64,
) -> Result<std::collections::HashMap<AssertionId, AssertionPlaintext>, DharmaError> {
    let mut map = std::collections::HashMap::new();
    for record in list_assertions(env, subject)? {
        let assertion = AssertionPlaintext::from_cbor(&record.bytes)?;
        if assertion.header.ver != ver {
            continue;
        }
        map.insert(record.assertion_id, assertion);
    }
    Ok(map)
}

fn value_at_path(value: &Value, path: &str) -> Option<Value> {
    let mut current = value.clone();
    if path.is_empty() {
        return Some(current);
    }
    for segment in path.split('.') {
        match current {
            Value::Map(entries) => {
                let mut next = None;
                for (k, v) in entries {
                    if let Value::Text(key) = k {
                        if key == segment {
                            next = Some(v.clone());
                            break;
                        }
                    }
                }
                if let Some(val) = next {
                    current = val;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }
    Some(current)
}

fn prove_object(ctx: &ReplContext, store: &Store, bytes: &[u8]) -> Result<Value, DharmaError> {
    let mut canonical_ok = false;
    let mut signature_ok = None;
    let mut deps_missing: Vec<AssertionId> = Vec::new();
    let mut schema_ok = None;
    let mut contract_status: Option<String> = None;
    let mut contract_reason: Option<String> = None;
    let mut final_status = "REJECTED".to_string();
    let mut cqrs_schema: Option<CqrsSchema> = None;

    let mut assertion_opt = None;
    if let Ok(assertion) = AssertionPlaintext::from_cbor(bytes) {
        canonical_ok = true;
        assertion_opt = Some(assertion);
    } else if let Ok(envelope) = crate::envelope::AssertionEnvelope::from_cbor(bytes) {
        canonical_ok = true;
        if let Some(identity) = &ctx.identity {
            if let Ok(plaintext) =
                crate::envelope::decrypt_assertion(&envelope, &identity.subject_key)
            {
                if let Ok(assertion) = AssertionPlaintext::from_cbor(&plaintext) {
                    assertion_opt = Some(assertion);
                }
            }
        }
    }

    if let Some(assertion) = &assertion_opt {
        signature_ok = Some(assertion.verify_signature()?);
        for dep in assertion
            .header
            .refs
            .iter()
            .copied()
            .chain(assertion.header.prev)
        {
            let mut missing = true;
            if let Some(env_id) = store.lookup_envelope(&dep)? {
                if store.get_object_any(&env_id)?.is_some() {
                    missing = false;
                }
            }
            if missing {
                deps_missing.push(dep);
            }
        }
        if let Ok(schema_bytes) =
            store.get_object(&EnvelopeId::from_bytes(*assertion.header.schema.as_bytes()))
        {
            if let Ok(schema) = crate::schema::parse_schema(&schema_bytes) {
                schema_ok = Some(
                    crate::schema::validate_body(&schema, &assertion.header.typ, &assertion.body)
                        .is_ok(),
                );
            }
            if let Ok(schema) = CqrsSchema::from_cbor(&schema_bytes) {
                cqrs_schema = Some(schema);
            }
        }
        if let Some(schema) = cqrs_schema.as_ref() {
            if let Some(action_name) = assertion.header.typ.strip_prefix("action.") {
                if let Some(action_schema) = schema.action(action_name) {
                    let overlay_map = build_overlay_map(
                        &store.root().to_path_buf(),
                        &assertion.header.sub,
                        assertion.header.ver,
                    )?;
                    let assertion_id = assertion.assertion_id()?;
                    let overlay = overlay_map.get(&assertion_id);
                    let merged = merge_args(&assertion.body, overlay)?;
                    schema_ok = Some(validate_args(action_schema, &merged).is_ok());
                }
            }
        }
        if let Ok(contract_bytes) = store.get_object(&EnvelopeId::from_bytes(
            *assertion.header.contract.as_bytes(),
        )) {
            if let Some(schema) = cqrs_schema.as_ref() {
                if assertion.header.typ.starts_with("action.") {
                    let (status, reason) =
                        prove_cqrs_contract(store, schema, &contract_bytes, assertion)?;
                    contract_status = Some(status);
                    contract_reason = reason;
                }
            }
            if contract_status.is_none() {
                let engine = crate::contract::ContractEngine::new(contract_bytes);
                let context = Value::Map(vec![
                    (
                        Value::Text("subject".to_string()),
                        Value::Bytes(assertion.header.sub.as_bytes().to_vec()),
                    ),
                    (Value::Text("accepted".to_string()), Value::Array(vec![])),
                    (Value::Text("lookup".to_string()), Value::Map(vec![])),
                ]);
                let context_bytes = crate::cbor::encode_canonical_value(&context)?;
                let result =
                    engine.validate_with_env(store.env(), &assertion.to_cbor()?, &context_bytes)?;
                contract_status = Some(format!("{:?}", result.status));
                contract_reason = result.reason.clone();
            }
        }
        final_status = if !deps_missing.is_empty() {
            "PENDING".to_string()
        } else if signature_ok == Some(true)
            && schema_ok == Some(true)
            && contract_status.as_deref() == Some("Accept")
        {
            "ACCEPTED".to_string()
        } else if contract_status.as_deref() == Some("Pending") {
            "PENDING".to_string()
        } else {
            "REJECTED".to_string()
        };
    } else if canonical_ok {
        final_status = "PENDING".to_string();
    }

    let mut entries = Vec::new();
    entries.push((
        Value::Text("canonical_decode".to_string()),
        Value::Bool(canonical_ok),
    ));
    if let Some(sig) = signature_ok {
        entries.push((
            Value::Text("signature_verify".to_string()),
            Value::Bool(sig),
        ));
    }
    entries.push((
        Value::Text("deps_missing".to_string()),
        Value::Array(
            deps_missing
                .iter()
                .map(|id| Value::Bytes(id.as_bytes().to_vec()))
                .collect(),
        ),
    ));
    if let Some(schema_ok) = schema_ok {
        entries.push((
            Value::Text("schema_validation".to_string()),
            Value::Bool(schema_ok),
        ));
    }
    if let Some(status) = contract_status {
        entries.push((
            Value::Text("contract_validation".to_string()),
            Value::Text(status),
        ));
    }
    if let Some(reason) = contract_reason {
        entries.push((
            Value::Text("contract_reason".to_string()),
            Value::Text(reason),
        ));
    }
    entries.push((
        Value::Text("final_status".to_string()),
        Value::Text(final_status),
    ));
    Ok(Value::Map(entries))
}

fn prove_cqrs_contract(
    store: &Store,
    schema: &CqrsSchema,
    contract_bytes: &[u8],
    target: &AssertionPlaintext,
) -> Result<(String, Option<String>), DharmaError> {
    let subject = target.header.sub;
    let ver = target.header.ver;
    let target_id = target.assertion_id()?;
    let env = store.env();
    let assertions = load_assertions_for_ver(env, &subject, ver)?;
    let order = crate::validation::order_assertions(&assertions)?;
    let overlay_map = build_overlay_map(&store.root().to_path_buf(), &subject, ver)?;
    let vm = RuntimeVm::new(contract_bytes.to_vec());
    let mut memory = default_state_memory(schema);
    for assertion_id in order {
        let assertion = assertions
            .get(&assertion_id)
            .ok_or_else(|| DharmaError::Validation("missing assertion".to_string()))?;
        if assertion.header.typ == "core.merge" {
            continue;
        }
        let action_name = assertion
            .header
            .typ
            .strip_prefix("action.")
            .unwrap_or(&assertion.header.typ);
        let action_schema = schema
            .action(action_name)
            .ok_or_else(|| DharmaError::Schema("unknown action".to_string()))?;
        let action_index = crate::runtime::cqrs::action_index(schema, action_name)?;
        let overlay = overlay_map.get(&assertion_id);
        let merged = merge_args(&assertion.body, overlay)?;
        let args_buffer = crate::runtime::cqrs::encode_args_buffer(
            action_schema,
            &schema.structs,
            action_index,
            &merged,
            true,
        )?;
        let context = context_buffer_for_assertion(assertion);
        if assertion_id == target_id {
            match vm.validate(env, &mut memory, &args_buffer, Some(&context)) {
                Ok(()) => return Ok(("Accept".to_string(), None)),
                Err(err) => return Ok(("Reject".to_string(), Some(err.to_string()))),
            }
        }
        vm.reduce(env, &mut memory, &args_buffer, Some(&context))?;
    }
    Ok((
        "Pending".to_string(),
        Some("target assertion not found".to_string()),
    ))
}

fn context_buffer_for_assertion(assertion: &AssertionPlaintext) -> Vec<u8> {
    let mut buf = vec![0u8; 40];
    let signer = signer_from_meta(&assertion.header.meta).unwrap_or(assertion.header.sub);
    buf[..32].copy_from_slice(signer.as_bytes());
    let ts = assertion.header.ts.unwrap_or(0);
    buf[32..40].copy_from_slice(&ts.to_le_bytes());
    buf
}

fn print_proof_report(report: &Value) {
    let Ok(map) = crate::value::expect_map(report) else {
        println!("Invalid proof report.");
        return;
    };
    if let Some(value) = crate::value::map_get(map, "canonical_decode") {
        println!("canonical decode: {}", value_to_json(value));
    }
    if let Some(value) = crate::value::map_get(map, "signature_verify") {
        println!("signature verify: {}", value_to_json(value));
    }
    if let Some(value) = crate::value::map_get(map, "deps_missing") {
        println!("deps missing: {}", value_to_json(value));
    }
    if let Some(value) = crate::value::map_get(map, "schema_validation") {
        println!("schema validation: {}", value_to_json(value));
    }
    if let Some(value) = crate::value::map_get(map, "contract_validation") {
        println!("contract validation: {}", value_to_json(value));
    }
    if let Some(value) = crate::value::map_get(map, "contract_reason") {
        println!("contract reason: {}", value_to_json(value));
    }
    if let Some(value) = crate::value::map_get(map, "final_status") {
        println!("final status: {}", value_to_json(value));
    }
}

fn print_subject_status(
    ctx: &ReplContext,
    store: &Store,
    index: &FrontierIndex,
    subject: &SubjectId,
    verbose: bool,
) -> Result<(), DharmaError> {
    println!("Subject: {}", subject.to_hex());
    let tips = index.get_tips_for_ver(subject, ctx.current_lens);
    if tips.is_empty() {
        println!("Tips: <none>");
    } else {
        println!("Tips:");
        for tip in tips {
            let seq = index.tip_seq(subject, &tip).unwrap_or(0);
            println!("  {} (seq {})", tip.to_hex(), seq);
        }
    }
    let records = list_assertions(store.env(), subject)?;
    let counts = structural_counts(&records)?;
    println!("Accepted: {}", counts.accepted);
    println!(
        "Pending: {}",
        counts.pending + index.pending_objects().len()
    );
    println!("Rejected: {}", counts.rejected);
    if verbose {
        let overlays = list_overlays(store.env(), subject)?;
        println!("Assertions: {}", records.len());
        println!("Overlays: {}", overlays.len());
        if let Some(max_seq) = index.max_seq_for_ver(subject, ctx.current_lens) {
            println!("Max seq (lens {}): {}", ctx.current_lens, max_seq);
        }
    }
    Ok(())
}

fn print_open_assertion(envelope_id: &EnvelopeId, assertion: &AssertionPlaintext) {
    println!("Object {}", short_hex(&envelope_id.to_hex(), 12));
    println!("typ: {}", assertion.header.typ);
    println!("seq: {}", assertion.header.seq);
    println!("sub: {}", short_hex(&assertion.header.sub.to_hex(), 12));
    println!("body: {}", value_to_json(&assertion.body));
}

fn print_assertion(assertion: &crate::assertion::AssertionPlaintext) {
    print_assertion_header(assertion);
    println!("  body: {}", value_to_json(&assertion.body));
}

fn print_assertion_header(assertion: &crate::assertion::AssertionPlaintext) {
    println!("  type: {}", assertion.header.typ);
    println!("  ver: {}", assertion.header.ver);
    println!("  seq: {}", assertion.header.seq);
    println!("  auth: {}", assertion.header.auth.to_hex());
    println!(
        "  prev: {}",
        assertion
            .header
            .prev
            .map(|p| p.to_hex())
            .unwrap_or_else(|| "none".to_string())
    );
    if !assertion.header.refs.is_empty() {
        let refs = assertion
            .header
            .refs
            .iter()
            .map(|r| r.to_hex())
            .collect::<Vec<_>>()
            .join(", ");
        println!("  refs: [{refs}]");
    }
}

fn load_contract_ids_for_ver(
    root: &PathBuf,
    ver: u64,
) -> Result<(SchemaId, ContractId), DharmaError> {
    let config = std::fs::read_to_string(root.join("dharma.toml")).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            DharmaError::Config("missing dharma.toml".to_string())
        } else {
            DharmaError::from(err)
        }
    })?;
    let mut schema_hex = None;
    let mut contract_hex = None;
    let schema_key = format!("schema_v{ver}");
    let contract_key = format!("contract_v{ver}");
    for line in config.lines() {
        let line = line.trim();
        if let Some((k, v)) = line.split_once('=') {
            let key = k.trim();
            let value = v.trim().trim_matches('"');
            match key {
                k if k == schema_key => schema_hex = Some(value.to_string()),
                k if k == contract_key => contract_hex = Some(value.to_string()),
                "schema" if ver == crate::assertion::DEFAULT_DATA_VERSION => {
                    schema_hex = Some(value.to_string())
                }
                "contract" if ver == crate::assertion::DEFAULT_DATA_VERSION => {
                    contract_hex = Some(value.to_string())
                }
                _ => {}
            }
        }
    }
    let schema_hex = schema_hex
        .ok_or_else(|| DharmaError::Config("missing schema in dharma.toml".to_string()))?;
    let contract_hex = contract_hex
        .ok_or_else(|| DharmaError::Config("missing contract in dharma.toml".to_string()))?;
    Ok((
        SchemaId::from_hex(&schema_hex)?,
        ContractId::from_hex(&contract_hex)?,
    ))
}

fn load_schema_bytes(root: &PathBuf, id: &SchemaId) -> Result<Vec<u8>, DharmaError> {
    let path = root.join("objects").join(format!("{}.obj", id.to_hex()));
    std::fs::read(path).map_err(Into::into)
}

fn load_contract_bytes(root: &PathBuf, id: &ContractId) -> Result<Vec<u8>, DharmaError> {
    let path = root.join("objects").join(format!("{}.obj", id.to_hex()));
    std::fs::read(path).map_err(Into::into)
}

fn update_config_for_lens(
    root: &PathBuf,
    schema: &SchemaId,
    contract: &ContractId,
    reactor: Option<&EnvelopeId>,
    ver: u64,
) -> Result<(), DharmaError> {
    let path = root.join("dharma.toml");
    let schema_key = format!("schema_v{ver}");
    let contract_key = format!("contract_v{ver}");
    let reactor_key = format!("reactor_v{ver}");
    let mut lines = Vec::new();
    if path.exists() {
        let contents = fs::read_to_string(&path)?;
        for line in contents.lines() {
            let trimmed = line.trim();
            let key = trimmed.split('=').next().unwrap_or("").trim();
            if key == schema_key || key == contract_key || key == reactor_key {
                continue;
            }
            if ver == crate::assertion::DEFAULT_DATA_VERSION
                && (key == "schema" || key == "contract" || key == "reactor")
            {
                continue;
            }
            if !trimmed.is_empty() {
                lines.push(line.to_string());
            }
        }
    }
    lines.push(format!("{schema_key} = \"{}\"", schema.to_hex()));
    lines.push(format!("{contract_key} = \"{}\"", contract.to_hex()));
    if let Some(reactor) = reactor {
        lines.push(format!("{reactor_key} = \"{}\"", reactor.to_hex()));
    }
    if ver == crate::assertion::DEFAULT_DATA_VERSION {
        lines.push(format!("schema = \"{}\"", schema.to_hex()));
        lines.push(format!("contract = \"{}\"", contract.to_hex()));
        if let Some(reactor) = reactor {
            lines.push(format!("reactor = \"{}\"", reactor.to_hex()));
        }
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

fn current_subject_or_identity(ctx: &ReplContext) -> Result<SubjectId, DharmaError> {
    if let Some(subject) = ctx.current_subject {
        return Ok(subject);
    }
    if let Some(identity) = &ctx.identity {
        return Ok(identity.subject_id);
    }
    Err(DharmaError::Validation("no subject selected".to_string()))
}

fn resolve_assertion_id_for_subject(
    ctx: &ReplContext,
    token: &str,
) -> Result<AssertionId, DharmaError> {
    let subject = current_subject_or_identity(ctx)?;
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let records = list_assertions(&env, &subject)?;
    resolve_assertion_id_from_records(&records, token)
}

fn resolve_assertion_id_from_records(
    records: &[AssertionRecord],
    token: &str,
) -> Result<AssertionId, DharmaError> {
    if let Ok(assertion_id) = AssertionId::from_hex(token) {
        if records
            .iter()
            .any(|record| record.assertion_id == assertion_id)
        {
            return Ok(assertion_id);
        }
    }
    if let Ok(envelope_id) = EnvelopeId::from_hex(token) {
        if let Some(record) = records
            .iter()
            .find(|record| record.envelope_id == envelope_id)
        {
            return Ok(record.assertion_id);
        }
    }

    let mut matches = records
        .iter()
        .filter(|record| record.assertion_id.to_hex().starts_with(token))
        .map(|record| record.assertion_id)
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        return Ok(matches[0]);
    }
    if matches.is_empty() {
        matches = records
            .iter()
            .filter(|record| record.envelope_id.to_hex().starts_with(token))
            .map(|record| record.assertion_id)
            .collect::<Vec<_>>();
        if matches.len() == 1 {
            return Ok(matches[0]);
        }
        if matches.is_empty() {
            return Err(DharmaError::Validation("unknown assertion id".to_string()));
        }
    }
    Err(DharmaError::Validation(
        "ambiguous assertion id".to_string(),
    ))
}

fn resolve_envelope_id_any(store: &Store, token: &str) -> Result<EnvelopeId, DharmaError> {
    if let Ok(envelope_id) = EnvelopeId::from_hex(token) {
        if store.get_object_any(&envelope_id)?.is_some() {
            return Ok(envelope_id);
        }
    }
    if let Ok(assertion_id) = AssertionId::from_hex(token) {
        if let Some(envelope_id) = store.lookup_envelope(&assertion_id)? {
            return Ok(envelope_id);
        }
    }

    let matches = store
        .list_objects()?
        .into_iter()
        .filter(|envelope_id| envelope_id.to_hex().starts_with(token))
        .collect::<Vec<_>>();
    if matches.len() == 1 {
        return Ok(matches[0]);
    }
    if !matches.is_empty() {
        return Err(DharmaError::Validation("ambiguous id".to_string()));
    }

    let mut matches = Vec::new();
    for subject in store.list_subjects()? {
        let records = list_assertions(store.env(), &subject)?;
        for record in records {
            if record.assertion_id.to_hex().starts_with(token)
                || record.envelope_id.to_hex().starts_with(token)
            {
                matches.push(record.envelope_id);
            }
        }
    }
    matches.sort_by_key(|id| id.to_hex());
    matches.dedup();
    if matches.len() == 1 {
        return Ok(matches[0]);
    }
    if matches.is_empty() {
        return Err(DharmaError::Validation("unknown id".to_string()));
    }
    Err(DharmaError::Validation("ambiguous id".to_string()))
}

fn parse_show_args<'a>(args: &'a [&'a str]) -> Result<(Option<&'a str>, bool, bool), DharmaError> {
    let mut token = None;
    let mut json = false;
    let mut raw = false;
    for arg in args {
        match *arg {
            "--json" => json = true,
            "--raw" => raw = true,
            _ => {
                if token.is_none() {
                    token = Some(*arg);
                }
            }
        }
    }
    Ok((token, json, raw))
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(hex_char(b >> 4));
        out.push(hex_char(b & 0x0f));
    }
    out
}

fn hex_char(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => '0',
    }
}

#[derive(Clone)]
enum ContractSource {
    Package { name: String, ver: u64 },
    Local,
}

impl ContractSource {
    fn label(&self) -> String {
        match self {
            ContractSource::Package { name, ver } => format!("pkg:{name}@{ver}"),
            ContractSource::Local => "local".to_string(),
        }
    }
}

#[derive(Clone)]
struct ContractEntry {
    name: String,
    version: String,
    aggregate: String,
    schema_id: SchemaId,
    contract_id: ContractId,
    reactor_id: Option<EnvelopeId>,
    schema: CqrsSchema,
    source: ContractSource,
}

#[derive(Tabled)]
struct ContractRow {
    #[tabled(rename = "Contract")]
    name: String,
    #[tabled(rename = "Version")]
    version: String,
    #[tabled(rename = "Schema ID")]
    schema: String,
    #[tabled(rename = "Contract ID")]
    contract: String,
    #[tabled(rename = "Source")]
    source: String,
}

#[derive(Tabled)]
struct FieldRow {
    #[tabled(rename = "Field")]
    name: String,
    #[tabled(rename = "Visibility")]
    visibility: String,
    #[tabled(rename = "Type")]
    typ: String,
    #[tabled(rename = "Default")]
    default: String,
}

#[derive(Tabled)]
struct ActionArgRow {
    #[tabled(rename = "Arg")]
    arg: String,
    #[tabled(rename = "Type")]
    typ: String,
    #[tabled(rename = "Visibility")]
    visibility: String,
}

#[derive(Tabled)]
struct TableFieldRow {
    #[tabled(rename = "Field")]
    name: String,
    #[tabled(rename = "Type")]
    typ: String,
}

#[derive(Tabled)]
struct WhyRow {
    #[tabled(rename = "Seq")]
    seq: u64,
    #[tabled(rename = "Assertion")]
    assertion: String,
    #[tabled(rename = "Action")]
    action: String,
    #[tabled(rename = "Author")]
    author: String,
    #[tabled(rename = "Time")]
    time: String,
}

fn collect_contracts(ctx: &ReplContext) -> Result<Vec<ContractEntry>, DharmaError> {
    let mut out = Vec::new();
    out.extend(collect_package_contracts(&ctx.data_dir)?);
    out.extend(collect_local_contracts()?);
    out.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| compare_version(&a.version, &b.version))
    });
    Ok(out)
}

pub(crate) fn list_contract_names(data_dir: &PathBuf) -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(entries) = collect_package_contracts(data_dir) {
        for entry in entries {
            names.push(entry.name);
        }
    }
    if let Ok(entries) = collect_local_contracts() {
        for entry in entries {
            names.push(entry.name);
        }
    }
    names.sort();
    names.dedup();
    names
}

pub(crate) fn load_schema_for_lens(
    data_dir: &PathBuf,
    lens: u64,
) -> Result<CqrsSchema, DharmaError> {
    let (schema_id, _) = load_contract_ids_for_ver(data_dir, lens)?;
    let schema_bytes = load_schema_bytes(data_dir, &schema_id)?;
    CqrsSchema::from_cbor(&schema_bytes)
}

fn resolve_contract_by_name(ctx: &ReplContext, name: &str) -> Result<ContractEntry, DharmaError> {
    if let Some(entry) = select_contract_by_name(collect_package_contracts(&ctx.data_dir)?, name) {
        return Ok(entry);
    }
    if let Some(entry) = select_contract_by_name(collect_local_contracts()?, name) {
        return Ok(entry);
    }
    Err(DharmaError::Validation(format!(
        "unknown contract '{name}'"
    )))
}

fn parse_contract_table_name(name: &str, default_lens: u64) -> Result<(String, u64), DharmaError> {
    if let Some((base, lens)) = name.rsplit_once("@v") {
        if base.is_empty() {
            return Err(DharmaError::Validation("missing contract name".to_string()));
        }
        let lens = lens
            .parse::<u64>()
            .map_err(|_| DharmaError::Validation("invalid lens".to_string()))?;
        return Ok((base.to_string(), lens));
    }
    Ok((name.to_string(), default_lens))
}

fn parse_contract_table_spec_name(
    name: &str,
    default_lens: u64,
) -> Result<(String, u64, dharma::dharmaq::ContractTableKind), DharmaError> {
    let mut kind = dharma::dharmaq::ContractTableKind::State;
    let mut base = name;
    if let Some(stripped) = name.strip_suffix(".assertions") {
        kind = dharma::dharmaq::ContractTableKind::Assertions;
        base = stripped;
    }
    let (contract, lens) = parse_contract_table_name(base, default_lens)?;
    Ok((contract, lens, kind))
}

fn subject_schema_id(
    ctx: &ReplContext,
    subject: &SubjectId,
) -> Result<Option<SchemaId>, DharmaError> {
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    let records = list_assertions(&env, subject)?;
    for record in records.iter().rev() {
        if let Ok(assertion) = AssertionPlaintext::from_cbor(&record.bytes) {
            return Ok(Some(assertion.header.schema));
        }
    }
    Ok(None)
}

fn contract_entry_for_schema(
    ctx: &ReplContext,
    schema_id: &SchemaId,
) -> Result<Option<ContractEntry>, DharmaError> {
    let mut entries = collect_contracts(ctx)?;
    entries.retain(|entry| entry.schema_id == *schema_id);
    if entries.is_empty() {
        return Ok(None);
    }
    entries.sort_by(|a, b| compare_version(&a.version, &b.version));
    Ok(entries.pop())
}

fn ensure_contract_table(
    ctx: &ReplContext,
    entry: &ContractEntry,
    lens: u64,
    kind: dharma::dharmaq::ContractTableKind,
) -> Result<String, DharmaError> {
    let disabled = load_overlay_disabled(&ctx.data_dir)?;
    let include_private = overlay_enabled_for_schema(&entry.schema, &disabled);
    let spec = dharma::dharmaq::ContractTableSpec {
        name: entry.name.clone(),
        lens,
        schema_id: entry.schema_id,
        contract_id: entry.contract_id,
        include_private,
        kind,
    };
    dharma::dharmaq::ensure_contract_table(&ctx.data_dir, &spec)
}

fn resolve_contract_table_for_query(
    ctx: &ReplContext,
    table: &str,
    query: Option<&str>,
) -> Result<Option<String>, DharmaError> {
    if table == "assertions" {
        if let Some(query) = query {
            let lower = query.trim_start().to_ascii_lowercase();
            if lower.starts_with("assertion") || lower.starts_with("assertions") {
                return Ok(None);
            }
        }
        if let Some(subject) = ctx.current_subject {
            if let Some(schema_id) = subject_schema_id(ctx, &subject)? {
                if let Some(entry) = contract_entry_for_schema(ctx, &schema_id)? {
                    let table = ensure_contract_table(
                        ctx,
                        &entry,
                        ctx.current_lens,
                        dharma::dharmaq::ContractTableKind::State,
                    )?;
                    return Ok(Some(table));
                }
            }
        }
        return Ok(None);
    }

    if table_exists(ctx, table) {
        return Ok(Some(table.to_string()));
    }

    let (contract, lens, kind) = parse_contract_table_spec_name(table, ctx.current_lens)?;
    let entry = resolve_contract_by_name(ctx, &contract)?;
    let table = ensure_contract_table(ctx, &entry, lens, kind)?;
    Ok(Some(table))
}

fn table_exists(ctx: &ReplContext, name: &str) -> bool {
    let path = ctx.data_dir.join("dharmaq").join("tables").join(name);
    path.exists()
}

fn select_contract_by_name(entries: Vec<ContractEntry>, name: &str) -> Option<ContractEntry> {
    let mut matches: Vec<ContractEntry> = entries.into_iter().filter(|e| e.name == name).collect();
    if matches.is_empty() {
        return None;
    }
    matches.sort_by(|a, b| compare_version(&a.version, &b.version));
    matches.pop()
}

fn collect_package_contracts(root: &PathBuf) -> Result<Vec<ContractEntry>, DharmaError> {
    let manifests = pkg::list_installed(root)?;
    if manifests.is_empty() {
        return Ok(Vec::new());
    }
    let store = Store::from_root(root);
    let mut out = Vec::new();
    for manifest in manifests {
        let selected = manifest
            .pinned
            .and_then(|v| manifest.versions.get(&v).cloned())
            .or_else(|| pkg::select_best_version(&manifest.versions));
        let Some(version) = selected else {
            continue;
        };
        let schema_bytes = match store.get_object(&version.schema) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let schema = match CqrsSchema::from_cbor(&schema_bytes) {
            Ok(schema) => schema,
            Err(_) => continue,
        };
        let schema_id = SchemaId::from_bytes(*version.schema.as_bytes());
        let contract_id = ContractId::from_bytes(*version.contract.as_bytes());
        let reactor_id = version
            .reactor
            .map(|id| EnvelopeId::from_bytes(*id.as_bytes()));
        out.push(ContractEntry {
            name: schema.namespace.clone(),
            version: schema.version.clone(),
            aggregate: schema.aggregate.clone(),
            schema_id,
            contract_id,
            reactor_id,
            schema,
            source: ContractSource::Package {
                name: manifest.name.clone(),
                ver: version.ver,
            },
        });
    }
    Ok(out)
}

fn collect_local_contracts() -> Result<Vec<ContractEntry>, DharmaError> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let root = find_project_root(&cwd).unwrap_or(cwd);
    let contracts_dir = root.join("contracts");
    let mut out = Vec::new();
    let mut seen: HashSet<(SchemaId, ContractId)> = HashSet::new();

    let project_build = root.join(".dharma").join("contracts");
    if project_build.exists() {
        collect_compiled_contracts(&project_build, false, &mut seen, &mut out)?;
    }

    if contracts_dir.exists() {
        let build_dir = contracts_dir.join("_build");
        if build_dir.exists() {
            collect_compiled_contracts(&build_dir, false, &mut seen, &mut out)?;
        }
        collect_compiled_contracts(&contracts_dir, true, &mut seen, &mut out)?;
    }

    Ok(out)
}

fn find_project_root(start: &Path) -> Option<PathBuf> {
    let mut current = Some(start);
    while let Some(dir) = current {
        if dir.join("dharma.toml").is_file() {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
}

fn collect_compiled_contracts(
    root: &PathBuf,
    skip_build_dirs: bool,
    seen: &mut HashSet<(SchemaId, ContractId)>,
    out: &mut Vec<ContractEntry>,
) -> Result<(), DharmaError> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                if skip_build_dirs && path.file_name().and_then(|s| s.to_str()) == Some("_build") {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|s| s.to_str()) != Some("schema") {
                continue;
            }
            let stem = path.with_extension("");
            let contract_path = stem.with_extension("contract");
            if !contract_path.exists() {
                continue;
            }
            let schema_bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let schema = match CqrsSchema::from_cbor(&schema_bytes) {
                Ok(schema) => schema,
                Err(_) => continue,
            };
            let contract_bytes = match fs::read(&contract_path) {
                Ok(bytes) => bytes,
                Err(_) => continue,
            };
            let schema_id = SchemaId::from_bytes(crypto::sha256(&schema_bytes));
            let contract_id = ContractId::from_bytes(crypto::sha256(&contract_bytes));
            if !seen.insert((schema_id, contract_id)) {
                continue;
            }
            let reactor_path = stem.with_extension("reactor");
            let reactor_id = if reactor_path.exists() {
                match fs::read(&reactor_path) {
                    Ok(bytes) => Some(EnvelopeId::from_bytes(crypto::sha256(&bytes))),
                    Err(_) => None,
                }
            } else {
                None
            };
            out.push(ContractEntry {
                name: schema.namespace.clone(),
                version: schema.version.clone(),
                aggregate: schema.aggregate.clone(),
                schema_id,
                contract_id,
                reactor_id,
                schema,
                source: ContractSource::Local,
            });
        }
    }
    Ok(())
}

fn print_contracts_table(contracts: &[ContractEntry]) {
    let mut rows = Vec::new();
    for contract in contracts {
        rows.push(ContractRow {
            name: contract.name.clone(),
            version: contract.version.clone(),
            schema: contract.schema_id.to_hex(),
            contract: contract.contract_id.to_hex(),
            source: contract.source.label(),
        });
    }
    println!("{}", Table::new(rows).with(Style::rounded()).to_string());
}

fn print_contract_schema(entry: &ContractEntry) {
    println!("Contract: {}", entry.name);
    println!("Version: {}", entry.version);
    println!("Aggregate: {}", entry.aggregate);
    println!("Schema ID: {}", entry.schema_id.to_hex());
    println!("Contract ID: {}", entry.contract_id.to_hex());
    if let Some(reactor) = entry.reactor_id {
        println!("Reactor ID: {}", reactor.to_hex());
    }
    println!("Concurrency: {}", entry.schema.concurrency.as_str());
    if entry.schema.fields.is_empty() {
        println!("No fields.");
        return;
    }
    let mut rows = Vec::new();
    for (name, field) in &entry.schema.fields {
        let default = field
            .default
            .as_ref()
            .map(value_to_json)
            .unwrap_or_else(|| "".to_string());
        rows.push(FieldRow {
            name: name.clone(),
            visibility: field.visibility.as_str().to_string(),
            typ: format_type(&field.typ),
            default,
        });
    }
    println!("{}", Table::new(rows).with(Style::rounded()).to_string());
}

fn print_schema_actions(schema: &CqrsSchema) {
    if schema.actions.is_empty() {
        println!("No actions.");
        return;
    }
    for (name, action) in &schema.actions {
        println!("{name}");
        if action.args.is_empty() {
            println!("  (no args)");
            continue;
        }
        let mut rows = Vec::new();
        for (arg, typ) in &action.args {
            let vis = action
                .arg_vis
                .get(arg)
                .copied()
                .unwrap_or(Visibility::Public);
            rows.push(ActionArgRow {
                arg: arg.clone(),
                typ: format_type(typ),
                visibility: vis.as_str().to_string(),
            });
        }
        println!("{}", Table::new(rows).with(Style::rounded()).to_string());
    }
}

fn print_contract_actions(entry: &ContractEntry) {
    print_schema_actions(&entry.schema);
}

pub(crate) fn format_type(typ: &TypeSpec) -> String {
    match typ {
        TypeSpec::Int => "Int".to_string(),
        TypeSpec::Decimal(scale) => match scale {
            Some(scale) => format!("Decimal(scale={scale})"),
            None => "Decimal".to_string(),
        },
        TypeSpec::Ratio => "Ratio".to_string(),
        TypeSpec::Duration => "Duration".to_string(),
        TypeSpec::Timestamp => "Timestamp".to_string(),
        TypeSpec::Currency => "Currency".to_string(),
        TypeSpec::Text(Some(len)) => format!("Text(len={len})"),
        TypeSpec::Text(None) => "Text".to_string(),
        TypeSpec::Bool => "Bool".to_string(),
        TypeSpec::Enum(values) => format!("Enum({})", values.join(", ")),
        TypeSpec::Identity => "Identity".to_string(),
        TypeSpec::Ref(name) => format!("Ref<{name}>"),
        TypeSpec::SubjectRef(Some(name)) => format!("SubjectRef<{name}>"),
        TypeSpec::SubjectRef(None) => "SubjectRef".to_string(),
        TypeSpec::Struct(name) => format!("Struct<{name}>"),
        TypeSpec::GeoPoint => "GeoPoint".to_string(),
        TypeSpec::List(inner) => format!("List<{}>", format_type(inner)),
        TypeSpec::Map(key, value) => format!("Map<{}, {}>", format_type(key), format_type(value)),
        TypeSpec::Optional(inner) => format!("Optional<{}>", format_type(inner)),
    }
}

fn compare_version(left: &str, right: &str) -> std::cmp::Ordering {
    match (parse_version_triplet(left), parse_version_triplet(right)) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => left.cmp(right),
    }
}

fn parse_version_triplet(raw: &str) -> Option<(u64, u64, u64)> {
    let mut parts = raw.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

#[derive(Tabled)]
struct SubjectRow {
    #[tabled(rename = "Alias")]
    alias: String,
    #[tabled(rename = "Subject")]
    subject: String,
}

#[derive(Tabled)]
struct FindRow {
    #[tabled(rename = "#")]
    idx: usize,
    #[tabled(rename = "Subject")]
    subject: String,
    #[tabled(rename = "Seq")]
    seq: u64,
    #[tabled(rename = "Type")]
    typ: String,
    #[tabled(rename = "Score")]
    score: u32,
    #[tabled(rename = "Assertion")]
    object: String,
    #[tabled(rename = "Snippet")]
    snippet: String,
}

#[derive(Tabled)]
struct QueryRowView {
    #[tabled(rename = "#")]
    idx: usize,
    #[tabled(rename = "Subject")]
    subject: String,
    #[tabled(rename = "Seq")]
    seq: u64,
    #[tabled(rename = "Type")]
    typ: String,
    #[tabled(rename = "Score")]
    score: u32,
    #[tabled(rename = "Assertion")]
    object: String,
}

fn print_subjects_table(ctx: &ReplContext, subjects: &[SubjectId]) {
    let mut rows = Vec::new();
    for subject in subjects {
        let alias = alias_for_subject(&ctx.aliases, subject).unwrap_or_else(|| "-".to_string());
        rows.push(SubjectRow {
            alias,
            subject: short_hex(&subject.to_hex(), 12),
        });
    }
    let table = Table::new(rows).with(Style::rounded()).to_string();
    println!("{table}");
}

fn print_find_table(ctx: &ReplContext, results: &[crate::dharmaq_core::QueryRow]) {
    let rows = results
        .iter()
        .enumerate()
        .map(|(idx, row)| FindRow {
            idx: idx + 1,
            subject: subject_label(ctx, &row.subject),
            seq: row.seq,
            typ: row.typ.clone(),
            score: row.score,
            object: short_hex(&row.assertion_id.to_hex(), 12),
            snippet: sanitize_snippet(row.snippet.as_deref().unwrap_or("")),
        })
        .collect::<Vec<_>>();
    let table = Table::new(rows).with(Style::rounded()).to_string();
    println!("{table}");
}

fn print_query_table(ctx: &ReplContext, results: &[crate::dharmaq_core::QueryRow]) {
    let rows = results
        .iter()
        .enumerate()
        .map(|(idx, row)| QueryRowView {
            idx: idx + 1,
            subject: subject_label(ctx, &row.subject),
            seq: row.seq,
            typ: row.typ.clone(),
            score: row.score,
            object: short_hex(&row.assertion_id.to_hex(), 12),
        })
        .collect::<Vec<_>>();
    let table = Table::new(rows).with(Style::rounded()).to_string();
    println!("{table}");
}

fn subject_label(ctx: &ReplContext, subject: &SubjectId) -> String {
    alias_for_subject(&ctx.aliases, subject).unwrap_or_else(|| short_hex(&subject.to_hex(), 12))
}

fn sanitize_snippet(snippet: &str) -> String {
    let cleaned = snippet.replace('\n', " ").replace('\r', " ");
    if cleaned.len() > 60 {
        format!("{}...", &cleaned[..60])
    } else {
        cleaned
    }
}

fn short_hex(hex: &str, take: usize) -> String {
    if hex.len() <= take {
        hex.to_string()
    } else {
        format!("{}...", &hex[..take])
    }
}

fn prompt_text(label: &str) -> Result<String, DharmaError> {
    if io::stdin().is_terminal() {
        return Text::new(label)
            .prompt()
            .map_err(|err| DharmaError::Validation(err.to_string()));
    }
    let mut input = String::new();
    print!("{label}");
    io::stdout().flush()?;
    io::stdin().read_line(&mut input)?;
    Ok(input.trim_end().to_string())
}

fn prompt_password(label: &str) -> Result<String, DharmaError> {
    if io::stdin().is_terminal() {
        return Password::new(label)
            .without_confirmation()
            .prompt()
            .map_err(|err| DharmaError::Validation(err.to_string()));
    }
    prompt_text(label)
}

fn confirm(label: &str) -> Result<bool, DharmaError> {
    if io::stdin().is_terminal() {
        return Confirm::new(label)
            .with_default(false)
            .prompt()
            .map_err(|err| DharmaError::Validation(err.to_string()));
    }
    let response = prompt_text(label)?;
    Ok(matches!(response.as_str(), "yes" | "y" | "Y"))
}

fn select_subject_interactive(ctx: &ReplContext) -> Result<Option<SubjectId>, DharmaError> {
    let store = Store::from_root(&ctx.data_dir);
    let mut subjects = store.list_subjects()?;
    if let Some(identity) = &ctx.identity {
        if !subjects.contains(&identity.subject_id) {
            subjects.push(identity.subject_id);
        }
    }
    if subjects.is_empty() {
        println!("No subjects.");
        return Ok(None);
    }
    let mut options = Vec::new();
    for subject in subjects {
        let label = subject_label(ctx, &subject);
        options.push(SubjectOption { label, subject });
    }
    let selected = Select::new("Select subject", options)
        .with_page_size(12)
        .prompt()
        .map_err(|err| DharmaError::Validation(err.to_string()))?;
    Ok(Some(selected.subject))
}

#[derive(Clone)]
struct SubjectOption {
    label: String,
    subject: SubjectId,
}

impl std::fmt::Display for SubjectOption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdl::schema::{ConcurrencyMode, CqrsSchema, FieldSchema, TypeSpec, Visibility};
    use tempfile::tempdir;

    #[test]
    fn overlay_disabled_roundtrip() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let empty = load_overlay_disabled(&root).unwrap();
        assert!(empty.is_empty());

        let mut set = BTreeSet::new();
        set.insert("com.test".to_string());
        save_overlay_disabled(&root, &set).unwrap();
        let loaded = load_overlay_disabled(&root).unwrap();
        assert_eq!(loaded, set);

        let cleared = BTreeSet::new();
        save_overlay_disabled(&root, &cleared).unwrap();
        let loaded = load_overlay_disabled(&root).unwrap();
        assert!(loaded.is_empty());
        assert!(!root.join(OVERLAY_DISABLED).exists());
    }

    #[test]
    fn filter_private_fields_hides_private() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "public".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: None,
                visibility: Visibility::Public,
            },
        );
        fields.insert(
            "secret".to_string(),
            FieldSchema {
                typ: TypeSpec::Int,
                default: None,
                visibility: Visibility::Private,
            },
        );
        let schema = CqrsSchema {
            namespace: "com.test".to_string(),
            version: "1.0.0".to_string(),
            aggregate: "Test".to_string(),
            extends: None,
            implements: Vec::new(),
            structs: BTreeMap::new(),
            fields,
            actions: BTreeMap::new(),
            queries: BTreeMap::new(),
            projections: BTreeMap::new(),
            concurrency: ConcurrencyMode::Strict,
        };
        let value = Value::Map(vec![
            (Value::Text("public".to_string()), Value::Integer(1.into())),
            (Value::Text("secret".to_string()), Value::Integer(2.into())),
        ]);
        let filtered = filter_private_fields(&schema, &value).unwrap();
        let map = crate::value::expect_map(&filtered).unwrap();
        assert!(crate::value::map_get(map, "public").is_some());
        assert!(crate::value::map_get(map, "secret").is_none());
    }

    #[test]
    fn peers_roundtrip() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let peer = PeerEntry {
            addr: "127.0.0.1:3000".to_string(),
            subject: Some(SubjectId::from_bytes([1u8; 32])),
            pubkey: Some(crate::types::IdentityKey::from_bytes([2u8; 32])),
            last_seen: Some(123),
        };
        save_peers(&root, &[peer.clone()]).unwrap();
        let loaded = load_peers(&root).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].addr, peer.addr);
        assert_eq!(loaded[0].subject, peer.subject);
        assert_eq!(loaded[0].pubkey, peer.pubkey);
        assert_eq!(loaded[0].last_seen, peer.last_seen);
    }

    #[test]
    fn discovery_state_defaults_and_persists() {
        let dir = tempdir().unwrap();
        let mut ctx = ReplContext::new();
        ctx.data_dir = dir.path().to_path_buf();
        ctx.profile = Profile::HighSec;
        let (enabled, configured) = discovery_state(&ctx);
        assert!(!enabled);
        assert!(!configured);

        set_discovery_state(&ctx.data_dir, true).unwrap();
        let (enabled, configured) = discovery_state(&ctx);
        assert!(enabled);
        assert!(configured);
    }

    #[test]
    fn short_hex_truncates() {
        let hex = "0123456789abcdef";
        assert_eq!(short_hex(hex, 8), "01234567...");
        assert_eq!(short_hex(hex, 32), hex.to_string());
    }

    #[test]
    fn split_command_line_preserves_quoted_args() {
        let line = "commit action Create text=\"This is great\" description='Fresh, Whole Milk'";
        let parts = split_command_line(line);
        let expected = vec![
            "commit",
            "action",
            "Create",
            "text=This is great",
            "description=Fresh, Whole Milk",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        assert_eq!(parts, expected);
    }

    #[test]
    fn sanitize_snippet_truncates_and_flattens() {
        let snippet = "line1\nline2";
        let cleaned = sanitize_snippet(snippet);
        assert!(!cleaned.contains('\n'));
        let long = "a".repeat(100);
        let truncated = sanitize_snippet(&long);
        assert!(truncated.len() <= 63);
    }

    #[test]
    fn index_status_tracks_build_and_drop() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let status = collect_index_status(&root).unwrap();
        assert!(!status.built);

        crate::dharmaq_core::rebuild(&root).unwrap();
        let status = collect_index_status(&root).unwrap();
        assert!(status.built);

        drop_index(&root).unwrap();
        let status = collect_index_status(&root).unwrap();
        assert!(!status.built);
    }
}
