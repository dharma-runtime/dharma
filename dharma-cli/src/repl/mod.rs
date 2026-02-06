pub mod context;
pub mod core;
pub mod aliases;
pub mod subjects;
pub mod render;
pub mod history;
pub mod diff;

use crate::{APP_NAME, APP_VERSION, DharmaError};
use crate::identity_store;
use crate::reactor;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::Editor;
use rustyline::completion::{Completer, Pair};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::highlight::Highlighter;
use rustyline::validate::{Validator, ValidationContext, ValidationResult};
use rustyline::{Context, Helper, Result as RustyResult};
use inquire::Password;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::repl::aliases::AliasMap;
use crate::repl::subjects::recent_subjects;
use crate::repl::core::{format_type, list_contract_names, load_schema_for_lens, split_command_line};
use crate::types::SubjectId;

pub fn run() -> Result<(), DharmaError> {
    let mut rl =
        Editor::<DharmaHelper, DefaultHistory>::new().map_err(|e| DharmaError::Validation(e.to_string()))?;
    
    // Load history if available
    let history_path = get_history_path();
    if let Some(path) = &history_path {
        let _ = rl.load_history(path);
    }

    let mut ctx = context::ReplContext::new();
    let helper_state = ReplState::from_ctx(&ctx);
    let helper = DharmaHelper::new(helper_state);
    rl.set_helper(Some(helper));
    auto_unlock(&mut ctx)?;
    ctx.refresh_identity();
    if let Some(identity) = ctx.identity.clone() {
        reactor::spawn_daemon(ctx.data_dir.clone(), identity);
    }

    let version = APP_VERSION;
    println!("Welcome to {APP_NAME} v{version}.");
    println!("Type 'help' for commands.");

    loop {
        if let Some(helper) = rl.helper_mut() {
            helper.state.lock().ok().map(|mut state| state.update_from_ctx(&ctx));
        }
        ctx.maybe_warn_backup_policy();
        let prompt = ctx.prompt();
        let readline = rl.readline(&prompt);
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);
                match core::handle_command(&mut ctx, line) {
                    Ok(should_exit) => {
                        if should_exit {
                            break;
                        }
                    }
                    Err(e) => eprintln!("Error: {}", e),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }
    
    if let Some(path) = &history_path {
        let _ = rl.save_history(path);
    }
    
    Ok(())
}

fn get_history_path() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".dharma_history"))
}

#[derive(Clone)]
struct ReplState {
    data_dir: PathBuf,
    current_subject: Option<SubjectId>,
    current_lens: u64,
    aliases: AliasMap,
}

impl ReplState {
    fn from_ctx(ctx: &context::ReplContext) -> Self {
        Self {
            data_dir: ctx.data_dir.clone(),
            current_subject: ctx.current_subject,
            current_lens: ctx.current_lens,
            aliases: ctx.aliases.clone(),
        }
    }

    fn update_from_ctx(&mut self, ctx: &context::ReplContext) {
        self.data_dir = ctx.data_dir.clone();
        self.current_subject = ctx.current_subject;
        self.current_lens = ctx.current_lens;
        self.aliases = ctx.aliases.clone();
    }
}

struct DharmaHelper {
    state: Arc<Mutex<ReplState>>,
    hinter: HistoryHinter,
}

impl DharmaHelper {
    fn new(state: ReplState) -> Self {
        Self {
            state: Arc::new(Mutex::new(state)),
            hinter: HistoryHinter::new(),
        }
    }

    fn contract_names(&self, state: &ReplState) -> Vec<String> {
        list_contract_names(&state.data_dir)
    }

    fn action_names(&self, state: &ReplState) -> Vec<String> {
        let Ok(schema) = load_schema_for_lens(&state.data_dir, state.current_lens) else {
            return Vec::new();
        };
        let mut names = schema.actions.keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    fn action_args(&self, state: &ReplState, action: &str) -> Vec<(String, String)> {
        let Ok(schema) = load_schema_for_lens(&state.data_dir, state.current_lens) else {
            return Vec::new();
        };
        let Some(action_schema) = schema.action(action) else {
            return Vec::new();
        };
        let mut args = action_schema
            .args
            .iter()
            .map(|(name, typ)| (name.clone(), format_type(typ)))
            .collect::<Vec<_>>();
        args.sort_by(|a, b| a.0.cmp(&b.0));
        args
    }

    fn recent_subjects(&self, state: &ReplState) -> Vec<String> {
        recent_subjects(&state.data_dir, 10)
            .unwrap_or_default()
            .into_iter()
            .map(|id| id.to_hex())
            .collect()
    }

    fn alias_names(&self, state: &ReplState) -> Vec<String> {
        let mut out = state.aliases.keys().cloned().collect::<Vec<_>>();
        out.sort();
        out
    }

    fn table_names(&self, state: &ReplState) -> Vec<String> {
        let base = state.data_dir.join("dharmaq").join("tables");
        let mut tables = Vec::new();
        if let Ok(entries) = fs::read_dir(&base) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    if let Some(name) = entry.file_name().to_str() {
                        tables.push(name.to_string());
                    }
                }
            }
        }
        if tables.is_empty() {
            tables.push("assertions".to_string());
        }
        tables.sort();
        tables
    }

    fn completion_pairs(candidates: Vec<String>, prefix: &str) -> Vec<Pair> {
        candidates
            .into_iter()
            .filter(|c| c.starts_with(prefix))
            .map(|c| Pair {
                display: c.clone(),
                replacement: c,
            })
            .collect()
    }
}

impl Helper for DharmaHelper {}

impl Completer for DharmaHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> RustyResult<(usize, Vec<Pair>)> {
        let prefix = &line[..pos];
        let start = prefix
            .rfind(char::is_whitespace)
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let word = &prefix[start..];
        let tokens = split_command_line(prefix);
        let state = self
            .state
            .lock()
            .ok()
            .map(|s| s.clone())
            .unwrap_or_else(|| ReplState {
                data_dir: PathBuf::from("data"),
                current_subject: None,
                current_lens: 1,
                aliases: AliasMap::new(),
            });

        if tokens.is_empty() {
            let cmds = vec![
                "id", "identity", "alias", "conf", "config", "ct", "contracts", "pkg", "ls",
                "subjects", "net", "new", "use", "do", "try", "can", "state", "info", "why",
                "status", "tail", "log", "show", "prove", "diff", "overlay", "pwd", "version",
                "help", "exit", "peers", "connect", "sync", "tables", "table", "q", "find",
                "index", "open",
            ]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
            return Ok((start, DharmaHelper::completion_pairs(cmds, word)));
        }

        let head = tokens[0].as_str();
        let candidates = match head {
            "use" => {
                let mut items = self.alias_names(&state);
                items.extend(self.recent_subjects(&state));
                items
            }
            "new" => self.contract_names(&state),
            "contracts" | "ct" => {
                if tokens.len() == 1 {
                    vec!["schema", "actions", "info", "reactors", "ls"]
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect()
                } else if tokens.len() >= 2
                    && matches!(tokens[1].as_str(), "schema" | "actions" | "info" | "reactors")
                {
                    self.contract_names(&state)
                } else {
                    Vec::new()
                }
            }
            "ls" | "subjects" => {
                if tokens.len() >= 2 && tokens[1].as_str() == "c" {
                    self.contract_names(&state)
                } else {
                    Vec::new()
                }
            }
            "do" | "try" | "can" => {
                if tokens.len() == 1 {
                    self.action_names(&state)
                } else {
                    let action = tokens[1].as_str();
                    let mut args = self
                        .action_args(&state, action)
                        .into_iter()
                        .map(|(name, _)| format!("{name}="))
                        .collect::<Vec<_>>();
                    let supplied = tokens
                        .iter()
                        .skip(2)
                        .filter_map(|item| item.split_once('=').map(|(k, _)| k.to_string()))
                        .collect::<Vec<_>>();
                    args.retain(|arg| {
                        let key = arg.trim_end_matches('=');
                        !supplied.iter().any(|s| s == key)
                    });
                    args
                }
            }
            "net" => {
                vec!["peers", "connect", "sync"]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect()
            }
            "table" => {
                if tokens.len() == 1 {
                    self.table_names(&state)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };

        Ok((start, DharmaHelper::completion_pairs(candidates, word)))
    }
}

impl Hinter for DharmaHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }
        let mut hint = None;
        if let Some(history_hint) = self.hinter.hint(line, pos, ctx) {
            hint = Some(history_hint);
        }
        if hint.is_some() {
            return hint;
        }
        let tokens = split_command_line(line);
        if tokens.len() < 2 {
            return None;
        }
        let head = tokens[0].as_str();
        if !matches!(head, "do" | "try" | "can") {
            return None;
        }
        let action = tokens[1].as_str();
        let state = self.state.lock().ok()?;
        let args = self.action_args(&state, action);
        if args.is_empty() {
            return None;
        }
        let signature = args
            .into_iter()
            .map(|(name, typ)| format!("{name}: {typ}"))
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!(" ({signature})"))
    }
}

impl Validator for DharmaHelper {
    fn validate(&self, _ctx: &mut ValidationContext) -> RustyResult<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Highlighter for DharmaHelper {}

fn auto_unlock(ctx: &mut context::ReplContext) -> Result<(), DharmaError> {
    let env = dharma::env::StdEnv::new(&ctx.data_dir);
    if ctx.identity.is_some() {
        return Ok(());
    }
    if !identity_store::identity_exists(&env) {
        return Ok(());
    }
    if let Ok(subject) = identity_store::read_identity_subject(&env) {
        let alias = ctx
            .aliases
            .iter()
            .find(|(_, value)| *value == &subject)
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| subject.to_hex());
        println!("Identity: {} ({})", alias, subject.to_hex());
    }
    let passphrase = Password::new("Password: ")
        .without_confirmation()
        .prompt()
        .map_err(|err| DharmaError::Validation(err.to_string()))?;
    match identity_store::load_identity(&env, &passphrase) {
        Ok(identity) => {
            ctx.current_subject = Some(identity.subject_id);
            ctx.current_alias = ctx.alias_for_subject(&identity.subject_id);
            ctx.identity = Some(identity);
            println!("Identity unlocked.");
        }
        Err(err) => {
            if let Ok(root) = std::env::current_dir() {
                if let Ok(cfg) = dharma::config::Config::load(&root) {
                    let key_path = cfg.keystore_path_for(&root, &ctx.data_dir);
                    eprintln!("Identity unlock failed: {err} (key: {})", key_path.display());
                    return Ok(());
                }
            }
            eprintln!("Identity unlock failed: {err}");
        }
    }
    Ok(())
}
