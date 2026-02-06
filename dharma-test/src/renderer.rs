use std::collections::VecDeque;
use std::io::{self, Write};

use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::execute;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Style};
use ratatui::text::{Line, Text};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::{backend::CrosstermBackend, Terminal};

#[derive(Clone, Debug, PartialEq)]
pub enum Phase {
    Init,
    Properties,
    Vectors,
    Simulation,
    External,
    Done,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Phase::Init => "init",
            Phase::Properties => "properties",
            Phase::Vectors => "vectors",
            Phase::Simulation => "simulation",
            Phase::External => "external",
            Phase::Done => "done",
        };
        write!(f, "{name}")
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Status {
    pub phase: Phase,
    pub seed: u64,
    pub seed_index: usize,
    pub seed_total: usize,
    pub passed: usize,
    pub failed: usize,
    pub current: Option<String>,
    pub iteration: usize,
    pub iterations: usize,
    pub nodes: usize,
    pub error: Option<String>,
}

impl Status {
    pub fn new(seed: u64, seed_total: usize) -> Self {
        Self {
            phase: Phase::Init,
            seed,
            seed_index: 0,
            seed_total,
            passed: 0,
            failed: 0,
            current: None,
            iteration: 0,
            iterations: 0,
            nodes: 0,
            error: None,
        }
    }

    pub fn summary_lines(&self) -> Vec<String> {
        let current = self.current.as_deref().unwrap_or("-");
        let iter = if self.iterations == 0 {
            "-".to_string()
        } else {
            format!("{}/{}", self.iteration, self.iterations)
        };
        let nodes = if self.nodes == 0 {
            "-".to_string()
        } else {
            self.nodes.to_string()
        };
        vec![
            format!("Phase: {}", self.phase),
            format!("Seed: {} ({}/{})", self.seed, self.seed_index, self.seed_total),
            format!("Passed: {}  Failed: {}", self.passed, self.failed),
            format!("Current: {}", current),
            format!("Iteration: {}", iter),
            format!("Nodes: {}", nodes),
        ]
    }
}

pub trait Renderer {
    fn start(&mut self, status: &Status);
    fn update(&mut self, status: &Status);
    fn log(&mut self, line: &str);
    fn finish(&mut self, status: &Status);
    fn pause(&mut self);
    fn is_interactive(&self) -> bool;
}

pub struct HeadlessRenderer {
    last: Option<Status>,
}

impl HeadlessRenderer {
    pub fn new() -> Self {
        Self { last: None }
    }

    fn print(&mut self, status: &Status) {
        let line = status.summary_lines().join(" | ");
        println!("{line}");
        let _ = io::stdout().flush();
    }

    fn should_print(&self, status: &Status) -> bool {
        match &self.last {
            None => true,
            Some(last) => {
                if last.phase != status.phase || last.current != status.current {
                    return true;
                }
                if status.iterations == 0 {
                    return false;
                }
                status.iteration == status.iterations || status.iteration % 10 == 0
            }
        }
    }
}

impl Renderer for HeadlessRenderer {
    fn start(&mut self, status: &Status) {
        self.print(status);
        self.last = Some(status.clone());
    }

    fn update(&mut self, status: &Status) {
        if self.should_print(status) {
            self.print(status);
            self.last = Some(status.clone());
        }
    }

    fn log(&mut self, line: &str) {
        println!("[log] {line}");
    }

    fn finish(&mut self, status: &Status) {
        self.print(status);
        self.last = Some(status.clone());
    }

    fn pause(&mut self) {}

    fn is_interactive(&self) -> bool {
        false
    }
}

pub struct TuiRenderer {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
    status: Status,
    logs: VecDeque<String>,
}

impl TuiRenderer {
    pub fn new() -> io::Result<Self> {
        let mut stdout = io::stdout();
        enable_raw_mode()?;
        execute!(stdout, EnterAlternateScreen, cursor::Hide)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self {
            terminal,
            status: Status::new(0, 0),
            logs: VecDeque::new(),
        })
    }

    fn draw(&mut self) {
        let status = self.status.clone();
        let logs: Vec<String> = self.logs.iter().cloned().collect();
        let _ = self.terminal.draw(|frame| {
            let size = frame.size();
            let mut constraints = Vec::new();
            let show_error = status.error.is_some();
            if show_error {
                constraints.push(Constraint::Length(3));
            }
            constraints.push(Constraint::Length(8));
            constraints.push(Constraint::Min(5));
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(constraints)
                .split(size);

            let mut idx = 0usize;
            if let Some(message) = status.error.as_ref() {
                let error = Paragraph::new(Text::from(message.as_str()))
                    .block(Block::default().title("Error").borders(Borders::ALL))
                    .wrap(Wrap { trim: false });
                frame.render_widget(error, chunks[idx]);
                idx += 1;
            }
            let stats_text = status
                .summary_lines()
                .into_iter()
                .map(Line::from)
                .collect::<Vec<_>>();
            let stats = Paragraph::new(Text::from(stats_text))
                .block(Block::default().title("Status").borders(Borders::ALL))
                .style(Style::default());
            frame.render_widget(stats, chunks[idx]);
            idx += 1;

            let log_lines = logs.into_iter().map(Line::from).collect::<Vec<_>>();
            let log = Paragraph::new(Text::from(log_lines))
                .block(Block::default().title("Trace").borders(Borders::ALL))
                .wrap(Wrap { trim: false });
            frame.render_widget(log, chunks[idx]);
        });
    }
}

impl Drop for TuiRenderer {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen, cursor::Show);
        let _ = self.terminal.show_cursor();
    }
}

impl Renderer for TuiRenderer {
    fn start(&mut self, status: &Status) {
        self.status = status.clone();
        self.draw();
    }

    fn update(&mut self, status: &Status) {
        self.status = status.clone();
        self.draw();
    }

    fn log(&mut self, line: &str) {
        if self.logs.len() >= 200 {
            self.logs.pop_front();
        }
        self.logs.push_back(line.to_string());
        self.draw();
    }

    fn finish(&mut self, status: &Status) {
        self.status = status.clone();
        self.draw();
    }

    fn pause(&mut self) {
        loop {
            if let Ok(Event::Key(key)) = event::read() {
                if matches!(key.code, KeyCode::Char(' ')) {
                    break;
                }
            }
        }
    }

    fn is_interactive(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_summary_lines_include_phase_and_seed() {
        let mut status = Status::new(42, 3);
        status.phase = Phase::Properties;
        status.seed_index = 1;
        status.seed_total = 3;
        status.current = Some("P-CBOR-001".to_string());
        status.iteration = 2;
        status.iterations = 10;
        status.nodes = 3;
        let lines = status.summary_lines();
        assert!(lines.iter().any(|line| line.contains("Phase: properties")));
        assert!(lines.iter().any(|line| line.contains("Seed: 42 (1/3)")));
        assert!(lines.iter().any(|line| line.contains("Current: P-CBOR-001")));
        assert!(lines.iter().any(|line| line.contains("Iteration: 2/10")));
        assert!(lines.iter().any(|line| line.contains("Nodes: 3")));
    }
}
