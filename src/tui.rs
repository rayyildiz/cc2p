use crate::conversion::{convert_to_parquet_with_columns, infer_schema, remove_deduplicate_columns};
use crate::error::Result;
use crate::utils::find_files;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame, Terminal,
};
use std::io;
use std::path::PathBuf;

struct App {
    files: Vec<PathBuf>,
    file_list_state: ListState,
    columns: Vec<(String, String, bool)>, // (name, type, selected)
    column_list_state: ListState,
    active_panel: ActivePanel,
    delimiter: char,
    has_header: bool,
    sampling_size: u16,
    message: String,
}

#[derive(PartialEq)]
enum ActivePanel {
    FileList,
    ColumnList,
}

impl App {
    fn new(files: Vec<PathBuf>, delimiter: char, has_header: bool, sampling_size: u16) -> App {
        let mut file_list_state = ListState::default();
        if !files.is_empty() {
            file_list_state.select(Some(0));
        }
        App {
            files,
            file_list_state,
            columns: Vec::new(),
            column_list_state: ListState::default(),
            active_panel: ActivePanel::FileList,
            delimiter,
            has_header,
            sampling_size,
            message: String::from("Use Arrow keys to navigate, Space to select/unselect, Enter to export, Tab to switch panels, Q to quit"),
        }
    }

    fn next_file(&mut self) {
        let i = match self.file_list_state.selected() {
            Some(i) => {
                if i >= self.files.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.file_list_state.select(Some(i));
        self.update_columns();
    }

    fn previous_file(&mut self) {
        let i = match self.file_list_state.selected() {
            Some(i) => {
                if i == 0 {
                    self.files.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.file_list_state.select(Some(i));
        self.update_columns();
    }

    fn next_column(&mut self) {
        if self.columns.is_empty() {
            return;
        }
        let i = match self.column_list_state.selected() {
            Some(i) => {
                if i >= self.columns.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.column_list_state.select(Some(i));
    }

    fn previous_column(&mut self) {
        if self.columns.is_empty() {
            return;
        }
        let i = match self.column_list_state.selected() {
            Some(i) => {
                if i == 0 {
                    self.columns.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.column_list_state.select(Some(i));
    }

    fn toggle_column(&mut self) {
        if let Some(i) = self.column_list_state.selected() {
            if i < self.columns.len() {
                self.columns[i].2 = !self.columns[i].2;
            }
        }
    }

    fn update_columns(&mut self) {
        if let Some(i) = self.file_list_state.selected() {
            let file_path = &self.files[i];
            match infer_schema(file_path, self.delimiter, self.has_header, self.sampling_size) {
                Ok(schema) => {
                    let deduplicated_schema = remove_deduplicate_columns(schema);
                    self.columns = deduplicated_schema
                        .fields()
                        .iter()
                        .map(|f| (f.name().clone(), f.data_type().to_string(), true))
                        .collect();
                    if !self.columns.is_empty() {
                        self.column_list_state.select(Some(0));
                    } else {
                        self.column_list_state.select(None);
                    }
                }
                Err(e) => {
                    self.message = format!("Error reading schema: {}", e);
                    self.columns = Vec::new();
                    self.column_list_state.select(None);
                }
            }
        }
    }

    async fn export_selected(&mut self) -> Result<()> {
        if let Some(i) = self.file_list_state.selected() {
            let file_path = &self.files[i];
            let selected_cols: Vec<String> = self
                .columns
                .iter()
                .filter(|(_, _, selected)| *selected)
                .map(|(name, _, _)| name.clone())
                .collect();

            if selected_cols.is_empty() {
                self.message = String::from("No columns selected!");
                return Ok(());
            }

            self.message = format!("Exporting {}...", file_path.display());
            match convert_to_parquet_with_columns(
                file_path,
                self.delimiter,
                self.has_header,
                self.sampling_size,
                selected_cols,
            )
            .await
            {
                Ok(_) => {
                    self.message = format!("Successfully exported to {}", file_path.with_extension("parquet").display());
                }
                Err(e) => {
                    self.message = format!("Export failed: {}", e);
                }
            }
        }
        Ok(())
    }
}

pub async fn run_tui(path: &str, delimiter: char, has_header: bool, sampling_size: u16) -> Result<()> {
    let files = find_files(path).map_err(|e| crate::error::Cc2pError::Other(e.to_string()))?;
    if files.is_empty() {
        return Err(crate::error::Cc2pError::Other(format!("No CSV files found for path: {}", path)));
    }

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(files, delimiter, has_header, sampling_size);
    app.update_columns();

    let res = run_app(&mut terminal, app).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err);
    }

    Ok(())
}

async fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, &mut app)).map_err(|e| crate::error::Cc2pError::Other(e.to_string()))?;

        if event::poll(std::time::Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => return Ok(()),
                        KeyCode::Down => {
                            if app.active_panel == ActivePanel::FileList {
                                app.next_file();
                            } else {
                                app.next_column();
                            }
                        }
                        KeyCode::Up => {
                            if app.active_panel == ActivePanel::FileList {
                                app.previous_file();
                            } else {
                                app.previous_column();
                            }
                        }
                        KeyCode::Tab => {
                            app.active_panel = if app.active_panel == ActivePanel::FileList {
                                ActivePanel::ColumnList
                            } else {
                                ActivePanel::FileList
                            };
                        }
                        KeyCode::Char(' ') => {
                            if app.active_panel == ActivePanel::ColumnList {
                                app.toggle_column();
                            }
                        }
                        KeyCode::Enter => {
                            let _ = app.export_selected().await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}

fn ui(f: &mut Frame, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
        .split(f.area());

    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[0]);

    // File List
    let files: Vec<ListItem> = app
        .files
        .iter()
        .map(|p| {
            let content = p.file_name().unwrap().to_string_lossy();
            ListItem::new(content.to_string())
        })
        .collect();

    let file_list_block = Block::default()
        .borders(Borders::ALL)
        .title("CSV Files")
        .border_style(if app.active_panel == ActivePanel::FileList {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        });
    let file_list = List::new(files)
        .block(file_list_block)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).bg(Color::Blue))
        .highlight_symbol(">> ");
    f.render_stateful_widget(file_list, main_chunks[0], &mut app.file_list_state);

    // Column List
    let columns: Vec<ListItem> = app
        .columns
        .iter()
        .map(|(name, ty, selected)| {
            let status = if *selected { "[x]" } else { "[ ]" };
            ListItem::new(format!("{} {} : {}", status, name, ty))
        })
        .collect();

    let column_list_block = Block::default()
        .borders(Borders::ALL)
        .title("Columns (Name : Type)")
        .border_style(if app.active_panel == ActivePanel::ColumnList {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        });
    let column_list = List::new(columns)
        .block(column_list_block)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).bg(Color::Blue))
        .highlight_symbol(">> ");
    f.render_stateful_widget(column_list, main_chunks[1], &mut app.column_list_state);

    // Status Message
    let status_bar = Paragraph::new(app.message.as_str())
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status_bar, chunks[1]);
}
