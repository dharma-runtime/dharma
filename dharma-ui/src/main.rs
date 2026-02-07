mod model;

use gpui::prelude::*;
use gpui::{
    div, px, rgb, size, App, Application, Bounds, Context, Window, WindowBounds, WindowOptions,
};
use model::{BufferKind, BufferModel};

fn main() {
    Application::new().run(|cx: &mut App| {
        let bounds = Bounds::centered(None, size(px(720.), px(480.)), cx);
        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                ..Default::default()
            },
            |_, cx| cx.new(|_| WorkspaceView::new()),
        )
        .unwrap();
        cx.activate(true);
    });
}

struct WorkspaceView {
    buffers: BufferModel,
    selected: Option<model::BufferId>,
}

impl WorkspaceView {
    fn new() -> Self {
        let mut buffers = BufferModel::new();
        let welcome = buffers.create("Welcome", BufferKind::Text, "Dharma Workspace");
        buffers.create("Inbox", BufferKind::Data, "");
        buffers.create("Flow", BufferKind::Logic, "");
        Self {
            buffers,
            selected: Some(welcome),
        }
    }
}

impl Render for WorkspaceView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let mut root = div().flex().flex_row().w_full().h_full().bg(rgb(0x0f1115));

        let mut sidebar = div()
            .flex()
            .flex_col()
            .gap_2()
            .p_3()
            .w_64()
            .h_full()
            .bg(rgb(0x12161b))
            .text_color(rgb(0xcbd5f5));

        sidebar = sidebar.child(div().text_xl().text_color(rgb(0xe2e8f0)).child("Buffers"));

        for buffer in self.buffers.list() {
            let id = buffer.id;
            let is_selected = self.selected == Some(id);
            let row_bg = if is_selected {
                rgb(0x1f2937)
            } else {
                rgb(0x14181c)
            };
            let row_text = if is_selected {
                rgb(0xf8fafc)
            } else {
                rgb(0xcbd5f5)
            };
            let label = format!("#{:02} {}", buffer.id.value(), buffer.title);
            let row = div()
                .id(("buffer", id.value()))
                .w_full()
                .p_2()
                .bg(row_bg)
                .text_color(row_text)
                .cursor_pointer()
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.selected = Some(id);
                    cx.notify();
                }))
                .child(label);
            sidebar = sidebar.child(row);
        }

        let mut preview = div()
            .flex()
            .flex_col()
            .gap_3()
            .p_4()
            .flex_1()
            .h_full()
            .bg(rgb(0x0f1115))
            .text_color(rgb(0xf1f5f9));

        if let Some(id) = self.selected {
            if let Some(buffer) = self.buffers.get(id) {
                preview = preview
                    .child(
                        div()
                            .text_xl()
                            .text_color(rgb(0xf8fafc))
                            .child(buffer.title.clone()),
                    )
                    .child(div().text_sm().text_color(rgb(0x94a3b8)).child(format!(
                        "Type: {} · {} chars",
                        buffer.kind,
                        buffer.contents.len()
                    )))
                    .child(div().child(buffer.contents.clone()));
            } else {
                preview = preview.child(div().child("No buffer selected."));
            }
        } else {
            preview = preview.child(div().child("No buffer selected."));
        }

        root = root.child(sidebar).child(preview);
        root
    }
}
