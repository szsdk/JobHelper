import sys
from typing import Any, Union

import yaml
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Vertical
from textual.reactive import var
from textual.widgets import (
    Footer,
    Header,
    Label,
    ListItem,
    ListView,
    Tree,
)
from textual.widgets.tree import TreeNode


class JobViewerApp(App):
    """A Textual app to view job details with a Tree for job list and parameters."""

    # Updated CSS for the correct layout
    BINDINGS = [
        Binding(
            "n", "next_job", "Next Job", show=False
        ),  # show=False keeps it out of Footer
        Binding(
            "p", "prev_job", "Previous Job", show=False
        ),  # show=False keeps it out of Footer
        Binding("q", "quit", "Quit", show=True),  # Example: keep 'q' visible in Footer
    ]
    CSS = """
    Screen {
        layout: grid;
        grid-rows: auto 1fr; /* Header takes auto height, main content takes remaining */
    }

    Header {
        background: $surface-darken-2;
        color: $text;
        height: 1; /* Fixed height for the header */
        dock: top; /* Ensure it stays at the very top */
    }

    #main-content-area {
        layout: grid;
        grid-size: 3 1; /* 3 columns */
        grid-columns: 1fr 3fr 2fr; /* Relative widths for the columns */
        grid-rows: 1fr; /* One row filling the remaining height */
        height: 1fr; /* Take remaining height after header */
        width: 100%; /* Ensure it spans full width */
    }

    #job-list-container {
        border: solid $accent; /* Border for the entire column */
        overflow: auto; /* Allow scrolling if many jobs */
        padding: 0 1; /* Add some internal padding */
    }

    #job-parameters-container {
        border: solid $success;
        overflow: auto;
        padding: 0 1;
    }

    #job-preamble-container {
        border: solid $success;
        overflow: auto;
        padding: 0 1;
    }

    /* Style for the Tree widgets themselves */
    Tree {
        background: $panel; /* Match the panel background */
        border: none; /* Remove any default tree border */
        height: 1fr; /* Take up all available vertical space in its container */
    }

    /* Style for the RichLog (preamble) */
    RichLog {
        background: $panel; /* Match the panel background */
        border: none; /* Remove any default RichLog border */
        height: 1fr; /* Take up all available vertical space in its container */
    }

    /* This rule styles the root node label of the Tree.
       The "Jobs" and "Parameters" labels are now part of the Tree widget itself. */
    Tree > .tree--root {
        color: $text;
        background: $surface-darken-1;
        text-align: center;
        border-bottom: heavy $border;
        padding: 0 1;
    }
    """

    def __init__(self, job_data) -> None:
        super().__init__()
        self.JOB_DATA = job_data

    selected_job_name: var[str | None] = var(None)

    def add_node_from_dict(self, node: TreeNode, data: Union[dict, list, Any]) -> None:
        """Recursively adds dictionary/list items to a Tree node."""
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    new_node = node.add(f"[bold green]{key}[/]")
                    self.add_node_from_dict(new_node, value)
                else:
                    node.add(f"[bold green]{key}[/]: {value}")
        elif isinstance(data, list):
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)):
                    new_node = node.add(f"[blue]Item {i}[/]")
                    self.add_node_from_dict(new_node, item)
                else:
                    node.add(f"[blue]{item}[/]")
        else:
            node.add(str(data))

    def watch_selected_job_name(self, new_job_name: str | None) -> None:
        """Called when selected_job_name changes."""
        param_tree = self.query_one("#job-parameters", Tree)
        preamble_tree = self.query_one("#job-preamble", Tree)

        # Clear existing content
        param_tree.clear()  # This clears child nodes, not the root itself
        preamble_tree.clear()

        if new_job_name is None:
            param_tree.root.set_label("No job selected")
            preamble_tree.root.set_label("No job selected")
            self.title = "Job Viewer App"
            return

        job_data = self.JOB_DATA.get(new_job_name)
        self.title = f"{new_job_name} ({job_data['command']})"

        if job_data:
            parameters = job_data.get("config", {})

            # Update parameters display in Tree
            param_tree.root.set_label("[b]config[/]")
            if parameters:
                self.add_node_from_dict(param_tree.root, parameters)
            else:
                param_tree.root.add("No parameters defined for this job.")

            # Expand the root node by default
            param_tree.root.expand_all()

            preamble = job_data.get("job_preamble", {})
            preamble_tree.root.set_label("[b]job_preamble[/]")
            if preamble:
                self.add_node_from_dict(preamble_tree.root, preamble)
            else:
                preamble_tree.root.add("No preamble defined for this job.")

            # Expand the root node by default
            preamble_tree.root.expand_all()

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header("Job Viewer App")
        yield Footer()

        # The main content area now uses a grid to hold the three columns
        with Container(id="main-content-area"):
            with Vertical(id="job-list-container"):
                yield Label(
                    "[b]jobs[/]", id="list-header"
                )  # Custom header for ListView
                yield ListView(id="job-list")  # The ListView widget
                # yield Tree("Jobs", id="job-list")
            with Vertical(id="job-parameters-container"):
                yield Tree("Parameters", id="job-parameters")
            with Vertical(id="job-preamble-container"):
                # yield RichLog(id="job-preamble")
                yield Tree("Preamble", id="job-preamble")

    def on_mount(self) -> None:
        """Called when the app is mounted."""
        job_list_view = self.query_one("#job-list", ListView)
        for job_name in self.JOB_DATA.keys():
            # Add each job name as a ListItem containing a Label
            job_list_view.append(ListItem(Label(job_name), id=job_name))
        if self.JOB_DATA:
            first_job_name = next(iter(self.JOB_DATA.keys()))
            # Find the ListItem by its ID and select it
            first_item = self.query_one(f"#{first_job_name}", ListItem)
            job_list_view.index = job_list_view.children.index(first_item)
            self.selected_job_name = first_job_name

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Called when a new item is selected in the ListView."""
        if event.item and event.item.id:
            self.selected_job_name = event.item.id

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Called when a node is selected in the job list Tree."""
        if event.node.tree.id == "job-list" and event.node.data is not None:
            self.selected_job_name = event.node.data

    def action_next_job(self) -> None:
        """Selects the next job in the list."""
        job_list_view = self.query_one("#job-list", ListView)
        if (
            job_list_view.index is not None
            and job_list_view.index < len(job_list_view.children) - 1
        ):
            job_list_view.index += 1
            # Manually trigger the selection change
            self.selected_job_name = job_list_view.children[job_list_view.index].id
        elif job_list_view.index == len(job_list_view.children) - 1:
            # Wrap around to the beginning if at the end
            job_list_view.index = 0
            self.selected_job_name = job_list_view.children[job_list_view.index].id

    def action_prev_job(self) -> None:
        """Selects the previous job in the list."""
        job_list_view = self.query_one("#job-list", ListView)
        if job_list_view.index is not None and job_list_view.index > 0:
            job_list_view.index -= 1
            # Manually trigger the selection change
            self.selected_job_name = job_list_view.children[job_list_view.index].id
        elif job_list_view.index == 0:
            # Wrap around to the end if at the beginning
            job_list_view.index = len(job_list_view.children) - 1
            self.selected_job_name = job_list_view.children[job_list_view.index].id
