# app.py

import dash
from dash import dcc, html, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import pandas as pd
import os
import pandas.api.types as ptypes
import joblib
import base64
from werkzeug.utils import secure_filename
import plotly.express as px
from dash.exceptions import PreventUpdate
import plotly.graph_objs as go
from pathlib import Path
from datetime import datetime
import traceback
import io
import hashlib
import glob
import time # For simulating delays
from flask import request, abort

# --- Configuration ---
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_FOLDER = BASE_DIR / "data"
CACHE_FOLDER = BASE_DIR / "cache"
MODEL_PATH = BASE_DIR / "model" / "failure_model.joblib"

os.makedirs(DATA_FOLDER, exist_ok=True)
os.makedirs(CACHE_FOLDER, exist_ok=True)
os.makedirs(MODEL_PATH.parent, exist_ok=True)

# --- App Initialization ---
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUX], suppress_callback_exceptions=True)
app.title = "Data Platform"

# <-- MODIFICATION 1: Add this line to expose the Flask server for Gunicorn -->
server = app.server

# --- IP-Based Access Restriction Middleware ---
# This is the IP prefix for your "scania" network.
ALLOWED_IP_PREFIX = '138.106.'

# <-- MODIFICATION 2: Updated IP check to work on Render -->
@app.server.before_request
def restrict_access_by_ip():
    # On deployment platforms like Render, the user's IP is in the 'X-Forwarded-For' header.
    # We check that first, and fall back to remote_addr for local development.
    if 'x-forwarded-for' in request.headers:
        # The header can be a comma-separated list of IPs. The first one is the client.
        client_ip = request.headers['x-forwarded-for'].split(',')[0].strip()
    else:
        # This is the fallback for running locally (`python app.py`)
        client_ip = request.remote_addr

    # Allow local development access (your own machine)
    if client_ip == '127.0.0.1':
        return

    # Check if the user's IP address does NOT start with the allowed prefix.
    if not client_ip.startswith(ALLOWED_IP_PREFIX):
        print(f"Blocking request from unauthorized IP: {client_ip}")
        abort(403)  # Return a "403 Forbidden" error page to the user.

# --- Helper Functions (No changes below this line, except the very last line) ---
def list_excel_files():
    if not os.path.exists(DATA_FOLDER):
        return []
    return sorted([f for f in os.listdir(DATA_FOLDER) if f.endswith(('.xlsx', '.xls', '.csv'))])

def read_file(file_source, filename_for_ext_check):
    try:
        if filename_for_ext_check.lower().endswith('.csv'):
            return pd.read_csv(file_source)
        elif filename_for_ext_check.lower().endswith(('.xlsx', '.xls')):
            return pd.read_excel(file_source)
    except Exception as e:
        print(f"Error reading file '{filename_for_ext_check}': {e}")
        return None
    return None

def create_dropdown_options(df, column_name):
    if column_name not in df.columns:
        return []
    raw_unique_vals = df[column_name].dropna().unique()
    options = [{'label': str(val).strip(), 'value': val} for val in raw_unique_vals]
    try:
        options.sort(key=lambda opt: str(opt['label']))
    except TypeError:
        pass
    return options

# --- Application Layout (No Changes) ---
app.layout = html.Div([
    # Stores are non-visual and can be placed anywhere at the top level
    dcc.Store(id='stored-data'),
    dcc.Store(id='stored-filename'),
    dcc.Store(id='graph-grouped-data'),
    dcc.Store(id='temp-uploaded-file-store'),

    # This is the global loading indicator.
    dcc.Loading(
        id="loading-overlay",
        type="circle",
        fullscreen=True,
        children=html.Div(id="loading-dummy-output")
    ),

    # --- Modals ---
    dbc.Modal(
        [
            dbc.ModalHeader(
                dbc.ModalTitle("Application Guide"),
                # Style for the header part
                style={"backgroundColor": "SteelBlue", "color": "Red", "borderTopLeftRadius": "20px", "borderTopRightRadius": "20px"}
            ),
            dbc.ModalBody(
                [
                    html.P("This application analyzes truck data for visualization and failure prediction."),
                    html.Hr(style={'borderColor': 'white'}),

                    html.H5("Step 1: Select a Data Source", style={'color': 'white'}),
                    html.Ul([
                        html.Li([html.B("Upload File:"), " Use this to analyze a new .xlsx or .csv file from your computer."]),
                        html.Li([html.B("Select File:"), " Choose from a list of datasets already saved on the server."]),
                    ]),

                    html.H5("Step 2: Choose an Action", style={'color': 'white'}),
                    html.Ul([
                        html.Li([html.B("Visualization:"), " Explore your data by creating interactive 1D, 2D, or 3D graphs."]),
                        html.Li([html.B("Prediction:"), " Predict the remaining useful life for a specific part instance."]),
                    ]),
                    html.Hr(style={'borderColor': 'white'}),

                    html.H4("Understanding the Options", className="mt-3", style={'color': 'white'}),

                    html.H5("Prediction", style={'color': 'white'}),
                    html.P("This tool predicts when a specific part might fail based on historical data."),
                    html.Ul([
                        html.Li("You must select the columns that identify the Chassis, Part, and Production Date."),
                        html.Li("Then, select the specific values to pinpoint a single part instance."),
                        html.Li("The result shows the part's current age, its average lifespan, and its predicted remaining life.")
                    ]),


                    html.H5("Visualization: 1D Graphs", style={'color': 'white'}),
                    html.P("Use 1D graphs to see the distribution of a single column."),
                    html.Ul([
                        html.Li([html.B("What it does:"), " It counts the occurrences of each unique value in your chosen column."]),
                        html.Li([html.B("Example:"), html.I(" \"Show me a count of all the different part numbers in my dataset.\"")]),
                        html.Li([html.B("Controls:"), " You can sort the data, show only the Top N results, and display as a raw count or percentage."]),
                    ]),

                    html.H5("Visualization: 2D Graphs", style={'color': 'white'}),
                    html.P("Use 2D graphs to compare two different columns. There are two modes:"),
                    html.B("Normal Mode"),
                    html.Ul([
                        html.Li([html.B("What it does:"), " Shows the composition of one category broken down by another. It is ideal for understanding relationships."]),
                        html.Li([html.B("Example:"), html.I(" \"For each Chassis (X-axis), show me a stacked bar of all the Part Numbers (Y-axis) on it.\"")]),
                        html.Li([html.B("Controls:"), " The X-axis can be sorted by the total count of items within it."]),
                    ]),
                    html.B("Comparison Mode"),
                     html.Ul([
                        html.Li([html.B("What it does:"), " A powerful time-series graph to track and compare the frequency of specific items over time."]),
                        html.Li([html.B("Example:"), html.I(" \"Compare the number of 'Part A' vs. 'Part B' repairs reported each month.\"")]),
                        html.Li([html.B("How to use:"), " (1) Select a date column, (2) select the column(s) that contain 'Part A' and 'Part B', and (3) select 'Part A' and 'Part B' from the values list."]),
                    ]),

                    html.H5("Visualization: 3D Graphs", style={'color': 'white'}),
                    html.P("Use 3D graphs to explore the relationship between three different variables at once."),
                     html.Ul([
                        html.Li([html.B("What it does:"), " Plots your data in a 3D space, which can help reveal complex patterns or clusters."]),
                        html.Li([html.B("Example:"), html.I(" \"Plot 'Mileage' (X) vs. 'Engine Temp' (Y) vs. 'Part Age' (Z) to see if failures cluster in a specific zone.\"")]),
                    ]),
                ],
                # Style for the body part
                style={
                    "backgroundColor": "LightBlue",
                    "maxHeight": "500px",
                    "overflowY": "auto",
                    "borderBottomLeftRadius": "20px",
                    "borderBottomRightRadius": "20px"
                }
            )
        ],
        id="info-modal", is_open=False, centered=True, size="lg"
    ),

    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle("Save Uploaded File")),
        dbc.ModalBody([
            html.P("Do you want to save this file to the server for future use?"),
            dbc.RadioItems(
                options=[
                    {"label": "Yes, save it permanently.", "value": "yes"},
                    {"label": "No, use for this session only.", "value": "no"},
                ],
                value="no",
                id="save-file-choice",
            ),
        ]),
        dbc.ModalFooter(
            dbc.Button("Confirm", id="save-file-confirm-btn", color="primary", n_clicks=0)
        ),
    ], id="save-file-modal", is_open=False, centered=True),

    # --- Main Application Content ---
    html.H2("Truck Data Analysis", className="header"),
    dbc.Button("Info", id="info-button", color="info", size="sm", className="ml-2 info-button", style={"display": "inline-block", "verticalAlign": "middle"}),

    html.Div([
        html.Label("Select Source", className="sub-header"),
        dcc.RadioItems(
            id='source-choice',
            options=[{'label': 'Upload Dataset', 'value': 'upload'}, {'label': 'Select Dataset', 'value': 'select'}],
            className="radio-group",
            inline=True
        ),
        html.Div([
            dcc.Upload(
                id='upload-data',
                children=html.Div(['Drag & Drop or ', html.A('Select Files')]),
                style={
                    'width': '100%', 'height': '60px', 'lineHeight': '60px', 'borderWidth': '2px',
                    'borderStyle': 'dotted', 'borderRadius': '5px', 'textAlign': 'center',
                    'marginTop': '10px', 'display': 'none'
                },
                accept='.csv, .xlsx, .xls',
                multiple=False
            ),
            dcc.Dropdown(
                id='select-dataset',
                options=[{'label': f, 'value': f} for f in list_excel_files()],
                placeholder="Choose dataset from folder",
                className="dropdown",
                style={'marginTop': '10px', 'display': 'none'}
            )
        ]),
        html.Div(id='second-step', children=[], style={"marginTop": "20px"}),
        html.Div(id='dynamic-section', children=[], style={"marginTop": "20px"}),
        html.Div([
            html.Div(id='graph-output-container'),
            dcc.Dropdown(id='graph-style-dropdown', style={'display': 'none', 'marginBottom': '20px'})
        ]),
    ], className="main-panel")
])

# --- Callbacks (No Changes) ---
# ... (all your callback functions remain exactly the same) ...
@app.callback(
    Output('info-modal', 'is_open'),
    Input('info-button', 'n_clicks'),
    State('info-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_info_modal(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open

@app.callback(
    [
        Output('upload-data', 'style'),
        Output('select-dataset', 'style'),
        Output('stored-data', 'data', allow_duplicate=True),
        Output('stored-filename', 'data', allow_duplicate=True),
        Output('second-step', 'children', allow_duplicate=True),
        Output('dynamic-section', 'children', allow_duplicate=True),
        Output('graph-output-container', 'children', allow_duplicate=True)
    ],
    Input('source-choice', 'value'),
    prevent_initial_call=True
)
def update_source_visibility_and_reset(source_choice):
    upload_style_hidden = {'display': 'none'}
    select_style_hidden = {'display': 'none'}
    upload_style_visible = {
        'width': '100%', 'height': '60px', 'lineHeight': '60px', 'borderWidth': '2px',
        'borderStyle': 'dotted', 'borderRadius': '5px', 'textAlign': 'center',
        'marginTop': '10px', 'display': 'block'
    }
    select_style_visible = {'marginTop': '10px', 'display': 'block'}

    if source_choice == 'upload':
        return upload_style_visible, select_style_hidden, None, None, [], [], []
    elif source_choice == 'select':
        return upload_style_hidden, select_style_visible, None, None, [], [], []

    return upload_style_hidden, select_style_hidden, None, None, [], [], []

@app.callback(
    [
        Output('stored-data', 'data', allow_duplicate=True),
        Output('stored-filename', 'data', allow_duplicate=True),
        Output('second-step', 'children', allow_duplicate=True),
        Output('save-file-modal', 'is_open', allow_duplicate=True),
        Output('temp-uploaded-file-store', 'data', allow_duplicate=True),
        Output('loading-dummy-output', 'children', allow_duplicate=True)
    ],
    [
        Input('upload-data', 'contents'),
        Input('select-dataset', 'value')
    ],
    State('upload-data', 'filename'),
    prevent_initial_call=True
)
def handle_file_selection_or_upload(uploaded_content, selected_file, uploaded_filename):
    triggered_id = ctx.triggered_id
    if not triggered_id:
        raise PreventUpdate

    if triggered_id == 'upload-data' and uploaded_content and uploaded_filename:
        temp_data = {'contents': uploaded_content, 'filename': uploaded_filename}
        return dash.no_update, dash.no_update, dash.no_update, True, temp_data, dash.no_update

    if triggered_id == 'select-dataset' and selected_file:
        file_label = selected_file
        source_path = DATA_FOLDER / selected_file
        if not source_path.exists(): raise PreventUpdate

        mtime = int(source_path.stat().st_mtime)
        s_filename = secure_filename(selected_file)
        cache_filename = f"select_{s_filename}_{mtime}.parquet"
        cache_filepath = CACHE_FOLDER / cache_filename
        cache_filepath_str = str(cache_filepath)

        if not cache_filepath.exists():
            print(f"CACHE MISS (select): Processing '{selected_file}'")
            print("Simulating a long file processing task...")
            time.sleep(3)
            for old_cache in glob.glob(str(CACHE_FOLDER / f"select_{s_filename}_*.parquet")):
                try: os.remove(old_cache)
                except OSError as e: print(f"Error removing old cache file {old_cache}: {e}")

            df_to_process = read_file(source_path, selected_file)
            if df_to_process is not None:
                for col in df_to_process.columns:
                    if df_to_process[col].dtype == 'object':
                        try:
                            converted_col = pd.to_datetime(df_to_process[col], errors='coerce')
                            if not converted_col.isnull().all() and (converted_col.notna().sum() / len(df_to_process) > 0.5):
                                df_to_process[col] = converted_col
                        except Exception: pass
                        df_to_process[col] = df_to_process[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
                df_to_process.to_parquet(cache_filepath_str)
        else:
            print(f"CACHE HIT (select): Using cached version of '{selected_file}'")

        second_step_layout = html.Div([
            html.H5(f"Loaded file: {file_label}", style={"marginTop": "10px"}),
            html.Label("Choose Action", className="sub-header"),
            dcc.RadioItems(
                id='action-choice',
                options=[{'label': 'Visualization', 'value': 'visualization'}, {'label': 'Prediction', 'value': 'prediction'}],
                className="radio-group", inline=True
            )
        ])
        return cache_filepath_str, file_label, second_step_layout, False, None, None

    raise PreventUpdate


@app.callback(
    [
        Output('stored-data', 'data', allow_duplicate=True),
        Output('stored-filename', 'data', allow_duplicate=True),
        Output('second-step', 'children', allow_duplicate=True),
        Output('save-file-modal', 'is_open', allow_duplicate=True),
        Output('temp-uploaded-file-store', 'data', allow_duplicate=True),
        Output('select-dataset', 'options'),
        Output('loading-dummy-output', 'children', allow_duplicate=True)
    ],
    Input('save-file-confirm-btn', 'n_clicks'),
    [
        State('save-file-choice', 'value'),
        State('temp-uploaded-file-store', 'data')
    ],
    prevent_initial_call=True
)
def handle_save_file_confirmation(n_clicks, save_choice, temp_data):
    if not n_clicks or not temp_data:
        raise PreventUpdate

    print("Simulating a long file upload processing task...")
    time.sleep(3)

    uploaded_content = temp_data['contents']
    uploaded_filename = temp_data['filename']
    file_label = uploaded_filename

    _, content_string = uploaded_content.split(',')
    decoded_bytes = base64.b64decode(content_string)

    if save_choice == 'yes':
        s_filename = secure_filename(uploaded_filename)
        save_path = DATA_FOLDER / s_filename
        with open(save_path, 'wb') as f:
            f.write(decoded_bytes)
        print(f"File '{s_filename}' saved to {DATA_FOLDER}")
        new_options = [{'label': f, 'value': f} for f in list_excel_files()]
    else:
        new_options = dash.no_update

    content_hash = hashlib.sha256(decoded_bytes).hexdigest()
    s_filename_cache = secure_filename(uploaded_filename)
    cache_filename = f"upload_{s_filename_cache}_{content_hash[:16]}.parquet"
    cache_filepath = CACHE_FOLDER / cache_filename
    cache_filepath_str = str(cache_filepath)

    print(f"CACHE (upload): Processing '{uploaded_filename}' for this session.")
    buffer = io.BytesIO(decoded_bytes)
    df_to_process = read_file(buffer, uploaded_filename)
    if df_to_process is not None:
        for col in df_to_process.columns:
            if df_to_process[col].dtype == 'object':
                try:
                    converted_col = pd.to_datetime(df_to_process[col], errors='coerce')
                    if not converted_col.isnull().all() and (converted_col.notna().sum() / len(df_to_process) > 0.5):
                        df_to_process[col] = converted_col
                except Exception: pass
                df_to_process[col] = df_to_process[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df_to_process.to_parquet(cache_filepath_str)

    second_step_layout = html.Div([
        html.H5(f"Loaded file: {file_label}", style={"marginTop": "10px"}),
        html.Label("Choose Action", className="sub-header"),
        dcc.RadioItems(
            id='action-choice',
            options=[{'label': 'Visualization', 'value': 'visualization'}, {'label': 'Prediction', 'value': 'prediction'}],
            className="radio-group", inline=True
        )
    ])
    return cache_filepath_str, file_label, second_step_layout, False, None, new_options, None

@app.callback(
    Output('dynamic-section', 'children'),
    Input('action-choice', 'value'),
    State('stored-data', 'data'),
    prevent_initial_call=True
)
def handle_action_choice(action, cache_filepath):
    if not action or not cache_filepath:
        raise PreventUpdate

    df = pd.read_parquet(cache_filepath)
    all_columns = [{'label': col, 'value': col} for col in df.columns]

    if action == 'prediction':
        return html.Div([
            dbc.Alert("Select a specific part instance by its production date to predict its failure.", color="info"),
            html.Br(),
            dbc.Row([
                dbc.Col(dcc.Dropdown(id='chassis-column-dropdown', options=all_columns, placeholder="1. Select Chassis Column"), width=6),
                dbc.Col(dcc.Dropdown(id='part-column-dropdown', options=all_columns, placeholder="2. Select Part Number Column"), width=6)
            ]),
            html.Br(),
            dbc.Row([
                dbc.Col(dcc.Dropdown(id='chassis-value-dropdown', placeholder="3. Select a Chassis ID", disabled=True), width=6),
                dbc.Col(dcc.Dropdown(id='part-value-dropdown', placeholder="4. Select a Part Number", disabled=True), width=6)
            ]),
            html.Br(),
            dbc.Row([
                dbc.Col(dcc.Dropdown(id='prod-date-column-dropdown', options=all_columns, placeholder="5. Select Production Date Column", disabled=True), width=6),
                dbc.Col(dcc.Dropdown(id='prod-date-value-dropdown', placeholder="6. Select a Production Date", disabled=True), width=6)
            ]),
            html.Br(),
            dbc.Button("Predict Failure", id='predict-button', n_clicks=0, disabled=True, color="primary", className="mt-2"),
            html.Div(id='prediction-result', style={'marginTop': '20px'})
        ])
    elif action == 'visualization':
        return html.Div([
            html.Label("Select Graph Type"),
            dcc.RadioItems(
                id='graph-type',
                options=[{'label': '1D', 'value': '1d'}, {'label': '2D', 'value': '2d'}, {'label': '3D', 'value': '3d'}],
                className="radio-group", inline=True, value=None
            ),
            html.Div(id='graph-options-container')
        ])
    raise PreventUpdate

@app.callback(
    Output('graph-options-container', 'children'),
    Input('graph-type', 'value')
)
def generate_graph_options_ui(graph_type):
    if not graph_type:
        return None
    return html.Div([
        dbc.Card(
            [
                dbc.CardHeader(html.H5("Step 2: Choose 2D Mode")),
                dbc.CardBody(
                    dcc.RadioItems(
                        id='two-d-mode',
                        options=[
                            {'label': 'Normal Graph', 'value': 'normal'},
                            {'label': 'Comparison', 'value': 'prod_repair'}
                        ],
                        className="radio-group", inline=True, value=None,
                    )
                ),
            ],
            id='two-d-mode-container', style={'display': 'none'}
        ),
        dcc.Dropdown(id='x-axis', placeholder='X axis (Required)', style={'display': 'none'}),
        dcc.Dropdown(id='x-axis-filter-values', multi=True, placeholder='(Optional) Filter X-axis values', style={'display': 'none'}),
        dcc.Dropdown(id='y-axis', placeholder='Select Y-axis (Required)', style={'display': 'none'}),
        dcc.Dropdown(id='y-axis-filter-values', multi=True, placeholder='(Optional) Filter Y-axis values', style={'display': 'none'}),
        dcc.Dropdown(id='z-axis', placeholder='Select Z-axis (Required)', style={'display': 'none'}),
        dcc.Dropdown(id='z-axis-filter-values', multi=True, placeholder='(Optional) Filter Z-axis values', style={'display': 'none'}),
        html.Div(id='pr-controls-wrapper', children=[
            dcc.Dropdown(id='pr-date-col', placeholder='1. Select Date Column', style={'marginTop': '10px'}),
            dcc.Dropdown(id='pr-group-col', placeholder='2. Select Column(s) to Search In', style={'marginTop': '10px'}, multi=True),
            dcc.Dropdown(id='pr-group-filter', placeholder='3. Select Values to Compare', multi=True, style={'marginTop': '10px'}),
            dbc.Row([
                dbc.Col(
                    dcc.Dropdown(id='pr-agg-col', placeholder='4. (Optional) Group By Column'),
                    width=7
                ),
                dbc.Col(
                    dcc.Dropdown(id='pr-agg-side-filter', placeholder='Filter to Value', disabled=True, clearable=True),
                    width=5
                )
            ], className="mt-2")
        ], style={'display': 'none'}),
        html.Div(id='graph-controls', children=[
            html.Button("Generate Graph", id='generate-graph-btn', n_clicks=0, className="btn btn-primary"),
            dcc.Dropdown(id='graph-style-select', value='bar', placeholder='Select Graph Style', clearable=False, style={'width': '150px', 'marginLeft': '20px'}),
            dcc.Dropdown(id='count-mode-select', options=[{'label': 'Normal', 'value': 'count'}, {'label': 'Percentage', 'value': 'percentage'}], placeholder="Count Mode", value='count', clearable=False, style={'width': '150px', 'marginLeft': '20px'}),
            dcc.Dropdown(id='sort-mode-select', options=[{'label': 'Ascending', 'value': 'asc'}, {'label': 'Descending', 'value': 'desc'}], placeholder="Sort", value=None, clearable=True, style={'width': '150px', 'marginLeft': '20px'}),
            dcc.Dropdown(id='top-data-select', options=[{'label': 'Top 10', 'value': 10}, {'label': 'Top 20', 'value': 20}, {'label': 'Top 50', 'value': 50}, {'label': 'Top 100', 'value': 100}], placeholder="Select Top N", value=None, clearable=True, style={'width': '150px', 'marginLeft': '20px', 'display': 'none'}),
        ], style={'display': 'flex', 'alignItems': 'center', 'flexWrap': 'wrap', 'marginTop': '10px'})
    ])

@app.callback(
    [
        Output('chassis-value-dropdown', 'options'),
        Output('chassis-value-dropdown', 'disabled')
    ],
    Input('chassis-column-dropdown', 'value'),
    State('stored-data', 'data'),
    prevent_initial_call=True
)
def update_chassis_values(chassis_col, cache_filepath):
    if not chassis_col or not cache_filepath: return [], True
    df = pd.read_parquet(cache_filepath)
    options = create_dropdown_options(df, chassis_col)
    return options, False

@app.callback(
    [
        Output('part-value-dropdown', 'options'),
        Output('part-value-dropdown', 'disabled')
    ],
    Input('chassis-value-dropdown', 'value'),
    [
        State('chassis-column-dropdown', 'value'),
        State('part-column-dropdown', 'value'),
        State('stored-data', 'data'),
    ],
    prevent_initial_call=True
)
def update_part_values(chassis_val, chassis_col, part_col, cache_filepath):
    if not all([chassis_val, chassis_col, part_col, cache_filepath]): return [], True
    df = pd.read_parquet(cache_filepath)
    filtered_df = df[df[chassis_col] == chassis_val]
    options = create_dropdown_options(filtered_df, part_col)
    return options, False

@app.callback(
    Output('prod-date-column-dropdown', 'disabled'),
    Input('part-value-dropdown', 'value'),
    prevent_initial_call=True
)
def enable_prod_date_column_dropdown(part_value):
    return not bool(part_value)

@app.callback(
    [
        Output('prod-date-value-dropdown', 'options'),
        Output('prod-date-value-dropdown', 'disabled')
    ],
    Input('prod-date-column-dropdown', 'value'),
    [
        State('chassis-column-dropdown', 'value'),
        State('chassis-value-dropdown', 'value'),
        State('part-column-dropdown', 'value'),
        State('part-value-dropdown', 'value'),
        State('stored-data', 'data'),
    ],
    prevent_initial_call=True
)
def update_prod_date_values(prod_date_col, chassis_col, chassis_val, part_col, part_val, cache_filepath):
    if not all([prod_date_col, chassis_col, chassis_val, part_col, part_val, cache_filepath]):
        return [], True
    df = pd.read_parquet(cache_filepath)
    filtered_df = df[(df[chassis_col] == chassis_val) & (df[part_col] == part_val)]
    if prod_date_col not in filtered_df.columns:
        return [], True
    dates = pd.to_datetime(filtered_df[prod_date_col], errors='coerce').dropna().unique()
    options = [{'label': str(d.date()), 'value': str(d.date())} for d in sorted(dates)]
    return options, False

@app.callback(
    Output('predict-button', 'disabled'),
    Input('prod-date-value-dropdown', 'value'),
    prevent_initial_call=True
)
def enable_predict_button(selected_prod_date):
    return not bool(selected_prod_date)

@app.callback(
    [
        Output('prediction-result', 'children'),
        Output('loading-dummy-output', 'children', allow_duplicate=True)
    ],
    Input('predict-button', 'n_clicks'),
    [
        State('chassis-column-dropdown', 'value'),
        State('chassis-value-dropdown', 'value'),
        State('part-column-dropdown', 'value'),
        State('part-value-dropdown', 'value'),
        State('prod-date-column-dropdown', 'value'),
        State('prod-date-value-dropdown', 'value'),
        State('stored-data', 'data'),
    ],
    prevent_initial_call=True
)
def run_prediction(n_clicks, chassis_col, chassis_val, part_col, part_val, prod_date_col, prod_date_val, cache_filepath):
    if n_clicks == 0: raise PreventUpdate

    print("Simulating a prediction task...")
    time.sleep(2)

    if not all([chassis_col, chassis_val, part_col, part_val, prod_date_col, prod_date_val, cache_filepath]):
        return dbc.Alert("Missing one or more selections.", color="warning"), None

    try:
        failure_model = joblib.load(MODEL_PATH)
    except Exception as e:
        return dbc.Alert(f"Error loading model: {e}. Ensure 'failure_model.joblib' is in the 'model' folder.", color="danger"), None

    df = pd.read_parquet(cache_filepath)
    selected_production_date = pd.to_datetime(prod_date_val)
    part_instance = df[
        (df[chassis_col] == chassis_val) &
        (df[part_col] == part_val) &
        (pd.to_datetime(df[prod_date_col]).dt.date == selected_production_date.date())
    ]

    if part_instance.empty:
        return dbc.Alert("Could not find the specific part instance.", color="danger"), None

    avg_ttf = failure_model.get(part_val) if isinstance(failure_model, dict) else 365
    
    if avg_ttf is None:
        return dbc.Alert(f"No historical failure data for part '{part_val}'.", color="info"), None

    days_in_service = (datetime.now() - selected_production_date).days
    remaining_life = int(avg_ttf - days_in_service)

    color = "success"
    heading = "Prediction: Low Risk"
    if remaining_life < 0:
        color = "danger"
        heading = "Prediction: High Risk - Past Due!"
    elif remaining_life <= 30:
        color = "warning"
        heading = "Prediction: Failure Expected Soon"

    alert = dbc.Alert(
        [
            html.H4(heading, className="alert-heading"),
            html.P(f"Part Number: {part_val} on Chassis: {chassis_val}"),
            html.P(f"Production Date: {selected_production_date.strftime('%Y-%m-%d')}"),
            html.Hr(),
            html.P(f"This part's average life is ~{int(avg_ttf)} days."),
            html.P(f"It has been in service for {days_in_service} days."),
            html.P(f"Predicted Remaining Useful Life: {remaining_life} days.", className="mb-0", style={'fontWeight': 'bold'}),
        ], color=color
    )
    return alert, None

@app.callback(
    [
        Output('x-axis', 'options'), Output('y-axis', 'options'), Output('z-axis', 'options'),
        Output('pr-date-col', 'options'), Output('pr-group-col', 'options'), Output('pr-agg-col', 'options'),
        Output('two-d-mode-container', 'style'), Output('x-axis', 'style'), Output('y-axis', 'style'),
        Output('z-axis', 'style'), Output('x-axis-filter-values', 'style'), Output('y-axis-filter-values', 'style'),
        Output('z-axis-filter-values', 'style'), Output('pr-controls-wrapper', 'style'),
    ],
    [Input('graph-type', 'value'), Input('two-d-mode', 'value')],
    State('stored-data', 'data')
)
def update_graph_axis_inputs(graph_type, two_d_mode, cache_filepath):
    if not graph_type or not cache_filepath:
        raise PreventUpdate

    df = pd.read_parquet(cache_filepath)
    options = [{'label': col, 'value': col} for col in df.columns]
    date_cols = [{'label': col, 'value': col} for col in df.select_dtypes(include=['datetime64[ns]']).columns]
    hidden, default_style = {'display': 'none'}, {'width': '100%', 'marginTop': '10px'}

    styles = [hidden] * 8

    if graph_type == '1d':
        styles[1], styles[4] = default_style, default_style
    elif graph_type == '2d':
        styles[0] = {'marginTop': '20px'}
        if two_d_mode == 'normal':
            styles[1], styles[2], styles[4], styles[5] = [default_style] * 4
        elif two_d_mode == 'prod_repair':
            styles[7] = {'display': 'block'}
    elif graph_type == '3d':
        styles[1], styles[2], styles[3], styles[4], styles[5], styles[6] = [default_style] * 6

    return (
        options, options, options, # Generic
        date_cols, options, options, # Comparison
        *styles
    )

@app.callback(
    [
        Output('graph-style-select', 'options'), Output('graph-style-select', 'value'),
        Output('count-mode-select', 'style'), Output('sort-mode-select', 'style'),
        Output('top-data-select', 'style'),
    ],
    [
        Input('graph-type', 'value'), Input('two-d-mode', 'value'), Input('sort-mode-select', 'value'),
    ]
)
def update_graph_style_controls(graph_type, two_d_mode, sort_mode):
    if not graph_type:
        raise PreventUpdate

    default_options = [{'label': 'Bar', 'value': 'bar'}, {'label': 'Line', 'value': 'line'}, {'label': 'Area', 'value': 'area'}]
    three_d_options = [{'label': '3D Scatter', 'value': 'scatter_3d'}, {'label': '3D Line', 'value': 'line_3d'}, {'label': '3D Bubble', 'value': 'bubble_3d'}]
    hidden, visible_style = {'display': 'none'}, {'width': '150px', 'marginLeft': '20px'}
    options, value = default_options, 'bar'
    count_style, sort_style, top_style = hidden, hidden, hidden

    if graph_type == '1d':
        options, value, sort_style, count_style = default_options, 'bar', visible_style, visible_style
        if sort_mode: top_style = visible_style
    elif graph_type == '2d':
        if two_d_mode == 'normal':
            options = [{'label': 'Bar (Stacked)', 'value': 'bar_stacked'}, {'label': 'Line', 'value': 'line'}, {'label': 'Bar', 'value': 'bar'}, {'label': 'Area', 'value': 'area'}]
            value = 'bar_stacked'
            sort_style, count_style = visible_style, hidden
            if sort_mode: top_style = visible_style
        elif two_d_mode == 'prod_repair':
             options, value = [{'label': 'Line', 'value': 'line'}, {'label': 'Bar', 'value': 'bar'}, {'label': 'Area', 'value': 'area'}], 'line'
             sort_style, top_style, count_style = hidden, hidden, hidden
    elif graph_type == '3d':
        options, value = three_d_options, 'scatter_3d'

    return options, value, count_style, sort_style, top_style

for axis in ['x', 'y', 'z']:
    @app.callback(
        Output(f'{axis}-axis-filter-values', 'options'),
        Input(f'{axis}-axis', 'value'),
        State('stored-data', 'data'),
        prevent_initial_call=True
    )
    def update_axis_filter_values(selected_col, cache_filepath, axis_name=axis):
        if not selected_col or not cache_filepath: return []
        df = pd.read_parquet(cache_filepath)
        return create_dropdown_options(df, selected_col)

@app.callback(
    Output('pr-group-filter', 'options'),
    Input('pr-group-col', 'value'),
    State('stored-data', 'data'),
    prevent_initial_call=True
)
def update_pr_main_filter_options(selected_cols, cache_filepath):
    if not selected_cols or not cache_filepath:
        return []
        
    df = pd.read_parquet(cache_filepath)
    all_unique_vals = set()
    for col in selected_cols:
        if col in df.columns:
            all_unique_vals.update(df[col].dropna().unique())
            
    sorted_vals = sorted(list(all_unique_vals), key=str)
    options = [{'label': str(val), 'value': val} for val in sorted_vals]
    
    return options

@app.callback(
    [
        Output('pr-agg-side-filter', 'options'),
        Output('pr-agg-side-filter', 'disabled')
    ],
    [
        Input('pr-agg-col', 'value'),
        Input('pr-group-filter', 'value')
    ],
    [
        State('pr-group-col', 'value'),
        State('stored-data', 'data'),
    ],
    prevent_initial_call=True
)
def update_pr_side_filter_options(agg_col, group_filter_vals, group_cols, cache_filepath):
    if not all([agg_col, group_filter_vals, group_cols, cache_filepath]):
        return [], True

    df = pd.read_parquet(cache_filepath)
    mask = pd.Series([False] * len(df), index=df.index)
    for col in group_cols:
        if col in df.columns:
            mask = mask | df[col].isin(group_filter_vals)
            
    filtered_df = df[mask]
    options = create_dropdown_options(filtered_df, agg_col)
    
    return options, False if options else True

@app.callback(
    [
        Output('graph-output-container', 'children', allow_duplicate=True),
        Output('graph-style-dropdown', 'style'),
        Output('graph-grouped-data', 'data'),
        Output('loading-dummy-output', 'children', allow_duplicate=True)
    ],
    Input('generate-graph-btn', 'n_clicks'),
    [
        State('graph-style-select', 'value'), State('count-mode-select', 'value'), State('sort-mode-select', 'value'),
        State('graph-type', 'value'), State('two-d-mode', 'value'), State('top-data-select', 'value'),
        State('x-axis', 'value'), State('x-axis-filter-values', 'value'), State('y-axis', 'value'),
        State('y-axis-filter-values', 'value'), State('z-axis', 'value'), State('z-axis-filter-values', 'value'),
        State('pr-date-col', 'value'), State('pr-group-col', 'value'), State('pr-group-filter', 'value'),
        State('pr-agg-col', 'value'), State('pr-agg-side-filter', 'value'),
        State('stored-data', 'data'),
    ],
    prevent_initial_call=True
)
def generate_graph(n_clicks, style, count_mode, sort_mode, g_type, td_mode, top_n, x, x_filter, y, y_filter, z, z_filter, pr_date_col, pr_group_cols, pr_group_filter, pr_agg_col, pr_agg_side_val, cache_filepath):
    if n_clicks == 0:
        raise PreventUpdate

    print("Simulating a long graph generation task...")
    time.sleep(2)

    if not cache_filepath:
        raise PreventUpdate

    df = pd.read_parquet(cache_filepath)
    hidden_style = {'display': 'none'}

    try:
        if g_type == '2d' and td_mode == 'prod_repair':
            if not all([pr_date_col, pr_group_cols, pr_group_filter]):
                return dbc.Alert("Please complete steps 1-3: Date, Search Column(s), and Compare Values.", color="warning"), hidden_style, None, None

            id_cols = [pr_date_col]
            if pr_agg_col and pr_agg_col not in id_cols:
                id_cols.append(pr_agg_col)
                
            valid_id_cols = [col for col in id_cols if col in df.columns]
            valid_value_cols = [col for col in pr_group_cols if col in df.columns]
            
            if not valid_value_cols:
                 return dbc.Alert("None of the selected 'Search Columns' exist in the data.", color="danger"), hidden_style, None, None

            df_melted = df.melt(
                id_vars=valid_id_cols,
                value_vars=valid_value_cols,
                var_name='SourceColumn',
                value_name='ComparisonValue'
            )

            df_plot = df_melted[df_melted['ComparisonValue'].isin(pr_group_filter)].copy()

            if df_plot.empty:
                return dbc.Alert("No data found for the selected comparison values.", color="info"), hidden_style, None, None
            
            df_plot['DateGroup'] = pd.to_datetime(df_plot[pr_date_col]).dt.to_period('M').astype(str)

            if not pr_agg_col:
                grouped_df = df_plot.groupby(['DateGroup', 'ComparisonValue']).size().reset_index(name='Count')
                y_axis_col, chart_title, y_axis_title = 'Count', 'Comparison of Occurrences', 'Occurrences'
            else:
                if pr_agg_side_val:
                    df_plot = df_plot[df_plot[pr_agg_col] == pr_agg_side_val]
                    if df_plot.empty:
                         return dbc.Alert(f"No data found for the filter '{pr_agg_col} = {pr_agg_side_val}'.", color="info"), hidden_style, None, None
                    grouped_df = df_plot.groupby(['DateGroup', 'ComparisonValue']).size().reset_index(name='Count')
                    y_axis_col, chart_title, y_axis_title = 'Count', f"Occurrences for '{pr_agg_col}' = '{pr_agg_side_val}'", 'Occurrences'
                else:
                    grouped_df = df_plot.groupby(['DateGroup', 'ComparisonValue']).agg({pr_agg_col: 'nunique'}).reset_index()
                    y_axis_col, chart_title, y_axis_title = pr_agg_col, f"Comparison by Unique Count of '{pr_agg_col}'", f"Unique Count of {pr_agg_col}"
            
            legend_title = "Compared Value"
            min_date, max_date = df[pr_date_col].min(), df[pr_date_col].max()
            full_month_range = pd.date_range(start=min_date, end=max_date, freq='MS').to_period('M').astype(str).tolist()

            grouped_df = grouped_df.sort_values('DateGroup')
            plot_func = {'line': px.line, 'bar': px.bar, 'area': px.area}.get(style, px.line)
            plot_args = {'data_frame': grouped_df, 'x': 'DateGroup', 'y': y_axis_col, 'color': 'ComparisonValue', 'title': chart_title}
            if style == 'line': plot_args['markers'] = True

            fig = plot_func(**plot_args)
            fig.update_layout(height=600, xaxis_tickangle=-45, yaxis_title=y_axis_title, xaxis_title=f"Month ({pr_date_col})", legend_title=legend_title)
            if style == 'bar': fig.update_layout(barmode='group')
            fig.update_xaxes(type='category', categoryorder='array', categoryarray=full_month_range)

            return dcc.Graph(figure=fig, style={'overflowX': 'auto'}), hidden_style, grouped_df.to_json(orient='split'), None

        elif g_type == '2d' and td_mode == 'normal':
            if not x or not y: return dbc.Alert("Please select both X and Y axis columns.", color="warning"), hidden_style, dash.no_update, None
            df_plot = df.dropna(subset=[x, y]).copy()

            is_date_axis = ptypes.is_datetime64_any_dtype(df_plot[x])
            x_axis_col, full_month_range, final_x_order = x, None, None

            if is_date_axis:
                x_axis_col = 'DateGroup'
                df_plot[x_axis_col] = df_plot[x].dt.to_period('M').astype(str)
                if not df_plot.empty:
                    min_date, max_date = df_plot[x].min(), df_plot[x].max()
                    full_month_range = pd.date_range(start=min_date, end=max_date, freq='MS').to_period('M').astype(str).tolist()
                    final_x_order = full_month_range

            df_plot[x_axis_col] = df_plot[x_axis_col].astype(str)
            df_plot[y] = df_plot[y].astype(str)

            if x_filter: df_plot = df_plot[df_plot[x if not is_date_axis else x_axis_col].isin([str(v) for v in x_filter])]
            if y_filter: df_plot = df_plot[df_plot[y].isin([str(v) for v in y_filter])]
            if df_plot.empty: return dbc.Alert("No data to display after applying filters.", color="info"), hidden_style, dash.no_update, None

            if style == 'bar_stacked':
                if not is_date_axis:
                    x_totals = df_plot.groupby(x_axis_col).size().reset_index(name='_total_count')
                    if sort_mode: x_totals = x_totals.sort_values(by='_total_count', ascending=(sort_mode == 'asc'))
                    final_x_order = x_totals[x_axis_col].tolist()
                    if top_n:
                        if not sort_mode: x_totals = x_totals.sort_values(by='_total_count', ascending=False)
                        final_x_order = x_totals.head(top_n)[x_axis_col].tolist()
                
                df_plot = df_plot[df_plot[x_axis_col].isin(final_x_order)]
                if df_plot.empty: return dbc.Alert("No data after secondary filters.", color="info"), hidden_style, dash.no_update, None

                grouped = df_plot.groupby([x_axis_col, y]).size().reset_index(name='Count')
                chart_title = f"Stacked Count of '{y}' per '{x}'"
                if top_n and not is_date_axis: chart_title += f" (Top {top_n} by Total Count)"

                fig = px.bar(grouped, x=x_axis_col, y='Count', color=y, barmode='stack', title=chart_title)
                fig.update_layout(yaxis_title='Count', xaxis_title=x, xaxis_tickangle=-45, height=600, width=max(1000, len(final_x_order) * 40), legend_title=y, legend=dict(x=1.02, y=1, traceorder="normal", orientation="v"), margin=dict(r=250))
                fig.update_xaxes(type='category', categoryorder='array', categoryarray=final_x_order)
                return dcc.Graph(figure=fig, style={'overflowX': 'auto'}), hidden_style, grouped.to_json(date_format='iso', orient='split'), None

            else:
                value_counts = df_plot.groupby(x_axis_col).size().reset_index(name='Count')
                if not is_date_axis:
                    if sort_mode: value_counts = value_counts.sort_values(by='Count', ascending=(sort_mode == 'asc'))
                    final_x_order = value_counts[x_axis_col].tolist()
                    if top_n:
                        if not sort_mode: value_counts = value_counts.sort_values(by='Count', ascending=False)
                        final_x_order = value_counts.head(top_n)[x_axis_col].tolist()
                
                chart_title = f"Total Count for each '{x}'"
                if top_n and not is_date_axis: chart_title += f" (Top {top_n})"
                plot_func = {'line': px.line, 'area': px.area, 'bar': px.bar}.get(style)
                fig = plot_func(value_counts[value_counts[x_axis_col].isin(final_x_order)], x=x_axis_col, y='Count', title=chart_title)
                if style == 'line': fig.update_traces(mode='lines+markers')
                fig.update_layout(xaxis_title=x, xaxis_tickangle=-65, yaxis_title='Total Count', height=600, width=max(1000, len(final_x_order) * 30))
                fig.update_xaxes(type='category', categoryorder='array', categoryarray=final_x_order)
                return dcc.Graph(figure=fig, style={'overflowX': 'auto'}), hidden_style, None, None
        
        elif g_type == '1d':
            if not x: return dbc.Alert("Please select an X-axis column.", color="warning"), hidden_style, dash.no_update, None
            df_plot = df.copy()
            df_plot[x] = df_plot[x].astype(str)
            if x_filter: df_plot = df_plot[df_plot[x].isin([str(val) for val in x_filter])]
            if df_plot.empty: return dbc.Alert("No data after filters.", color="info"), hidden_style, dash.no_update, None
            
            value_counts = df_plot[x].value_counts().reset_index()
            value_counts.columns = [x, 'Count']
            if sort_mode == 'asc': value_counts = value_counts.sort_values(by='Count', ascending=True)
            elif sort_mode == 'desc': value_counts = value_counts.sort_values(by='Count', ascending=False)
            if top_n: value_counts = value_counts.head(top_n)

            y_axis_col, y_label, chart_title = 'Count', 'Count', f"Frequency of '{x}'"
            if count_mode == 'percentage':
                total = value_counts['Count'].sum()
                value_counts['Percentage'] = (value_counts['Count'] / total) * 100 if total > 0 else 0
                y_axis_col, y_label, chart_title = 'Percentage', 'Percentage (%)', f"Percentage Distribution of '{x}'"
            if top_n: chart_title += f" (Top {top_n})"

            plot_func = {'line': px.line, 'area': px.area}.get(style, px.bar)
            fig = plot_func(value_counts, x=x, y=y_axis_col, title=chart_title)
            if style == 'bar': fig.update_traces(marker_color='#636EFA')
            fig.update_layout(xaxis_tickangle=-65, yaxis_title=y_label, height=600, width=max(1000, len(value_counts) * 30))
            return dcc.Graph(figure=fig, style={'overflowX': 'auto'}), hidden_style, None, None

        elif g_type == '3d':
            if not all([x, y, z]): return dbc.Alert("Please select X, Y, and Z axes.", color="warning"), hidden_style, None, None
            df_plot = df.dropna(subset=[x, y, z]).copy()
            if x_filter: df_plot = df_plot[df_plot[x].isin(x_filter)]
            if y_filter: df_plot = df_plot[df_plot[y].isin(y_filter)]
            if z_filter: df_plot = df_plot[df_plot[z].isin(z_filter)]
            if df_plot.empty: return dbc.Alert("No data after filters.", color="info"), hidden_style, None, None

            chart_title = f"3D Plot: {x}, {y}, {z}"
            if style == 'line_3d': fig = px.line_3d(df_plot.sort_values(by=x), x=x, y=y, z=z, color=z, title=chart_title)
            elif style == 'bubble_3d':
                grouped_3d = df_plot.groupby([x, y, z]).size().reset_index(name='Count')
                fig = px.scatter_3d(grouped_3d, x=x, y=y, z=z, color=z, size='Count', title=f"{chart_title} (by Count)")
            else: fig = px.scatter_3d(df_plot, x=x, y=y, z=z, color=z, title=chart_title)
            fig.update_layout(height=700)
            return dcc.Graph(figure=fig, style={'overflowX': 'auto'}), hidden_style, None, None

        else:
            return dbc.Alert("Invalid graph configuration.", color="warning"), hidden_style, None, None

    except Exception as e:
        print(traceback.format_exc())
        return dbc.Alert(f"An error occurred while generating the graph: {e}", color="danger"), hidden_style, None, None

# This block is only for running the app on your local machine
if __name__ == '__main__':
    app.run(debug=True)