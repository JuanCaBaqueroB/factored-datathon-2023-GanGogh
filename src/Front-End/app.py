#Dash Libraries
import base64

import dash
import dash_bootstrap_components as dbc
from dash import dcc
from dash import html
from dash.dependencies import Input, Output, State

#Python Libraries
import pandas as pd
import plotly.express as px
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Azure Blob Storage credentials
storage_connection_string = "DefaultEndpointsProtocol=https;AccountName=test1fast;AccountKey=QnSkjChqVUQWCLs9t+yDSK4w02oQVBjWtP9dOOBhpw1O002GrWnk8LHfsU8Ys16QjNKmjnDw2RbM+AStEQNjww==;EndpointSuffix=core.windows.net"
container_name = "visualization-tables"
csv_filename = "summary_score_reviews.csv"

# Connect to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(csv_filename)

# Download the CSV file and read it into a DataFrame
with open(csv_filename, "wb") as f:
    f.write(blob_client.download_blob().readall())

df = pd.read_csv(csv_filename)

#Image Sources
image_sidebar= 'Images/side_bar_logo3.png'
image_sidebar_2= 'Images/side_bar_logo4.png'
encoded_image = base64.b64encode(open(image_sidebar, 'rb').read())
encoded_image_2 = base64.b64encode(open(image_sidebar_2, 'rb').read())

#Button definition
button_groups = html.Div(
    [
        dbc.ButtonGroup(
            [dbc.Button("Upload"), dbc.Button("Process"), dbc.Button("Cancel")],
            size="lg",
            className="mr-1")
    ]
)

button = dbc.Button("Upload", color="primary")

#Building Blocks
#------------------------------------------
# Meta Tags
#------------------------------------------
app = dash.Dash(
    external_stylesheets=[dbc.themes.CYBORG],
    # These meta_tags ensure content is scaled correctly on different devices. Don't Delete!!
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ],
)

app.title = 'GanGogh Datathon'
app.config.suppress_callback_exceptions = True

#------------------------------------------
# Sidebar Component
#------------------------------------------
# we use the Row and Col components to construct the sidebar header
# it consists of a title, and a toggle, the latter is hidden on large screens
sidebar_header = dbc.Row(
    [
        dbc.Col(dbc.CardImg(src='data:image/png;base64,{}'.format(encoded_image.decode()), className="display-4")),
        dbc.Col(
            [
                html.Button(
                    # use the Bootstrap navbar-toggler classes to style
                    html.Span(className="navbar-toggler-icon"),
                    className="navbar-toggler",
                    # the navbar-toggler classes don't set color
                    style={
                        "color": "rgba(0,0,0,.5)",
                        "border-color": "rgba(0,0,0,.1)",
                    },
                    id="navbar-toggle",
                ),
                html.Button(
                    # use the Bootstrap navbar-toggler classes to style
                    html.Span(className="navbar-toggler-icon"),
                    className="navbar-toggler",
                    # the navbar-toggler classes don't set color
                    style={
                        "color": "rgba(0,0,0,.5)",
                        "border-color": "rgba(0,0,0,.1)",
                    },
                    id="sidebar-toggle",
                ),
            ],
            # the column containing the toggle will be only as wide as the
            # toggle, resulting in the toggle being right aligned
            width="auto",
            # vertically align the toggle in the center
            align="center",
        ),

    ]
)

sidebar = html.Div(
    [
        sidebar_header,
        # we wrap the horizontal rule and short blurb in a div that can be
        # hidden on a small screen
        html.Div(
            [
                html.Hr(),
                html.P(
                    "Welcome to the Datathon dashboard!",
                    className="lead",
                      ),
            ],
            id="blurb",
        ),
        # use the Collapse component to animate hiding / revealing links
        dbc.Collapse(
            dbc.Nav(
                [
                    dbc.NavLink("Home", href="/page-1", id="page-1-link", className="ico_home", ),
                    dbc.NavLink("General overview", href="/page-2", id="page-2-link", className="ico_upload"),
                    dbc.NavLink("Categories dashboard", href="/page-3", id="page-3-link", className="ico_store"),
                    dbc.NavLink("About Us", href="/page-4", id="page-4-link", className="ico_about_"),
                    dbc.Col(dbc.CardImg(src='data:image/png;base64,{}'.format(encoded_image_2.decode()), className="display-4")),
                ],
                vertical=True,
                pills=True,
            ),
            id="collapse",
        ),
    ],
    id="sidebar",
)

content = html.Div(id="page-content")
app.layout = html.Div([dcc.Location(id="url"), sidebar, content])

# this callback uses the current pathname to set the active state of the
# corresponding nav link to true, allowing users to tell see page they are on
@app.callback(
    [Output(f"page-{i}-link", "active") for i in range(1, 5)],
    [Input("url", "pathname")],
)
def toggle_active_links(pathname):
    if pathname == "/":
        # Treat page 1 as the homepage / index
        return True, False, False, False
    return [pathname == f"/page-{i}" for i in range(1, 5)]


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname in ["/", "/page-1"]:
        return html.Div([
                         html.P("Welcome Page"),
                         html.H4('Simple stock plot with adjustable axis'),
                         dcc.Dropdown(
                            id='dropdown-category-verified',
                            options=[
                                {'label': 'True', 'value': 1},
                                {'label': 'False', 'value': 0}
                            ],
                            value = 1),
                         dcc.Dropdown(
                            id='dropdown-category',
                            options=[
                                {'label': 'Software', 'value': 'Software'},
                                {'label': 'Books', 'value': 'Books'},
                                {'label': 'Movies & TV', 'value': 'Movies & TV'},
                                {'label': 'Audible Audiobooks', 'value': 'Audible Audiobooks'},
                                {'label': 'Buy a Kindle', 'value': 'Buy a Kindle'},
                                {'label': 'Health & Personal Care', 'value': 'Health & Personal Care'},
                                {'label': 'Pet Supplies', 'value': 'Pet Supplies'},
                                {'label': 'Toys & Games', 'value': 'Toys & Games'},
                                {'label': 'Video Games', 'value': 'Video Games'},
                                {'label': 'Sports Collectibles', 'value': 'Sports Collectibles'},
                                {'label': 'Luxury Beauty', 'value': 'Luxury Beauty'}
                            ],
                            value='Software'),
                         dcc.Graph(id='graph-with-dropdown',
                                   config={"displayModeBar": False},
                                   animate=True,)
                         ])
    elif pathname == "/page-2":
        return html.Div([
                         html.P("Categories dashboard"),
                         dcc.Dropdown(
                            id='dropdown-category-qty',
                            options=[
                                {'label': 'Software', 'value': 'Software'},
                                {'label': 'Books', 'value': 'Books'},
                                {'label': 'Movies & TV', 'value': 'Movies & TV'},
                                {'label': 'Audible Audiobooks', 'value': 'Audible Audiobooks'},
                                {'label': 'Buy a Kindle', 'value': 'Buy a Kindle'},
                                {'label': 'Health & Personal Care', 'value': 'Health & Personal Care'},
                                {'label': 'Pet Supplies', 'value': 'Pet Supplies'},
                                {'label': 'Toys & Games', 'value': 'Toys & Games'},
                                {'label': 'Video Games', 'value': 'Video Games'},
                                {'label': 'Sports Collectibles', 'value': 'Sports Collectibles'},
                                {'label': 'Luxury Beauty', 'value': 'Luxury Beauty'}
                            ],
                            value='Software'),
                            dcc.Graph(id='graph-with-dropdown-qty',
                                      config={"displayModeBar": False},
                                      animate=True, )
                         ])
    elif pathname == "/page-3":
        return html.P("Oh cool, this is page 3!")
    elif pathname == "/page-4":
        return html.P("Oh cool, this is page 4!")
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )


@app.callback(
    Output("sidebar", "className"),
    [Input("sidebar-toggle", "n_clicks")],
    [State("sidebar", "className")],
)
def toggle_classname(n, classname):
    if n and classname == "":
        return "collapsed"
    return ""


@app.callback(
    Output("graph-with-dropdown-qty", "figure"),
    Input('dropdown-category-qty', 'value'))
def display_graph(selected_category):
    filtered_df = df[df['maincat_10'] == selected_category]
    fig = px.line(filtered_df, x='date', y='Qty', title=f"Data for Category {selected_category}").update_layout(
    {"plot_bgcolor": "rgba(1, 6, 4, 5)", "paper_bgcolor": "rgba(1, 2, 3, 3)"})
    return fig


@app.callback(
    Output("graph-with-dropdown", "figure"),
    Input('dropdown-category-verified', 'value'),
    Input('dropdown-category', 'value'))
def display_graph(selected_category_v, selected_category):
    filtered_df = df[(df['verified'] == bool(selected_category_v)) & (df['maincat_10'] == selected_category)]
    fig = px.line(filtered_df, x='date', y='Average', title=f"Data for Category {selected_category}").update_layout(
    {"plot_bgcolor": "rgba(1, 6, 4, 5)", "paper_bgcolor": "rgba(1, 2, 3, 3)"})
    return fig

@app.callback(
    Output("collapse", "is_open"),
    [Input("navbar-toggle", "n_clicks")],
    [State("collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open



if __name__ == "__main__":
     app.run_server(debug=True, )