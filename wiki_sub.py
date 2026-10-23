import os 
import json
from google.cloud import pubsub_v1
import datetime
from dotenv import load_dotenv
load_dotenv()


pubsub_client = pubsub_v1.PublisherClient()
subsciber_client = pubsub_v1.SubscriberClient()

# Define the Pub/Sub topic and subscription names
project_id = os.getenv('PROJECT_ID')
topic_name = os.getenv('topic')
subscription_name = os.getenv('subscription_name', 'wiki-subscription')  # Default subscription name if not set


# Subscription path
subscription_path = subsciber_client.subscription_path(project_id, subscription_name)


def ensure_subscription_exists():
    """
    Ensure that the Pub/Sub subscription exists. If it doesn't, create it.
    """
    try:
        subsciber_client.get_subscription(subscription_path)
        print(f"Subscription '{subscription_name}' already exists.")
    except Exception as e:
        print(f"Subscription '{subscription_name}' does not exist. Creating it now...")
        topic_path = pubsub_client.topic_path(project_id, topic_name)
        subsciber_client.create_subscription(name=subscription_path, topic=topic_path)
        print(f"Subscription '{subscription_name}' created successfully.")


def callback(message):
    """
    Callback function to process incoming Pub/Sub messages.

    Args:
        message (google.cloud.pubsub_v1.subscriber.message.Message): The incoming Pub/Sub message.
    """
    transformed_message = transform_message(message)
    print(json.dumps(transformed_message))

    message.ack()  # Acknowledge the message

def transform_message(message):
    event = json.loads(message.data.decode('utf-8'))
    length = event.get("length") or {}
    old_len, new_len = length.get("old", 0), length.get("new", 0)
    byte_change = new_len - old_len if length else None

    return {
            "id": event.get("id"),
            "type": event.get("type"),
            "wiki": event.get("wiki"),
            "domain": event.get("domain"),
            "namespace": event.get("namespace"),
            "title": event.get("title"),
            "user": event.get("user"),
            "bot": event.get("bot"),
            "minor": event.get("minor"),
            "comment": event.get("comment"),
            "byte_change": byte_change,
            "old_revision": event.get("revision", {}).get("old"),
            "new_revision": event.get("revision", {}).get("new"),
            "timestamp": datetime.datetime.fromtimestamp(event.get("timestamp", 0)).isoformat() if event.get("timestamp") else None,
            "url": event.get("title_url")
        }


# Streamlit dashboard to display the messages in real-time
import threading
from collections import deque

import altair as alt
import pandas as pd
import streamlit as st

MAX_EVENTS = 20000  # Rolling buffer of recent events kept in memory
HUMAN_COLOR, BOT_COLOR = "#2a78d6", "#eb6834"


class EventStore:
    """
    Thread-safe rolling buffer shared by every browser session. The Pub/Sub callback
    runs on a background thread and appends here; the dashboard reads snapshots.
    """

    def __init__(self):
        self.events = deque(maxlen=MAX_EVENTS)
        self.lock = threading.Lock()
        self.total_received = 0
        self.started_at = pd.Timestamp.now()

    def add(self, event):
        with self.lock:
            self.events.append(event)
            self.total_received += 1

    def snapshot(self):
        with self.lock:
            return list(self.events), self.total_received


@st.cache_resource
def start_subscriber():
    """
    Start the Pub/Sub subscriber once per Streamlit server.
    """
    store = EventStore()

    def store_callback(message):
        event = transform_message(message)
        event["received_at"] = pd.Timestamp.now()
        store.add(event)
        message.ack()

    streaming_pull_future = subsciber_client.subscribe(subscription_path, callback=store_callback)
    print(f"Listening for messages on {subscription_path}...")
    return store, streaming_pull_future


def to_dataframe(events):
    df = pd.DataFrame(events)
    df["bot"] = df["bot"].fillna(False).astype(bool)
    df["editor_type"] = df["bot"].map({True: "Bot", False: "Human"})
    df["byte_change"] = pd.to_numeric(df["byte_change"], errors="coerce")
    df["event_time"] = pd.to_datetime(df["timestamp"], errors="coerce").fillna(df["received_at"])
    return df


def bar_chart(df, field, title, top_n=10):
    counts = df[field].value_counts().head(top_n).rename_axis(field).reset_index(name="edits")
    return (
        alt.Chart(counts, title=title)
        .mark_bar(color=HUMAN_COLOR, cornerRadiusEnd=4)
        .encode(
            x=alt.X("edits:Q", title="Events"),
            y=alt.Y(f"{field}:N", sort="-x", title=None),
            tooltip=[alt.Tooltip(f"{field}:N"), alt.Tooltip("edits:Q", title="Events")],
        )
        .properties(height=30 * len(counts) + 40)
    )


st.set_page_config(page_title="Wikipedia Live Changes", layout="wide")
st.title("Real-time Wikipedia Changes")

# ensure_subscription_exists()
store, streaming_pull_future = start_subscriber()

# Sidebar filters (changing one reruns the whole app)
with st.sidebar:
    st.header("Filters")
    window_minutes = st.select_slider("Time window (minutes)", options=[1, 5, 15, 30, 60], value=15)
    include_bots = st.toggle("Include bot edits", value=True)
    event_types = st.multiselect(
        "Event types",
        ["edit", "new", "log", "categorize"],
        default=["edit", "new", "log", "categorize"],
    )
    wiki_filter = st.text_input("Wiki contains (e.g. enwiki)", "")
    refresh_seconds = st.select_slider("Refresh every (seconds)", options=[1, 2, 5, 10], value=2)


@st.fragment(run_every=refresh_seconds)
def render_dashboard():
    events, total_received = store.snapshot()

    if streaming_pull_future.done():
        st.error("The Pub/Sub subscriber stopped. Check the terminal for errors and restart the app.")

    if not events:
        st.info(f"Waiting for messages on `{subscription_path}`...")
        return

    df = to_dataframe(events)
    now = pd.Timestamp.now()
    df = df[df["received_at"] >= now - pd.Timedelta(minutes=window_minutes)]
    if not include_bots:
        df = df[~df["bot"]]
    if event_types:
        df = df[df["type"].isin(event_types)]
    if wiki_filter:
        df = df[df["wiki"].fillna("").str.contains(wiki_filter, case=False)]

    if df.empty:
        st.warning("No events match the current filters yet.")
        return

    # KPI row
    last_minute = df[df["received_at"] >= now - pd.Timedelta(minutes=1)]
    edits = df[df["type"].isin(["edit", "new"])]
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Events in window", f"{len(df):,}", help=f"{total_received:,} received since {store.started_at:%H:%M:%S}")
    col2.metric("Events / minute", f"{len(last_minute):,}")
    col3.metric("Unique editors", f"{df['user'].nunique():,}")
    col4.metric("Bot share", f"{df['bot'].mean():.0%}")
    col5.metric("New pages", f"{(df['type'] == 'new').sum():,}")
    col6.metric("Net bytes changed", f"{int(edits['byte_change'].sum()):+,}")

    # Activity over time, human vs bot
    activity = (
        df.set_index("received_at")
        .groupby("editor_type")
        .resample("10s")
        .size()
        .rename("events")
        .reset_index()
    )
    activity_chart = (
        alt.Chart(activity, title="Activity over time (events per 10 seconds)")
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X("received_at:T", title=None),
            y=alt.Y("events:Q", title="Events"),
            color=alt.Color(
                "editor_type:N",
                title=None,
                scale=alt.Scale(domain=["Human", "Bot"], range=[HUMAN_COLOR, BOT_COLOR]),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=[
                alt.Tooltip("received_at:T", title="Time", format="%H:%M:%S"),
                alt.Tooltip("editor_type:N", title="Editor"),
                alt.Tooltip("events:Q", title="Events"),
            ],
        )
        .properties(height=260)
    )
    st.altair_chart(activity_chart, use_container_width=True)

    # Breakdowns
    left, middle, right = st.columns(3)
    left.altair_chart(bar_chart(df, "wiki", "Most active wikis"), use_container_width=True)
    middle.altair_chart(bar_chart(df, "type", "Event types"), use_container_width=True)
    right.altair_chart(bar_chart(df[~df["bot"]], "user", "Most active human editors"), use_container_width=True)

    # Trending pages and biggest changes
    left, right = st.columns(2)
    with left:
        st.subheader("Trending pages")
        trending = (
            df.groupby(["title", "wiki"])
            .agg(edits=("id", "size"), editors=("user", "nunique"), url=("url", "last"))
            .sort_values(["edits", "editors"], ascending=False)
            .head(10)
            .reset_index()
        )
        st.dataframe(
            trending,
            hide_index=True,
            use_container_width=True,
            column_config={"url": st.column_config.LinkColumn("Link", display_text="open")},
        )
    with right:
        st.subheader("Biggest changes")
        biggest = edits.dropna(subset=["byte_change"])
        biggest = biggest.reindex(biggest["byte_change"].abs().sort_values(ascending=False).index).head(10)
        st.dataframe(
            biggest[["title", "wiki", "user", "byte_change", "url"]],
            hide_index=True,
            use_container_width=True,
            column_config={
                "byte_change": st.column_config.NumberColumn("Bytes", format="%+d"),
                "url": st.column_config.LinkColumn("Link", display_text="open"),
            },
        )

    # Live feed
    st.subheader("Latest changes")
    latest = df.sort_values("received_at", ascending=False).head(50)
    st.dataframe(
        latest[["event_time", "type", "wiki", "title", "user", "bot", "minor", "byte_change", "comment", "url"]],
        hide_index=True,
        use_container_width=True,
        column_config={
            "event_time": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
            "byte_change": st.column_config.NumberColumn("Bytes", format="%+d"),
            "url": st.column_config.LinkColumn("Link", display_text="open"),
        },
    )


render_dashboard()