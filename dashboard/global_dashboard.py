import streamlit as st
import pandas as pd
import plotly.express as px
import networkx as nx

from pathlib import Path

st.set_page_config(page_title="Global Financial Intelligence", layout="wide")

st.title("GLOBAL FINANCIAL INTELLIGENCE PLATFORM")

DATA_DIR = Path("data")
GLOBAL_LAKE = DATA_DIR / "global_lake"

st.header("Global risk heatmap (mock)")
mock = pd.DataFrame({
    "country": ["USA", "UK", "DE", "BR", "CN"],
    "risk": [0.65, 0.55, 0.4, 0.6, 0.7],
})
fig = px.choropleth(mock, locations="country", locationmode="country names", color="risk")
st.plotly_chart(fig, use_container_width=True)

st.header("Financial network graph")
exposures = pd.DataFrame({
    "source": ["BankA", "BankA", "BankB"],
    "target": ["BankB", "BankC", "BankC"],
    "exposure": [100, 50, 75],
})

g = nx.DiGraph()
for _, row in exposures.iterrows():
    g.add_edge(row["source"], row["target"], weight=row["exposure"])

pos = nx.spring_layout(g, seed=42)
node_x, node_y, labels = [], [], []
for n in g.nodes():
    x, y = pos[n]
    node_x.append(x)
    node_y.append(y)
    labels.append(n)

fig_net = px.scatter(x=node_x, y=node_y, text=labels)
fig_net.update_traces(marker=dict(size=20))
st.plotly_chart(fig_net, use_container_width=True)

st.header("Country risk ranking")
st.dataframe(mock.sort_values("risk", ascending=False))

st.header("Crisis probability indicators")
st.line_chart(pd.DataFrame({"t": range(10), "prob": [0.1, 0.12, 0.15, 0.2, 0.25, 0.3, 0.28, 0.35, 0.4, 0.45]}))

st.header("Fraud alerts")
st.info("No real-time alerts configured. Use monitoring module to push alerts.")
