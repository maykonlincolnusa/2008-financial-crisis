import streamlit as st
import pandas as pd
import plotly.express as px
import networkx as nx

from pathlib import Path

st.set_page_config(page_title="Financial Crisis Simulator", layout="wide")

st.title("Financial Crisis Simulator Dashboard")

DATA_DIR = Path("data")
PROCESSED = DATA_DIR / "processed"
STAGING = DATA_DIR / "staging"

st.header("Crisis probability")
financial_path = DATA_DIR / "financial_dataset.parquet"
if financial_path.exists():
    fin = pd.read_parquet(financial_path)
    if "crisis_probability_score" in fin.columns:
        st.line_chart(fin[["crisis_probability_score"]])
    else:
        st.dataframe(fin.head(50))
else:
    st.info("No financial dataset found. Build data/financial_dataset.parquet.")

st.header("Macro indicators")
macro_path = STAGING / "macro.parquet"
if macro_path.exists():
    macro = pd.read_parquet(macro_path)
    if "value" in macro.columns and "date" in macro.columns:
        fig = px.line(macro, x="date", y="value", color="series" if "series" in macro.columns else None)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(macro.head(50))
else:
    st.info("No macro data available.")

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

st.header("Simulation results")
sim_path = DATA_DIR / "simulation_results.parquet"
if sim_path.exists():
    sim = pd.read_parquet(sim_path)
    st.dataframe(sim.head(50))
    if "bank_equity" in sim.columns:
        st.line_chart(sim[["bank_equity", "household_equity"]])
else:
    st.info("No simulation results found.")

st.header("Risk heatmap (mock)")
mock = pd.DataFrame({
    "country": ["USA", "UK", "DE", "BR", "CN"],
    "risk": [0.65, 0.55, 0.4, 0.6, 0.7],
})
fig = px.choropleth(mock, locations="country", locationmode="country names", color="risk")
st.plotly_chart(fig, use_container_width=True)

st.header("Model metrics")
st.info("Metrics are logged in MLflow. Start MLflow UI to inspect experiments.")
