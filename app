import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ── Chemins ──
BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "raw" / "ventes_retail.csv"

# ── Configuration de la page ──
st.set_page_config(
    page_title="Retail Data Lake — Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 Retail Data Lake — Analyse des Ventes Tunisie")
st.markdown("**Master FAVD** · 100 000 transactions · 2022–2024")
st.divider()

# ── Chargement des données ──
@st.cache_data
def load_data():
    df = pd.read_csv(CSV_PATH)
    df['date'] = pd.to_datetime(df['date'])
    df['promo'] = df['discount_pct'].apply(lambda x: 'Avec promotion' if x > 0 else 'Sans promotion')
    df['periode'] = df['year'].astype(str) + '-' + df['month'].astype(str).str.zfill(2)
    return df

df = load_data()
st.success(f"✅ {len(df):,} transactions chargées · {df['date'].min().date()} → {df['date'].max().date()}")

# ── Sidebar : Filtres ──
st.sidebar.header("🔎 Filtres")

regions = ["Toutes"] + sorted(df['region'].unique().tolist())
region_sel = st.sidebar.selectbox("Région", regions)

annees = st.sidebar.multiselect("Année(s)", sorted(df['year'].unique()), default=sorted(df['year'].unique()))

categories = ["Toutes"] + sorted(df['category'].unique().tolist())
cat_sel = st.sidebar.selectbox("Catégorie", categories)

# ── Appliquer les filtres ──
dff = df.copy()
if region_sel != "Toutes":
    dff = dff[dff['region'] == region_sel]
if annees:
    dff = dff[dff['year'].isin(annees)]
if cat_sel != "Toutes":
    dff = dff[dff['category'] == cat_sel]

# ── KPIs en haut ──
k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 CA Total", f"{dff['total_ttc'].sum()/1e6:.2f} M TND")
k2.metric("🧾 Transactions", f"{len(dff):,}")
k3.metric("🏪 Magasins", f"{dff['store_name'].nunique()}")
k4.metric("📦 Catégories", f"{dff['category'].nunique()}")

st.divider()

fmt = mticker.FuncFormatter(lambda x, _: f'{x/1e6:.1f}M')

# ── Onglets ──
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📍 CA par Région",
    "📈 Évolution mensuelle",
    "📦 CA par Catégorie",
    "🏷️ Impact Promotions",
    "🏆 Top Magasins",
    "👥 Segments Clients"
])

# ── Tab 1 : CA par Région ──
with tab1:
    st.subheader("Chiffre d'Affaires par Région")
    ca_region = dff.groupby('region')['total_ttc'].sum().sort_values(ascending=False)
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(ca_region.index, ca_region.values,
                  color=['#2196F3', '#4CAF50', '#FF9800', '#E91E63', '#9C27B0'][:len(ca_region)])
    ax.set_xlabel('Région')
    ax.set_ylabel('CA Total (TND)')
    ax.yaxis.set_major_formatter(fmt)
    for bar, val in zip(bars, ca_region.values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + ca_region.max()*0.02,
                f'{val/1e6:.1f}M', ha='center', fontweight='bold')
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()
    with st.expander("📊 Données détaillées"):
        st.dataframe(ca_region.reset_index().rename(columns={'total_ttc': 'CA (TND)'}))

# ── Tab 2 : Évolution mensuelle ──
with tab2:
    st.subheader("Évolution Mensuelle du CA (2022–2024)")
    evolution = dff.groupby(['year', 'month'])['total_ttc'].sum().reset_index()
    evolution['periode'] = evolution['year'].astype(str) + '-' + evolution['month'].astype(str).str.zfill(2)
    evolution = evolution.sort_values('periode')
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(evolution['periode'], evolution['total_ttc'],
            color='#2196F3', linewidth=2, marker='o', markersize=4)
    ax.set_xlabel('Période')
    ax.set_ylabel('CA (TND)')
    ax.yaxis.set_major_formatter(fmt)
    plt.xticks(rotation=45, ha='right', fontsize=7)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

# ── Tab 3 : CA par Catégorie ──
with tab3:
    st.subheader("CA par Catégorie de Produit")
    ca_cat = dff.groupby('category')['total_ttc'].sum().sort_values(ascending=True)
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.barh(ca_cat.index, ca_cat.values, color='#4CAF50')
    ax.set_xlabel('CA Total (TND)')
    ax.xaxis.set_major_formatter(fmt)
    for bar, val in zip(bars, ca_cat.values):
        ax.text(bar.get_width() + ca_cat.max()*0.01,
                bar.get_y() + bar.get_height()/2,
                f'{val/1e6:.1f}M', va='center', fontweight='bold')
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()
    with st.expander("📊 Données détaillées"):
        st.dataframe(ca_cat.sort_values(ascending=False).reset_index().rename(columns={'total_ttc': 'CA (TND)'}))

# ── Tab 4 : Impact Promotions ──
with tab4:
    st.subheader("Impact des Promotions sur le CA")
    col_a, col_b = st.columns([2, 1])
    promo = dff.groupby('promo')['total_ttc'].sum()
    with col_a:
        fig, ax = plt.subplots(figsize=(7, 5))
        bars = ax.bar(promo.index, promo.values, color=['#FF9800', '#2196F3'], width=0.5)
        ax.set_ylabel('CA Total (TND)')
        ax.yaxis.set_major_formatter(fmt)
        for bar, val in zip(bars, promo.values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + promo.max()*0.02,
                    f'{val/1e6:.1f}M', ha='center', fontsize=12, fontweight='bold')
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
    with col_b:
        st.metric("Avec promotion", f"{promo.get('Avec promotion', 0)/1e6:.2f} M TND")
        st.metric("Sans promotion", f"{promo.get('Sans promotion', 0)/1e6:.2f} M TND")
        total = promo.sum()
        pct = promo.get('Avec promotion', 0) / total * 100 if total > 0 else 0
        st.metric("Part promo", f"{pct:.1f}%")

# ── Tab 5 : Top Magasins ──
with tab5:
    st.subheader("Top Magasins par CA")
    n_top = st.slider("Nombre de magasins", 5, 20, 10)
    top_magasins = dff.groupby('store_name')['total_ttc'].sum().sort_values(ascending=False).head(n_top)
    fig, ax = plt.subplots(figsize=(10, max(5, n_top * 0.5)))
    bars = ax.barh(top_magasins.index[::-1], top_magasins.values[::-1], color='#9C27B0')
    ax.set_xlabel('CA Total (TND)')
    ax.xaxis.set_major_formatter(fmt)
    for bar, val in zip(bars, top_magasins.values[::-1]):
        ax.text(bar.get_width() + top_magasins.max()*0.01,
                bar.get_y() + bar.get_height()/2,
                f'{val/1e6:.2f}M', va='center', fontweight='bold')
    plt.tight_layout()
    st.pyplot(fig)
    plt.close()

# ── Tab 6 : Segments Clients ──
with tab6:
    st.subheader("Répartition du CA par Segment Client")
    col_c, col_d = st.columns([1, 1])
    segment = dff.groupby('segment')['total_ttc'].sum()
    with col_c:
        fig, ax = plt.subplots(figsize=(7, 7))
        colors_pie = ['#2196F3', '#4CAF50', '#FF9800', '#E91E63']
        wedges, texts, autotexts = ax.pie(
            segment.values,
            labels=segment.index,
            autopct='%1.1f%%',
            colors=colors_pie[:len(segment)],
            startangle=90,
            textprops={'fontsize': 12}
        )
        for at in autotexts:
            at.set_fontweight('bold')
        plt.tight_layout()
        st.pyplot(fig)
        plt.close()
    with col_d:
        st.dataframe(
            segment.sort_values(ascending=False)
                   .reset_index()
                   .rename(columns={'total_ttc': 'CA (TND)', 'segment': 'Segment'})
                   .assign(**{'CA (TND)': lambda x: x['CA (TND)'].apply(lambda v: f'{v/1e6:.2f} M')})
        )

st.divider()
st.caption("Master FAVD · Retail Data Lake · Tunisie 2022–2024")
