import streamlit as st
import geopandas as gpd
import plotly.express as px
from pathlib import Path

from utils.data_utils import OUT_DIR

# 데이터 경로 설정
geojson_path = OUT_DIR / "merge.geojson"

# 데이터 읽기
gdf: gpd.GeoDataFrame = gpd.read_file(geojson_path, encoding="utf-8")

# Streamlit 앱 제목
st.title("서울시 단위 면적당 평균 아파트 매매 실거래가")

# 색상 범위 설정
min_price = gdf["avg_price"].min()
max_price = gdf["avg_price"].max()

# 가격 범위 슬라이더
price_range = st.slider(
    "가격 범위 선택 (단위: 만원)",
    min_value=int(min_price),
    max_value=int(max_price),
    value=(int(min_price), int(max_price))
)

# 범위 필터 적용
filtered_gdf = gdf[(gdf["avg_price"] >= price_range[0]) & (gdf["avg_price"] <= price_range[1])]

# Plotly Choropleth 지도
fig = px.choropleth_mapbox(
    filtered_gdf,
    geojson=filtered_gdf.geometry.__geo_interface__,
    locations=filtered_gdf.index,
    color="avg_price",
    color_continuous_scale="OrRd",
    range_color=(min_price, max_price),
    mapbox_style="carto-positron",
    zoom=10,
    center={"lat": 37.5665, "lon": 126.9780},
    opacity=0.7,
    labels={"avg_price": "평균가(만원)"},
    hover_data={"avg_price": True}
)

st.plotly_chart(fig, use_container_width=True)