from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns

from utils.data_utils import OUT_DIR

OUT7 = OUT_DIR / "geojson.png"

def geojson_to_img():
    sns.set_theme(context="poster", font="Malgun Gothic")
    fig, ax = plt.subplots(figsize=(16, 9), dpi=100)
    gdf: gpd.GeoDataFrame = gpd.read_file(OUT_DIR / "merge.geojson", encoding="utf-8")
    
    gdf.plot(column="avg_price", cmap="OrRd", edgecolor="k", legend=True, 
        legend_kwds={"label": "(단위:만원)", "orientation": "vertical"},
        ax=ax,)
    ax.set_axis_off()
    ax.set_title("단위 면적당 평균 아파트 매매 실거래가")
    fig.set_layout_engine("tight")
    fig.savefig(OUT7)

if __name__ == "__main__":
    geojson_to_img()