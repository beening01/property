from pathlib import Path
import pandas as pd 
import geopandas as gpd
from utils.data_load import OUT_DIR, OUT2, OUT3, OUT4

OUT5 = OUT_DIR / "avg_price.csv"

def avg_price_to_csv():
    df_apt = pd.read_csv(OUT3, dtype="string")
    df_apt["거래금액"] = df_apt["거래금액"].str.replace(",", "")    # 콤마 제거
    df_apt = df_apt.astype({"전용면적": float, "거래금액": int})    # 숫자 변환
    df_apt["면적당금액"] = df_apt["거래금액"] / df_apt["전용면적"]
    df_pivot = df_apt.pivot_table(index="지역코드", values=["전용면적", "면적당금액"], aggfunc="mean")
    df_reindex = df_pivot.reset_index(drop=False)
    
    df_sido_sgg = pd.read_csv(OUT2, dtype="string")
    df_merge = pd.merge(df_reindex, df_sido_sgg, left_on="지역코드", right_on="sido_sgg", how="inner")
    df_filter = df_merge.filter(["sido_sgg", "locatadd_nm", "전용면적", "면적당금액"])
    df_filter.columns =  ["sido_sgg", "locatadd_nm", "avg_area", "avg_price"]
    df_sort = df_filter.sort_values("locatadd_nm")
    df_sort.to_csv(OUT5, index=False)

OUT6 = OUT_DIR / "merge.geojson"
def merge_datatframe():
    gdf_geo = gpd.read_file(OUT4, encoding="utf-8")    # 행정구역 경계
    gdf_price = gpd.read_file(OUT5, encoding="utf-8")    # 실거래가
    gdf_merge = pd.merge(gdf_geo, gdf_price, left_on="adm_nm", right_on="locatadd_nm", how="inner")

    gdf_filter = gdf_merge.filter(["adm_nm", "avg_area", "avg_price", "geometry"])
    gdf_result = gdf_filter.astype({"avg_area":float, "avg_price":float})
    str_jsoned = gdf_result.to_json(drop_id=True, ensure_ascii=False, indent=2)
    OUT6.write_text(str_jsoned, encoding="utf-8")

if __name__ == "__main__":
    # avg_price_to_csv()
    merge_datatframe()


    