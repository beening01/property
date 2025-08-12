from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm
import geopandas as gpd

from .data_utils import OUT_DIR, Datagokr, Sgis

#############################################################
# 법정동 데이터
OUT2 = OUT_DIR / "region_code.csv"
load_dotenv()
DATAGO_KEY = os.getenv("DATAGO_KEY")

def sido_sgg_to_csv(region:str = None):
    datago = Datagokr(DATAGO_KEY)
    resp = datago.lawd_code(region)
    df = pd.DataFrame(resp)
    df["sido_sgg"] = df['sido_cd'] + df['sgg_cd']

    f_no_sgg = df["sgg_cd"] == "000"    # 시군구가 000인 경우
    f_no_umd = df["umd_cd"] == "000"    # 읍면동이 000인 경우
    f_no_ri = df["ri_cd"] == "00"    # 리가 000인 경우
    f_only_ssg = (~f_no_sgg) & (f_no_umd) & (f_no_ri)    # 시군구만 있는 경우
    df_sliced = df.loc[f_only_ssg]


    df_filter = df_sliced.filter(["sido_sgg", "locatadd_nm"])   # 사용할 열
    df_sort = df_filter.sort_values("locatadd_nm")   # 지역 주소명으로 오름차순
    df_result = df_sort.reset_index(drop=True)
    df_result.to_csv(OUT2, index=False)

#####################################################################################
# 아파트 매매 실거래가 데이터
OUT3 = OUT_DIR / "apt_trade.csv"
def apt_trade_to_csv():
    df_addr = pd.read_csv(OUT2, dtype="string")
    addr_list = df_addr.values.tolist()
    datagokr = Datagokr(DATAGO_KEY)
    yyyymm_range = [f"2024{m:02}" for m in range(1, 13)]    # 계약년월
    result = []
    with tqdm(total=len(addr_list)) as pbar:    # tqdm: 진행표시줄
        for code, addr in addr_list:
            for yyyymm in yyyymm_range:
                pbar.set_description(f"[{addr:20}[{code}{yyyymm}]]")
                resp = datagokr.apt_trade(code, yyyymm)   # 실거래가 조회
                result += resp
            pbar.update()
    
    df = pd.DataFrame(result)
    df_filter = df.filter(["sggCd", "dealYear", "dealMonth", "dealingGbn", "umdNm", "aptNm", "excluUseAr", "dealAmount", "cdealDay"])
    df_filter.columns = ["지역코드", "계약연도", "계약월", "거래유형", "법정동", "단지명", "전용면적", "거래금액", "해제사유발생일"]
    f_is_real_deal = df_filter["해제사유발생일"].isna()
    df_real = df_filter.loc[f_is_real_deal]    # 취소되지 않은 데이터

    df_real = df_real.drop(columns=["해제사유발생일"])
    df_real.to_csv(OUT3, index=False)


#####################################################################################
# 행정구역 경계 데이터
OUT4 = OUT_DIR / "geo_data.geojson"
def adm_cd_to_geojson(adm_cd: str = None, low_search: str = "1")->None:
    SGIS_ID = os.getenv("SGIS_ID")
    SGIS_KEY = os.getenv("SGIS_KEY")
    sgis = Sgis(SGIS_ID, SGIS_KEY)  # Sgis 객체 생성
    resp: str = sgis.hadm_area(adm_cd=adm_cd, low_search=low_search)  # GeoJSON 형식의 문자열 반환
    gdf_resp : gpd.GeoDataFrame = gpd.read_file(resp)
    gdf_resp.plot()
    OUT4.write_text(resp, encoding="utf-8")  # GeoJSON 형식의 텍스트 파일로 저장


if __name__ == "__main__":
    from data_utils import OUT_DIR, Datagokr, Sgis
    # sido_sgg_to_csv(region="서울특별시")
    # apt_trade_to_csv()
    adm_cd_to_geojson("11", "1")    # 서울특별시, 시군구 단위