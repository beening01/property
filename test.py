from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm
import geopandas as gpd
from utils.data_utils import Sgis

load_dotenv()
SGIS_ID = os.getenv("SGIS_ID")
SGIS_KEY = os.getenv("SGIS_KEY")
sgis = Sgis(SGIS_ID, SGIS_KEY)  # Sgis 객체 생성
resp: str = sgis.hadm_area(adm_cd="11", low_search="1")  # GeoJSON 형식의 문자열 반환
gdf_resp : gpd.GeoDataFrame = gpd.read_file(resp)

