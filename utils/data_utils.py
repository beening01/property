from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm

# datagokr
# from __future__ import annotations

import json
import logging
from enum import Enum

import requests
import xmltodict
from ratelimit import limits, sleep_and_retry

# Sgis
import pathlib
import time
from typing import Literal


WORK_DIR = Path(__file__).parent.parent
IN_DIR, OUT_DIR = WORK_DIR / "input", WORK_DIR / "output"


##################################################################################
# Datagokr
logger = logging.getLogger(__name__)
class RespType(str, Enum):
    JSON = "json"
    XML = "xml"

    def __str__(self) -> str:
        return self.value


class Datagokr:
    def __init__(self, api_key: str = None) -> None:
        if not api_key:
            raise ValueError(f"invalid api_key, got {api_key!r}")
        self.api_key = api_key

    @sleep_and_retry
    @limits(calls=25, period=1)
    def lawd_code(self, region: str = None, n_rows: int = 1000) -> list[dict]:
        # https://www.data.go.kr/data/15077871/openapi.do
        def _api_call(region: str, n_rows: int, page: int) -> dict:
            url = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
            params = {
                "serviceKey": f"{self.api_key}",
                "pageNo": f"{page}",
                "numOfRows": f"{n_rows}",
                "type": f"{RespType.JSON}",
                "locatadd_nm": region,
            }
            resp = requests.get(url, params=params)
            try:
                return resp.json()
            except json.JSONDecodeError:
                return xmltodict.parse(resp.content)

        page: int = 1
        total_cnt: int = None
        total_page: int = None
        result: list[dict] = []
        while True:
            parsed = _api_call(region=region, n_rows=n_rows, page=page)
            if "StanReginCd" in parsed:
                first, second = parsed.get("StanReginCd", [])
                if not total_cnt:
                    head = first.get("head", [])
                    total_cnt = head[0].get("totalCount", 0)
                row = second.get("row", [])
                if n_rows >= total_cnt:
                    return row
                result += row

                if not total_page:
                    total_page, remainder = divmod(total_cnt, n_rows)
                    if remainder > 0:
                        total_page += 1
                if page >= total_page:
                    return result
                page += 1

            elif "RESULT" in parsed:
                err_code = parsed.get("RESULT", {})
                e_code = err_code.get("resultCode", "")
                e_msg = err_code.get("resultMsg", "")
                raise ValueError(f"[{e_code}] {e_msg}")

            else:
                raise ValueError(f"invalid response, got {parsed!r}")

    @sleep_and_retry
    @limits(calls=25, period=1)
    def apt_trade(self, lawd_code: str, deal_ym: str, n_rows: int = 9999) -> list[dict]:
        # https://www.data.go.kr/data/15126469/openapi.do
        def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
            url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
            params = {
                "serviceKey": f"{self.api_key}",
                "LAWD_CD": f"{lawd_code}",
                "DEAL_YMD": f"{deal_ym}",
                "numOfRows": f"{n_rows}",
                "pageNo": f"{page}",
            }
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return xmltodict.parse(resp.content)

        page: int = 1
        total_cnt: int = None
        result: list[dict] = []
        while True:
            parsed = _api_call(lawd_code=lawd_code, deal_ym=deal_ym, n_rows=n_rows, page=page)
            response: dict = parsed.get("response", {})
            header: dict = response.get("header", {})
            result_code = header.get("resultCode", "")
            if result_code == "000":
                body: dict = response.get("body", {})
                items: dict = body.get("items", {})
                if items:
                    item: list = items.get("item", [])
                    result += item
                    total_cnt = int(body.get("totalCount", 0))
                    if len(result) >= total_cnt:
                        return result
                    page += 1
                else:
                    return result
            else:
                raise ValueError(f'[{result_code}] {header.get("resultMsg","")}')

    @sleep_and_retry
    @limits(calls=25, period=1)
    def apt_trade_detailed(self, lawd_code: str, deal_ym: str, n_rows: int = 1000) -> list[dict]:
        # https://www.data.go.kr/data/15126468/openapi.do
        def _api_call(lawd_code: str, deal_ym: str, n_rows: int, page: int) -> dict:
            url = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
            params = {
                "serviceKey": f"{self.api_key}",
                "LAWD_CD": f"{lawd_code}",
                "DEAL_YMD": f"{deal_ym}",
                "numOfRows": f"{n_rows}",
                "pageNo": f"{page}",
            }
            resp = requests.get(url, params=params)
            resp.raise_for_status()
            return xmltodict.parse(resp.content)

        page: int = 1
        total_cnt: int = None
        result: list[dict] = []
        while True:
            parsed = _api_call(lawd_code=lawd_code, deal_ym=deal_ym, n_rows=n_rows, page=page)
            response: dict = parsed.get("response", {})
            header: dict = response.get("header", {})
            result_code = header.get("resultCode", "")
            if result_code == "000":
                body: dict = response.get("body", {})
                items: dict = body.get("items", {})
                if items:
                    item: list = items.get("item", [])
                    result += item
                    total_cnt = int(body.get("totalCount", 0))
                    if len(result) >= total_cnt:
                        return result
                    page += 1
                else:
                    return result
            else:
                raise ValueError(f'[{result_code}] {header.get("resultMsg","")}')
            

##################################################################################
# Sgis            
logger = logging.getLogger(__name__)

class AuthenticationError(Exception):
    """[-401] 인증정보가 존재하지 않습니다"""

    def __init__(self, *args):
        super().__init__(*args)


class Sgis:
    """통계지리정보서비스 SGIS"""

    def __init__(self, api_key: str, api_sec: str) -> None:
        self.api_key: str = api_key
        self.api_sec: str = api_sec

    @property
    def timeout(self) -> float:
        if hasattr(self, "_timeout"):
            return int(self._timeout) / 1000
        return 0.0

    @property
    def access_token(self) -> str:
        if not hasattr(self, "_token") or self.timeout - 60 * 60 < time.time():
            self.auth()
        return self._token

    def raise_for_err_cd(self, parsed: dict) -> None:
        err_cd, err_msg = parsed.get("errCd", 0), parsed.get("errMsg", "")
        if f"{err_cd}" == "-401":
            raise AuthenticationError(f"[{err_cd}] {err_msg}")
        if err_cd:
            raise ValueError(f"[{err_cd}] {err_msg}")

    def auth(self) -> dict:
        # https://sgis.kostat.go.kr/developer/html/newOpenApi/api/dataApi/basics.html#auth
        url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
        params = dict(consumer_key=self.api_key, consumer_secret=self.api_sec)
        resp = requests.get(url, params=params)
        parsed = resp.json()
        self.raise_for_err_cd(parsed)

        result = parsed.get("result", {})
        self._timeout = result.get("accessTimeout", 0)
        self._token = result.get("accessToken", "")
        return result

    @staticmethod
    def hadm_codes() -> list[dict]:
        # https://sgis.kostat.go.kr/view/board/faqView?post_no=11
        path_to_json = pathlib.Path(__file__).parent / "assets" / "adm_codes_2306.json"
        with open(path_to_json, "r", encoding="utf-8") as fp:
            return json.load(fp)

    def hadm_area(
        self,
        adm_cd: str = None,
        low_search: Literal["0", "1", "2"] = "1",
        year: str = "2023",
        session: requests.Session = None,
    ) -> str:
        """행정구역 코드 이용 행정구역 경계 정보 제공 API(좌표계: WGS84 "EPSG:4326")

        Args:
            adm_cd (str, optional): 행정구역코드. Defaults to None.
            low_search (str, optional): 하위 통계 정보 유무. Defaults to "1".
            year (str, optional): 기준연도("2000" ~ "2023"). Defaults to "2023".
            session (requests.Session, optional): 세션. Defaults to None.

        Returns:
            str: GeoJSON 형식의 결과

        """
        # https://sgis.kostat.go.kr/developer/html/newOpenApi/api/dataApi/addressBoundary.html#hadmarea
        try:
            import geopandas as gpd
        except ImportError:
            raise ImportError("The geopandas package is required for fetching data. You can install it using `pip install -U geopandas`")

        url = "https://sgisapi.kostat.go.kr/OpenAPI3/boundary/hadmarea.geojson"
        params = dict(
            accessToken=self.access_token,
            adm_cd=adm_cd,
            low_search=low_search,
            year=year,
        )
        resp = session.get(url, params=params) if session else requests.get(url, params=params)
        parsed = resp.json()
        self.raise_for_err_cd(parsed)

        gdf_resp: gpd.GeoDataFrame = gpd.read_file(resp.content)
        gdf_resp.set_crs("EPSG:5179", allow_override=True, inplace=True)  # 좌표계: UTM-K "EPSG:5179"
        gdf_filter: gpd.GeoDataFrame = gdf_resp.filter(["adm_cd", "adm_nm", "addr_en", "geometry"])
        return gdf_filter.to_json(drop_id=True, to_wgs84=True, separators=(",", ":"), ensure_ascii=False)  # 좌표계: WGS84 "EPSG:4326"

    def geocode_wgs84(self, address: str, page: int = 0, limit: int = 5, session: requests.Session = None) -> list[dict]:
        """입력된 주소 위치 제공 API(좌표계: WGS84 "EPSG:4326")

        Args:
            address (str): 검색주소
            page (int, optional): 페이지. Defaults to 0.
            limit (int, optional): 결과 수. Defaults to 5.
            session (requests.Session, optional): 세션. Defaults to None.

        Returns:
            list[dict]: 검색결과
        """
        # https://sgis.kostat.go.kr/developer/html/newOpenApi/api/dataApi/addressBoundary.html#geocodewgs84
        url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/geocodewgs84.json"
        params = dict(
            accessToken=self.access_token,
            address=f"{address}",
            pagenum=f"{page}",
            resultcount=f"{limit}",
        )
        for cnt in range(200):
            try:
                resp = session.get(url, params=params) if session else requests.get(url, params=params)
                parsed: dict = resp.json()
                self.raise_for_err_cd(parsed)
                result: dict = parsed.get("result", {})
                return result.get("resultdata", [])
            except AuthenticationError as err:
                logger.warning(f"{err}")
                time.sleep(10)
                self.auth()
            except ValueError as err:
                logger.warning(f"{err}")
                time.sleep(10)
        raise ValueError(f"invalid cnt, {cnt=}")

    def geocode_utmk(self, address: str, page: int = 0, limit: int = 5, session: requests.Session = None) -> list[dict]:
        """입력된 주소 위치 제공 API(좌표계: UTM-K "EPSG:5179")

        Args:
            address (str): 검색주소
            page (int, optional): 페이지. Defaults to 0.
            limit (int, optional): 결과 수. Defaults to 5.
            session (requests.Session, optional): 세션. Defaults to None.

        Returns:
            list[dict]: 검색결과
        """
        # https://sgis.kostat.go.kr/developer/html/newOpenApi/api/dataApi/addressBoundary.html#geocode
        url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/geocode.json"
        params = dict(
            accessToken=self.access_token,
            address=f"{address}",
            pagenum=f"{page}",
            resultcount=f"{limit}",
        )
        resp = session.get(url, params=params) if session else requests.get(url, params=params)
        parsed: dict = resp.json()
        self.raise_for_err_cd(parsed)

        result: dict = parsed.get("result", {})
        return result.get("resultdata", [])
            
if __name__ == "__main__":
    IN_DIR.mkdir(exist_ok=True)
    OUT_DIR.mkdir(exist_ok=True)