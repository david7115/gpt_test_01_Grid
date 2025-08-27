# app.py  — auto ETL when DB/record missing
import json, re, time, pathlib
import pandas as pd
import streamlit as st
import requests
from datetime import datetime
from sqlalchemy import create_engine, text

# =========================
# Config
# =========================
DB_PATH = pathlib.Path("data/kepco.db")
DATA_DIR = DB_PATH.parent
API_URL = "https://online.kepco.co.kr/ew/status/pwtr/initInfo"
REF_URL = "https://online.kepco.or.kr/EWM079D00"
HEADERS = {
    "Accept": "application/json",
    'Content-Type': 'application/json; charset="UTF-8"',
    "Origin": "https://online.kepco.or.kr",
    "Referer": REF_URL,
    "User-Agent": "Mozilla/5.0 (kepco offline app)",
    "submissionid": "mf_wfm_layout_sbm_init",
}

FIELD_LABELS = {
    "PROGRESSSTATE": "진행상태코드",
    "GENSOURCENM": "발전원",
    "DLCD": "지사코드",
    "JURISOFFICENM": "관할지사",
    "CNSTRCTNVSOR": "시공사",
    "GENINSTCLNM": "설비종별",
    "ACPTSEQNO": "접수순번",
    "PBLCREINFORCE": "공용보강",
    "UPPOOFFICENM": "상위본부",
    "JURISOFFICETEL": "지사전화",
    "APPLNM": "신청자",
    "EQUIPCAPA": "설비용량(kW)",
    "DLNM": "전력구분",
    "MTRNO": "계기번호",
    "JURISOFFICECD": "관할지사코드",
    "SUBSTNM": "변전소",
    "UPPOOFFICECD": "상위본부코드",
    "APPLCD": "신청코드",
    "YMD01": "접수일",
    "YMD02": "검토통보일",
    "YMD03": "계약체결일",
    "YMD04": "공사완료일",
    "YMD05": "준공검사일",
    "YMD06": "계통연계예정일",
    "YMD07": "비고일자",
}
DATE_KEYS = {"YMD01","YMD02","YMD03","YMD04","YMD05","YMD06","YMD07"}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS receipt (
  rcpt_no TEXT PRIMARY KEY,
  payload_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

# =========================
# Helpers
# =========================
def fmt_date(yymmdd: str) -> str:
    if not yymmdd:
        return ""
    try:
        return datetime.strptime(yymmdd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return yymmdd

def rcpt_to_keynum(rcpt_no: str) -> str:
    # 5201-20230120-010178 -> 520120230120010178
    return re.sub(r"[^0-9]", "", rcpt_no)

def ensure_db(engine):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with engine.begin() as conn:
        conn.exec_driver_sql(SCHEMA_SQL)

def fetch_from_api(rcpt_no: str) -> dict:
    payload = {"dma_initInfo": {"gubun": "A", "keynum": rcpt_to_keynum(rcpt_no)}}
    # 간단한 재시도(최대 3회)
    last_err = None
    for _ in range(3):
        try:
            r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.2)
    raise last_err

def upsert_receipt(engine, rcpt_no: str, js: dict):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO receipt (rcpt_no, payload_json, updated_at)
                VALUES (:k, :p, :u)
                ON CONFLICT(rcpt_no) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at   = excluded.updated_at
            """),
            {"k": rcpt_no, "p": json.dumps(js, ensure_ascii=False), "u": now}
        )

def get_receipt(engine, rcpt_no: str):
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT payload_json, updated_at FROM receipt WHERE rcpt_no = :rcpt"),
            {"rcpt": rcpt_no}
        ).fetchone()
    return row

# =========================
# UI
# =========================
st.set_page_config(page_title="배전망 접속 대상 (오프라인 조회)", layout="wide")
st.title("배전망 접속 대상 (오프라인 조회)")

rcpt = st.text_input("접수번호 입력", value="5201-20230120-010178")

if rcpt:
    engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
    # 1) DB/스키마 자동 보장
    try:
        ensure_db(engine)
    except Exception as e:
        st.error("DB 초기화에 실패했습니다.")
        st.exception(e)
        st.stop()

    # 2) DB 조회 → 없으면 즉시 수집·저장
    row = get_receipt(engine, rcpt)
    if not row:
        with st.spinner("DB에 없어 한전 API에서 자동 수집 중..."):
            try:
                js = fetch_from_api(rcpt)
                upsert_receipt(engine, rcpt, js)
                row = get_receipt(engine, rcpt)
            except Exception as e:
                st.error("자동 수집에 실패했습니다. 네트워크/방화벽/요청정보를 확인하세요.")
                st.exception(e)
                st.stop()

    # 3) 표시
    try:
        payload = row[0]
        updated_at = row[1]
        data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
    except Exception:
        st.error("payload_json 파싱 오류")
        st.stop()

    st.caption(f"업데이트: {updated_at}")
    d = dict(data.get("dma_initData", {}))

    # 날짜 포맷
    for k in DATE_KEYS:
        if k in d:
            d[k] = fmt_date(d.get(k, ""))

    # 보기 좋은 순서
    show_order = [
        "APPLNM","APPLCD","PROGRESSSTATE","GENSOURCENM","EQUIPCAPA",
        "YMD01","YMD02","YMD03","YMD04","YMD05","YMD06","YMD07",
        "JURISOFFICENM","JURISOFFICETEL","UPPOOFFICENM","SUBSTNM",
        "DLNM","DLCD","JURISOFFICECD","UPPOOFFICECD","ACPTSEQNO",
        "MTRNO","PBLCREINFORCE","CNSTRCTNVSOR"
    ]
    pretty = {FIELD_LABELS.get(k, k): d.get(k, "") for k in show_order if k in d}
    # 나머지 키도 포함
    for k, v in d.items():
        if k not in show_order:
            pretty[FIELD_LABELS.get(k, k)] = v

    st.dataframe(pd.DataFrame(pretty.items(), columns=["항목", "값"]), use_container_width=True)
    st.download_button(
        "CSV 다운로드",
        pd.DataFrame([pretty]).to_csv(index=False).encode("utf-8-sig"),
        file_name=f"{rcpt}.csv",
        mime="text/csv",
    )
