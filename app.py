import json, pandas as pd, streamlit as st
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine("sqlite:///data/kepco.db", future=True)

def fmt_date(yymmdd:str):
    if not yymmdd: return ""
    try: return datetime.strptime(yymmdd, "%Y%m%d").strftime("%Y-%m-%d")
    except: return yymmdd

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

st.title("배전망 접속 대상 (오프라인 조회)")
rcpt = st.text_input("접수번호 입력", value="5201-20230120-010178")

if rcpt:
    row = engine.execute(
        "SELECT payload_json, updated_at FROM receipt WHERE rcpt_no = ?", (rcpt,)
    ).fetchone()
    if not row:
        st.warning("DB에 데이터가 없습니다. 먼저 etl.py를 실행해 저장하세요.")
    else:
        data = json.loads(row[0])
        st.caption(f"업데이트: {row[1]}")

        d = data.get("dma_initData", {})
        # 날짜 포맷 교정
        for k in DATE_KEYS:
            d[k] = fmt_date(d.get(k, ""))

        # 라벨 매핑
        pretty = { FIELD_LABELS.get(k,k): v for k,v in d.items() }

        st.dataframe(pd.DataFrame(pretty.items(), columns=["항목","값"]), use_container_width=True)
        st.download_button(
            "CSV 다운로드",
            pd.DataFrame([pretty]).to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{rcpt}.csv",
            mime="text/csv",
        )
