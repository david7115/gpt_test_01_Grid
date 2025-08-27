import json, pandas as pd, streamlit as st
from sqlalchemy import create_engine, text
from datetime import datetime
import pathlib

# DB 존재 확인
db_path = pathlib.Path("data/kepco.db")
if not db_path.exists():
    st.error("DB 파일(data/kepco.db)이 없습니다. 먼저 etl.py를 실행해 데이터를 저장하세요.")
    st.stop()

engine = create_engine("sqlite:///data/kepco.db", future=True)

def fmt_date(yymmdd: str):
    if not yymmdd: 
        return ""
    try:
        return datetime.strptime(yymmdd, "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return yymmdd

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
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT payload_json, updated_at FROM receipt WHERE rcpt_no = :rcpt"),
                {"rcpt": rcpt}
            ).fetchone()
    except Exception as e:
        st.exception(e)
        st.stop()

    if not row:
        st.warning("DB에 데이터가 없습니다. 먼저 etl.py를 실행해 저장하세요.")
    else:
        try:
            data = json.loads(row[0]) if isinstance(row[0], (str, bytes)) else row[0]
        except Exception:
            st.error("payload_json 파싱에 실패했습니다. ETL 저장 형식을 확인하세요.")
            st.stop()

        st.caption(f"업데이트: {row[1]}")
        d = dict(data.get("dma_initData", {}))

        # 날짜 포맷
        for k in DATE_KEYS:
            if k in d:
                d[k] = fmt_date(d.get(k, ""))

        # 보기 좋은 순서(원하시는 순서로 정렬 가능)
        show_order = [
            "APPLNM","APPLCD","PROGRESSSTATE","GENSOURCENM","EQUIPCAPA",
            "YMD01","YMD02","YMD03","YMD04","YMD05","YMD06","YMD07",
            "JURISOFFICENM","JURISOFFICETEL","UPPOOFFICENM","SUBSTNM",
            "DLNM","DLCD","JURISOFFICECD","UPPOOFFICECD","ACPTSEQNO",
            "MTRNO","PBLCREINFORCE","CNSTRCTNVSOR"
        ]
        pretty = {FIELD_LABELS.get(k, k): d.get(k, "") for k in show_order if k in d}
        # 나머지 키도 포함하고 싶다면:
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
