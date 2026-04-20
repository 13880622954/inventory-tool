import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import datetime

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="文件管理助手", layout="wide")
st.title("📁 文件管理助手 - 纯文本版")

# 侧边栏添加记录
with st.sidebar:
    st.header("➕ 添加记录")
    with st.form("add_form"):
        danhao = st.text_input("单号 *")
        event = st.text_input("事件")
        remark = st.text_area("备注")
        submitted = st.form_submit_button("保存")
        if submitted and danhao:
            now = datetime.now().isoformat()
            supabase.table("records").insert({
                "danhao": danhao,
                "event": event,
                "remark": remark,
                "upload_time": now
            }).execute()
            st.success("保存成功")
            st.rerun()

# 显示记录
data = supabase.table("records").select("*").order("upload_time", desc=True).execute()
df = pd.DataFrame(data.data)
if not df.empty:
    st.dataframe(df[["danhao", "event", "remark", "upload_time"]])
else:
    st.info("暂无记录")
