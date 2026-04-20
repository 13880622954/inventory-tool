import streamlit as st
from supabase import create_client

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

st.title("Supabase 连接测试 - 完整查询")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

try:
    response = supabase.table("records").select("*").limit(1).execute()
    st.success("查询成功，数据：")
    st.json(response.data)
except Exception as e:
    st.error(f"查询失败: {e}")
