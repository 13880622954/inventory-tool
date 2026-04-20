import streamlit as st
from supabase import create_client

url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
st.write("URL:", url)
st.write("Key prefix:", key[:15] + "...")

try:
    supabase = create_client(url, key)
    # 尝试查询（即使表不存在也会测试连接）
    supabase.table("records").select("id").limit(1).execute()
    st.success("连接成功！")
except Exception as e:
    st.error(f"连接失败: {e}")
    st.code(str(e))
