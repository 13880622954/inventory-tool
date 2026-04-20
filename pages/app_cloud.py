import streamlit as st
import pandas as pd
import json
import os
import tempfile
import zipfile
from datetime import datetime
import mimetypes
import base64
import pymysql
import boto3
from botocore.client import Config

# ========== 从 Streamlit Secrets 读取配置 ==========
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = int(st.secrets.get("DB_PORT", 4000))
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_NAME = st.secrets.get("DB_NAME", "test")

S3_ENDPOINT = st.secrets["S3_ENDPOINT"]
S3_ACCESS_KEY = st.secrets["S3_ACCESS_KEY"]
S3_SECRET_KEY = st.secrets["S3_SECRET_KEY"]
S3_BUCKET = st.secrets["S3_BUCKET"]
S3_SECURE = st.secrets.get("S3_SECURE", "true").lower() == "true"

# ========== 数据库连接 ==========
@st.cache_resource
def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# ========== S3 客户端（七牛云） ==========
@st.cache_resource
def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        use_ssl=S3_SECURE
    )

s3_client = get_s3_client()

# ========== 数据库操作函数 ==========
def load_data():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id, danhao, event, remark, warehouse, material, status, files_info, upload_time FROM records ORDER BY upload_time DESC")
        rows = cur.fetchall()
    conn.close()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    def parse_files(x):
        if x:
            try:
                return json.loads(x) if isinstance(x, str) else x
            except:
                return []
        return []
    df['files_list'] = df['files_info'].apply(parse_files)
    df['文件名显示'] = df['files_list'].apply(lambda lst: '、'.join([f.get('filename', '') for f in lst]) if lst else '(无文件)')
    df.rename(columns={
        "danhao": "单号",
        "event": "涉及事件",
        "remark": "备注",
        "warehouse": "涉及仓库",
        "material": "涉及物料",
        "status": "处理情况",
        "upload_time": "上传时间"
    }, inplace=True)
    return df

def add_record(danhao, event, remark, warehouse, material, status, files_list):
    conn = get_db_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    files_json = json.dumps(files_list, ensure_ascii=False)
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO records (danhao, event, remark, warehouse, material, status, files_info, upload_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', (danhao, event, remark, warehouse, material, status, files_json, now))
        conn.commit()
    conn.close()

def update_record(record_id, danhao, event, remark, warehouse, material, status, files_list):
    conn = get_db_connection()
    files_json = json.dumps(files_list, ensure_ascii=False)
    with conn.cursor() as cur:
        cur.execute('''
            UPDATE records SET danhao=%s, event=%s, remark=%s, warehouse=%s, material=%s, status=%s, files_info=%s WHERE id=%s
        ''', (danhao, event, remark, warehouse, material, status, files_json, record_id))
        conn.commit()
    conn.close()

def delete_record(record_id, files_list):
    for f in files_list:
        object_key = f.get('object_key')
        if object_key:
            try:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=object_key)
            except Exception as e:
                st.error(f"删除文件失败: {e}")
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM records WHERE id=%s", (record_id,))
        conn.commit()
    conn.close()

# ========== 导出功能 ==========
def export_to_zip(df, label):
    if df.empty:
        st.warning("没有数据可导出")
        return None
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, f"{label}_报表.csv")
    export_df = df[["单号", "涉及事件", "备注", "涉及仓库", "涉及物料", "处理情况", "文件名显示", "上传时间"]].copy()
    export_df.rename(columns={"文件名显示": "文件名"}, inplace=True)
    export_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    for _, row in df.iterrows():
        danhao = str(row["单号"])
        for f in row['files_list']:
            object_key = f.get('object_key')
            if not object_key:
                continue
            try:
                target_subdir = os.path.join(temp_dir, danhao)
                os.makedirs(target_subdir, exist_ok=True)
                target_file = os.path.join(target_subdir, f['filename'])
                s3_client.download_file(S3_BUCKET, object_key, target_file)
            except Exception as e:
                st.warning(f"无法下载文件 {f['filename']}: {e}")
    zip_path = os.path.join(temp_dir, f"{label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".zip"):
                    continue
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, temp_dir)
                zf.write(full_path, arcname)
    return zip_path

def preview_file_from_cloud(object_key, filename):
    if not object_key:
        st.warning("无文件")
        return
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
            s3_client.download_fileobj(S3_BUCKET, object_key, tmp)
            tmp_path = tmp.name
        mime, _ = mimetypes.guess_type(filename)
        if mime is None:
            mime = "application/octet-stream"
        if mime.startswith("image/"):
            st.image(tmp_path, caption=filename, use_container_width=True)
        elif mime == "application/pdf":
            with open(tmp_path, "rb") as f:
                base64_pdf = base64.b64encode(f.read()).decode('utf-8')
            pdf_display = f'<iframe src="data:application/pdf;base64,{base64_pdf}" width="100%" height="600" type="application/pdf"></iframe>'
            st.markdown(pdf_display, unsafe_allow_html=True)
        elif mime.startswith("text/"):
            with open(tmp_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read(2000)
            st.text(content)
            if len(content) >= 2000:
                st.caption("文件较大，仅显示前2000字符")
        else:
            st.info(f"暂不支持预览该类型文件（{mime}），请下载查看")
        os.unlink(tmp_path)
    except Exception as e:
        st.error(f"预览失败: {e}")

# ========== Streamlit UI ==========
st.set_page_config(page_title="文件管理助手", layout="wide")
st.title("📁 文件管理助手 - 云版（TiDB + 七牛云）")

# 侧边栏：添加新记录
with st.sidebar:
    st.header("➕ 添加新记录")
    with st.form("upload_form", clear_on_submit=True):
        danhao = st.text_input("涉及单号 *")
        event = st.text_input("涉及事件")
        remark = st.text_area("备注", height=80)
        warehouse = st.text_input("涉及仓库")
        material = st.text_input("涉及物料")
        status = st.text_input("处理情况", placeholder="例如：待处理、已完成")
        uploaded_files = st.file_uploader("选择文件（可多选）", type=None, accept_multiple_files=True)
        text_content = st.text_input("文字内容（无文件时填写）", placeholder="输入文字替代文件名")
        submitted = st.form_submit_button("📤 保存记录", use_container_width=True)
        if submitted:
            if not danhao:
                st.error("请填写单号")
            else:
                files_list = []
                if uploaded_files:
                    for uploaded_file in uploaded_files:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        object_key = f"uploads/{timestamp}_{uploaded_file.name}"
                        try:
                            s3_client.upload_fileobj(uploaded_file, S3_BUCKET, object_key)
                            files_list.append({"filename": uploaded_file.name, "object_key": object_key})
                        except Exception as e:
                            st.error(f"上传文件 {uploaded_file.name} 失败: {e}")
                            st.stop()
                elif text_content.strip():
                    files_list.append({"filename": text_content.strip(), "object_key": ""})
                else:
                    st.error("请至少上传一个文件或填写文字内容")
                    st.stop()
                add_record(danhao, event, remark, warehouse, material, status, files_list)
                st.success(f"✅ 已保存记录，包含 {len(files_list)} 个文件/文字项！")
                st.rerun()

# 筛选区域
st.subheader("🔍 筛选记录")
st.caption("多个条件同时满足（AND），模糊匹配")

col1, col2, col3, col4 = st.columns(4)
with col1:
    search_danhao = st.text_input("涉及单号", placeholder="包含...")
with col2:
    search_event = st.text_input("涉及事件", placeholder="包含...")
with col3:
    search_remark = st.text_input("备注", placeholder="包含...")
with col4:
    search_warehouse = st.text_input("涉及仓库", placeholder="包含...")

col5, col6, col7, col8, col9, col10, col11 = st.columns([1, 1, 1, 1.5, 1, 1, 1])
with col5:
    search_material = st.text_input("涉及物料", placeholder="包含...")
with col6:
    search_status = st.text_input("处理情况", placeholder="包含...")
with col7:
    search_date = st.text_input("上传日期", placeholder="例如 2025-01")
with col8:
    search_filename = st.text_input("文件名/文字内容", placeholder="包含...")
with col9:
    query_clicked = st.button("🔍 查询", use_container_width=True)
with col10:
    reset_clicked = st.button("🗑️ 重置", use_container_width=True)
with col11:
    st.write("")

col_export1, col_export2 = st.columns(2)
with col_export1:
    export_all_clicked = st.button("📦 导出全部记录", use_container_width=True)
with col_export2:
    export_filtered_clicked = st.button("📂 导出筛选记录", use_container_width=True)

if reset_clicked:
    for key in ["search_danhao", "search_event", "search_remark", "search_warehouse", "search_material", "search_status", "search_date", "search_filename"]:
        if key in st.session_state:
            st.session_state[key] = ""
    st.rerun()

# 加载数据
df = load_data()
if df.empty:
    st.info("📭 暂无记录，请从左侧添加。")
    st.stop()

# 应用筛选
mask = pd.Series([True] * len(df))
if search_danhao:
    mask &= df["单号"].astype(str).str.contains(search_danhao, case=False, na=False)
if search_event:
    mask &= df["涉及事件"].astype(str).str.contains(search_event, case=False, na=False)
if search_remark:
    mask &= df["备注"].astype(str).str.contains(search_remark, case=False, na=False)
if search_warehouse:
    mask &= df["涉及仓库"].astype(str).str.contains(search_warehouse, case=False, na=False)
if search_material:
    mask &= df["涉及物料"].astype(str).str.contains(search_material, case=False, na=False)
if search_status:
    mask &= df["处理情况"].astype(str).str.contains(search_status, case=False, na=False)
if search_date:
    mask &= df["上传时间"].astype(str).str.contains(search_date, case=False, na=False)
if search_filename:
    mask &= df["文件名显示"].astype(str).str.contains(search_filename, case=False, na=False)

filtered_df = df[mask]
st.write(f"📊 共 **{len(filtered_df)}** 条记录")

# 导出逻辑
if export_all_clicked:
    if df.empty:
        st.warning("没有记录可导出")
    else:
        with st.spinner("正在打包全部记录和文件..."):
            zip_path = export_to_zip(df, "全部记录")
            if zip_path:
                with open(zip_path, "rb") as f:
                    zip_bytes = f.read()
                st.download_button("⬇️ 点击下载导出包 (全部记录)", data=zip_bytes, file_name=f"全部记录_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip", mime="application/zip", key="export_all_download")
                st.success("打包完成，点击上方按钮下载")
            else:
                st.error("导出失败")

if export_filtered_clicked:
    if filtered_df.empty:
        st.warning("当前筛选结果为空，无法导出")
    else:
        with st.spinner("正在打包筛选结果和文件..."):
            zip_path = export_to_zip(filtered_df, "筛选记录")
            if zip_path:
                with open(zip_path, "rb") as f:
                    zip_bytes = f.read()
                st.download_button("⬇️ 点击下载导出包 (筛选记录)", data=zip_bytes, file_name=f"筛选记录_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip", mime="application/zip", key="export_filtered_download")
                st.success("打包完成，点击上方按钮下载")
            else:
                st.error("导出失败")

# 表格显示记录
if not filtered_df.empty:
    header_cols = st.columns([1, 1, 1, 1, 1, 1, 1.5, 0.6, 0.6, 0.6])
    header_cols[0].write("**单号**")
    header_cols[1].write("**涉及事件**")
    header_cols[2].write("**备注**")
    header_cols[3].write("**涉及仓库**")
    header_cols[4].write("**涉及物料**")
    header_cols[5].write("**处理情况**")
    header_cols[6].write("**文件/内容**")
    header_cols[7].write("**详情**")
    header_cols[8].write("**编辑**")
    header_cols[9].write("**删除**")
    st.markdown("---")

    for idx, row in filtered_df.iterrows():
        record_id = row["id"]
        files_list = row['files_list']
        display_names = row['文件名显示']
        cols = st.columns([1, 1, 1, 1, 1, 1, 1.5, 0.6, 0.6, 0.6])
        cols[0].write(row["单号"])
        cols[1].write(row["涉及事件"][:15] + "..." if len(row["涉及事件"])>15 else row["涉及事件"])
        cols[2].write(row["备注"][:20] + "..." if len(row["备注"])>20 else row["备注"])
        cols[3].write(row["涉及仓库"])
        cols[4].write(row["涉及物料"])
        cols[5].write(row["处理情况"])
        cols[6].write(display_names)
        detail_expanded = cols[7].checkbox("📂", key=f"detail_check_{record_id}", label_visibility="collapsed")
        if cols[8].button("✏️", key=f"edit_btn_{record_id}"):
            st.session_state[f"editing_{record_id}"] = not st.session_state.get(f"editing_{record_id}", False)
            st.rerun()
        if cols[9].button("🗑️", key=f"del_btn_{record_id}"):
            confirm_key = f"confirm_del_{record_id}"
            if not st.session_state.get(confirm_key, False):
                st.session_state[confirm_key] = True
                st.warning(f"再次点击删除确认记录 **{row['单号']}**")
            else:
                delete_record(record_id, files_list)
                st.success("删除成功")
                st.session_state.pop(confirm_key, None)
                st.rerun()
        if detail_expanded:
            with st.expander(f"文件详情（共 {len(files_list)} 个）", expanded=True):
                for i, f in enumerate(files_list):
                    filename = f.get('filename', '')
                    object_key = f.get('object_key', '')
                    st.write(f"**{i+1}. {filename}**")
                    if object_key:
                        col_dl, col_pv = st.columns(2)
                        with col_dl:
                            try:
                                url = s3_client.generate_presigned_url(
                                    ClientMethod='get_object',
                                    Params={'Bucket': S3_BUCKET, 'Key': object_key},
                                    ExpiresIn=3600
                                )
                                st.markdown(f"[⬇️ 下载]({url})")
                            except:
                                st.write("下载链接生成失败")
                        with col_pv:
                            preview_key = f"preview_check_{record_id}_{i}"
                            if st.checkbox("👁️ 预览", key=preview_key):
                                preview_file_from_cloud(object_key, filename)
                    else:
                        st.info("文字记录，无文件")
        if st.session_state.get(f"editing_{record_id}", False):
            with st.expander(f"编辑记录 #{record_id}", expanded=True):
                with st.form(key=f"form_edit_{record_id}"):
                    new_danhao = st.text_input("单号", value=row["单号"])
                    new_event = st.text_input("涉及事件", value=row["涉及事件"])
                    new_remark = st.text_area("备注", value=row["备注"])
                    new_warehouse = st.text_input("涉及仓库", value=row["涉及仓库"])
                    new_material = st.text_input("涉及物料", value=row["涉及物料"])
                    new_status = st.text_input("处理情况", value=row["处理情况"])
                    if st.form_submit_button("保存修改"):
                        update_record(record_id, new_danhao, new_event, new_remark, new_warehouse, new_material, new_status, files_list)
                        st.session_state[f"editing_{record_id}"] = False
                        st.success("修改成功")
                        st.rerun()
        st.markdown("---")
