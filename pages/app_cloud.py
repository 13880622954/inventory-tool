import streamlit as st
import pandas as pd
import json
import tempfile
import zipfile
import os
from datetime import datetime
import mimetypes
import base64
from supabase import create_client, Client

# ========== 配置 ==========
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

@st.cache_resource
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_supabase()

# 存储桶名称（请确认已创建）
BUCKET_NAME = "files"

# 确保存储桶存在（如果不存在，会自动创建，但需要用户先手动创建一次）
try:
    supabase.storage.get_bucket(BUCKET_NAME)
except:
    supabase.storage.create_bucket(BUCKET_NAME, {"public": False})

# ========== 数据库操作函数 ==========
def load_data():
    response = supabase.table("records").select("*").order("upload_time", desc=True).execute()
    data = response.data
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
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
    now = datetime.now().isoformat()
    files_json = json.dumps(files_list, ensure_ascii=False)
    supabase.table("records").insert({
        "danhao": danhao,
        "event": event,
        "remark": remark,
        "warehouse": warehouse,
        "material": material,
        "status": status,
        "files_info": files_json,
        "upload_time": now
    }).execute()

def update_record(record_id, danhao, event, remark, warehouse, material, status, files_list):
    files_json = json.dumps(files_list, ensure_ascii=False)
    supabase.table("records").update({
        "danhao": danhao,
        "event": event,
        "remark": remark,
        "warehouse": warehouse,
        "material": material,
        "status": status,
        "files_info": files_json
    }).eq("id", record_id).execute()

def delete_record(record_id, files_list):
    for f in files_list:
        object_key = f.get('object_key')
        if object_key:
            try:
                supabase.storage.from_(BUCKET_NAME).remove([object_key])
            except Exception as e:
                st.error(f"删除文件失败: {e}")
    supabase.table("records").delete().eq("id", record_id).execute()

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
                file_data = supabase.storage.from_(BUCKET_NAME).download(object_key)
                target_subdir = os.path.join(temp_dir, danhao)
                os.makedirs(target_subdir, exist_ok=True)
                target_file = os.path.join(target_subdir, f['filename'])
                with open(target_file, "wb") as f_out:
                    f_out.write(file_data)
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

def preview_file_from_storage(object_key, filename):
    if not object_key:
        st.warning("无文件")
        return
    try:
        file_data = supabase.storage.from_(BUCKET_NAME).download(object_key)
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp:
            tmp.write(file_data)
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
st.title("📁 文件管理助手 - Supabase 版")

# 侧边栏表单
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
                            # 上传到 Supabase Storage
                            supabase.storage.from_(BUCKET_NAME).upload(object_key, uploaded_file.getvalue())
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

# 筛选区域（与原代码相同）
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

df = load_data()
if df.empty:
    st.info("📭 暂无记录，请从左侧添加。")
    st.stop()

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

# 导出处理
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
                            # 生成临时下载链接（有效期1小时）
                            try:
                                # Supabase Storage 生成签名 URL 的方法略有不同，这里使用 get_public_url 对于私有桶需要授权
                                # 对于私有桶，可以使用 create_signed_url 方法
                                signed_url = supabase.storage.from_(BUCKET_NAME).create_signed_url(object_key, 3600)
                                st.markdown(f"[⬇️ 下载]({signed_url})")
                            except Exception as e:
                                st.write(f"生成链接失败: {e}")
                        with col_pv:
                            preview_key = f"preview_check_{record_id}_{i}"
                            if st.checkbox("👁️ 预览", key=preview_key):
                                preview_file_from_storage(object_key, filename)
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
