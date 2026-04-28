"""
条形码批量生成与打印工具 - 最终版（可调文本距离，避免重叠）
使用方法：streamlit run barcode_batch_printer.py
"""

import io, base64, zipfile
import streamlit as st
import barcode
from barcode.writer import ImageWriter
from PIL import Image

# ---------- 页面配置 ----------
st.set_page_config(page_title="条形码批量生成&打印", page_icon="📦", layout="wide")
st.title("📦 条形码批量生成 & 打印工具")
st.markdown("支持 Code128、EAN13 等，批量生成后可顺序预览、下载或一键打印。")

# ---------- 初始化 Session State ----------
if "preview_active" not in st.session_state:
    st.session_state.preview_active = False
if "preview_images" not in st.session_state:
    st.session_state.preview_images = []
if "preview_names" not in st.session_state:
    st.session_state.preview_names = []

if "generated_images" not in st.session_state:
    st.session_state.generated_images = []
if "generated_names" not in st.session_state:
    st.session_state.generated_names = []
if "generated_barcode_type" not in st.session_state:
    st.session_state.generated_barcode_type = ""

# ---------- 侧边栏参数 ----------
st.sidebar.header("⚙️ 条码参数")
barcode_type = st.sidebar.selectbox(
    "条码类型",
    ["code128", "ean13", "ean8", "upca", "isbn13"],
    index=0,
    help="code128 最通用，可含字母数字；其他类型需符合标准格式"
)

dpi = st.sidebar.slider("输出 DPI", 100, 600, 300, 50)
module_width = st.sidebar.slider("模块宽度 (mm)", 0.1, 0.5, 0.2, 0.05)
module_height = st.sidebar.slider("条码高度 (mm)", 5.0, 30.0, 15.0, 1.0)
text_distance = st.sidebar.slider("文本距离 (mm)", 1.0, 15.0, 5.0, 0.5,
                                  help="调整条码下方文字与条码的间距，防止重叠")

writer_options = {
    "module_width": module_width,
    "module_height": module_height,
    "dpi": dpi,
    "quiet_zone": 6.5,
    "font_size": 10,
    "text_distance": text_distance,   # 使用滑块值
}

# ---------- 数据输入区 ----------
st.header("📝 输入条码数据")
input_mode = st.radio(
    "选择输入方式",
    ["单个条码", "多个条码（逗号或换行分隔）", "从文本文件上传"],
    horizontal=True
)

barcode_data_list = []

if input_mode == "单个条码":
    single = st.text_input("请输入条码内容", placeholder="例如：HCL905551210002G10130038")
    if single.strip():
        barcode_data_list = [single.strip()]

elif input_mode == "多个条码（逗号或换行分隔）":
    raw = st.text_area("请输入多个条码，逗号或换行分隔", placeholder="HCL..., DC...\n或每行一个")
    if raw.strip():
        for line in raw.split('\n'):
            line = line.strip()
            if line:
                barcode_data_list.extend([p.strip() for p in line.split(',') if p.strip()])

elif input_mode == "从文本文件上传":
    up_file = st.file_uploader("上传 .txt 文件（每行一个条码）", type=["txt"])
    if up_file:
        content = up_file.read().decode("utf-8")
        barcode_data_list = [line.strip() for line in content.split('\n') if line.strip()]
        st.success(f"已读取 {len(barcode_data_list)} 条数据")

# ---------- 生成按钮 ----------
if st.button("🎨 生成条形码", type="primary", use_container_width=True):
    if not barcode_data_list:
        st.warning("请先输入条码数据！")
    else:
        # 清空旧数据与预览
        st.session_state.preview_active = False
        st.session_state.preview_images = []
        st.session_state.preview_names = []
        st.session_state.generated_images = []
        st.session_state.generated_names = []
        st.session_state.generated_barcode_type = ""

        st.subheader("📊 生成结果")
        col1, col2 = st.columns(2)
        col1.metric("输入数量", len(barcode_data_list))
        col2.metric("条码类型", barcode_type.upper())

        new_images = []
        new_names = []
        failed_list = []

        try:
            bc_class = barcode.get_barcode_class(barcode_type)
        except Exception as e:
            st.error(f"条码类型错误：{e}")
            st.stop()

        progress = st.progress(0)
        for idx, data in enumerate(barcode_data_list):
            try:
                bar = bc_class(data, writer=ImageWriter())
                img_bytes = io.BytesIO()
                bar.write(img_bytes, options=writer_options)
                img_bytes.seek(0)
                img = Image.open(img_bytes).convert("RGB")
                if img.size == (0, 0):
                    raise ValueError("图片尺寸异常")
                new_images.append(img)

                safe_name = "".join(c if c.isalnum() or c in ('-','_') else '_' for c in data)
                new_names.append(f"{safe_name}_{idx+1:03d}.png")
            except Exception as e:
                failed_list.append(f"❌ {data}：{e}")
            progress.progress((idx + 1) / len(barcode_data_list))

        if failed_list:
            with st.expander("⚠️ 部分条码生成失败，点击查看详情"):
                for msg in failed_list:
                    st.write(msg)

        if not new_images:
            st.error("未生成任何条码，请检查数据与条码类型。")
        else:
            st.session_state.generated_images = new_images
            st.session_state.generated_names = new_names
            st.session_state.generated_barcode_type = barcode_type.upper()
            st.success(f"成功生成 {len(new_images)} 个条码！")

# ---------- 展示已生成的条码 ----------
if st.session_state.generated_images:
    images = st.session_state.generated_images
    names = st.session_state.generated_names
    btype = st.session_state.generated_barcode_type

    st.markdown("---")
    st.subheader("📊 已生成的条码")

    cols_per_row = 4
    for i in range(0, len(images), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, col in enumerate(cols):
            idx_img = i + j
            if idx_img >= len(images):
                break
            with col:
                st.image(images[idx_img],
                         caption=f"#{idx_img+1}  {names[idx_img].rsplit('_', 1)[0]}",
                         use_container_width=True)
                buf = io.BytesIO()
                images[idx_img].save(buf, format="PNG")
                st.download_button(label="💾 下载此张", data=buf.getvalue(),
                                   file_name=names[idx_img],
                                   mime="image/png", key=f"dl_{idx_img}")

    st.divider()
    st.subheader("📥 批量下载与打印")

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for idx, img in enumerate(images):
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            zf.writestr(names[idx], buf.getvalue())
    zip_buf.seek(0)

    col_dl, col_preview, col_print = st.columns([1, 1, 1])
    with col_dl:
        st.download_button(label="📦 下载 ZIP", data=zip_buf,
                           file_name="barcodes_batch.zip", mime="application/zip",
                           use_container_width=True)

    # 顺序预览
    with col_preview:
        if st.button("🔍 顺序预览", use_container_width=True):
            b64_list = []
            for img in images:
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                b64_list.append(base64.b64encode(buf.getvalue()).decode())
            st.session_state.preview_images = b64_list
            st.session_state.preview_names = names
            st.session_state.preview_active = True

    # 一键打印
    with col_print:
        if st.button("🖨️ 打印全部", use_container_width=True):
            b64_img = []
            for img in images:
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                b64_img.append(base64.b64encode(buf.getvalue()).decode())
            html_print = """<html><head><title>打印</title>
            <style>body{font-family:Arial;padding:20px}.bc{page-break-after:always;text-align:center}
            img{max-width:100%}@media print{.no-print{display:none}}</style></head><body>
            <div class="no-print"><button onclick="window.print()">打印</button>
            <button onclick="window.close()">关闭</button></div>"""
            for i, b in enumerate(b64_img):
                html_print += f'<div class="bc"><img src="data:image/png;base64,{b}"><p>条码 {i+1}</p></div>'
            html_print += "<script>window.onload=function(){window.print()}</script></body></html>"
            enc = base64.b64encode(html_print.encode()).decode()
            js = f'<script>var w=window.open("");w.document.write(atob("{enc}"));w.document.close();</script>'
            st.components.v1.html(js, height=0)

# ---------- 顺序预览组件 ----------
if st.session_state.preview_active and st.session_state.preview_images:
    st.markdown("---")
    st.subheader("🔍 顺序预览")
    close_col, _ = st.columns([1, 3])
    with close_col:
        if st.button("❌ 关闭预览"):
            st.session_state.preview_active = False
            st.session_state.preview_images = []
            st.session_state.preview_names = []
            st.rerun()

    b64_list = st.session_state.preview_images
    names = st.session_state.preview_names
    html_preview = f"""
    <div style="text-align:center">
      <div style="margin:10px">
        <button onclick="prev()">⬅ 上一张</button>
        <span id="counter" style="margin:0 15px;font-weight:bold"></span>
        <button onclick="next()">下一张 ➡</button>
      </div>
      <img id="barcodeImg" style="max-width:100%;max-height:70vh;border:1px solid #ddd;padding:5px"
           src="data:image/png;base64,{b64_list[0]}">
      <div id="info" style="margin-top:8px;color:gray;font-family:monospace"></div>
    </div>
    <script>
      const images = {b64_list};
      const names = {names};
      let cur = 0;
      function show(i) {{
          cur = (i + images.length) % images.length;
          document.getElementById('barcodeImg').src = "data:image/png;base64," + images[cur];
          document.getElementById('counter').innerText = (cur+1) + " / " + images.length;
          document.getElementById('info').innerText = names[cur];
      }}
      function prev() {{ show(cur-1); }}
      function next() {{ show(cur+1); }}
      show(0);
    </script>
    """
    st.components.v1.html(html_preview, height=520)

if not st.session_state.generated_images:
    st.info("👆 输入数据并点击“生成条形码”后，这里将展示结果。")

st.markdown("---")
st.caption("💡 长序列号请用 code128；文本距离可在侧边栏调节，避免文字重叠。")